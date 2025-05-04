#!/bin/bash

# Script: deploy.sh
# Description: Initializes and monitors a Docker Compose-based data pipeline with Airflow, Kafka,
#              MySQL, MongoDB, MinIO and other services. Ensures idempotency for safe re-runs.
# Author: Venkat CG
# Date: April 13, 2025
# Usage: ./deploy.sh
# Prerequisites:
#   - Docker and Docker Compose installed
#   - .env file with required variables (PROJECT_USER, PROJECT_PASSWORD, etc.)
# Notes:
#   - Idempotent: Safe to run multiple times without duplicating resources
#   - Cleans up on SIGINT/SIGTERM
#   - Exits with non-zero status on failure

# Exit on error, undefined variables, or pipeline errors
set -euo pipefail

# Configuration
readonly COMPOSE_FILE="docker_compose.yml"
readonly TIMEOUT=1200          # Timeout for container health checks (seconds)
readonly CHECK_INTERVAL=5      # Interval between health checks (seconds)
readonly MAX_RESTARTS=3        # Maximum container restarts before failure
readonly AIRFLOW_DAG_ID="minio_listener_dag"  # Updated DAG ID
readonly CONTAINERS=(
    "mysql"
    "mongodb"
    "zookeeper"
    "kafka"
    "kafka-ui"
    "minio"
    "airflow"
    "python-runner"
)

# Colors for logging
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly NC='\033[0m'

# Log function for consistent output
log() {
    local message=$1
    local color=${2:-$NC}
    echo -e "${color}[$(date +'%Y-%m-%d %H:%M:%S')] ${message}${NC}"
}

# Load environment variables from .env file
load_env() {
    if [[ ! -f .env ]]; then
        log ".env file not found!" "$RED"
        exit 1
    fi
    log "Loading environment variables..." "$YELLOW"
    set -a
    # Filter comments and format key=value pairs
    source <(grep -v '^\s*#' .env | sed -E 's/^\s*(.+)\s*=\s*(.+)\s*$/\1="\2"/')
    set +a
    # Validate critical environment variables
    for var in PROJECT_USER PROJECT_PASSWORD; do
        if [[ -z "${!var}" ]]; then
            log "Environment variable ${var} is not set in .env file." "$RED"
            exit 1
        fi
    done
    log "Environment variables loaded successfully." "$GREEN"
}

# Check if Docker Compose services are running
check_compose() {
    docker-compose -f "$COMPOSE_FILE" ps --services --filter "status=running" | grep -q .
}

# Check if Docker Compose services exist but are stopped
check_stopped_compose() {
    docker-compose -f "$COMPOSE_FILE" ps --services --filter "status=exited" | grep -q .
}

# Get container status
get_container_status() {
    local container_name=$1
    docker inspect --format='{{.State.Status}}' "$container_name" 2>/dev/null || echo "not_found"
}

# Get container health
get_container_health() {
    local container_name=$1
    local health
    health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}no_health{{end}}' "$container_name" 2>/dev/null)
    echo "${health:-no_health}"
}

# Get container restarts
get_container_restarts() {
    local container_name=$1
    docker inspect --format='{{.RestartCount}}' "$container_name" 2>/dev/null || echo "0"
}

# Get container logs (last 50 lines)
get_container_logs() {
    local container_name=$1
    docker logs "$container_name" 2>&1 | tail -n 50
}

# Cleanup Docker Compose services
cleanup() {
    log "Shutting down Docker Compose services..." "$YELLOW"
    docker-compose -f "$COMPOSE_FILE" down --remove-orphans
    log "Cleanup completed." "$GREEN"
}

# Wait for Airflow webserver to be reachable
wait_for_webserver() {
    local host=$1
    local port=$2
    local retries=30
    log "Waiting for Airflow webserver at ${host}:${port}..." "$YELLOW"

    for ((i=1; i<=retries; i++)); do
        if docker exec airflow bash -c "curl -s --head http://localhost:${port} | head -n 1 | grep 'HTTP/1.[01] [23]..'" >/dev/null; then
            log "Airflow webserver is reachable." "$GREEN"
            return 0
        fi
        log "Retry ${i}/${retries}..." "$YELLOW"
        sleep 10
    done
    log "Airflow webserver not reachable after ${retries} retries." "$RED"
    return 1
}

# Start Docker Compose services
start_compose() {
    if check_compose; then
        log "Docker Compose services are already running." "$GREEN"
        return 0
    elif check_stopped_compose; then
        log "Detected stopped Docker Compose services. Starting them..." "$YELLOW"
        if ! docker-compose -f "$COMPOSE_FILE" start; then
            log "Failed to start existing Docker Compose services." "$RED"
            for container in "${CONTAINERS[@]}"; do
                log "Status for ${container}: $(get_container_status "$container")" "$RED"
                log "Health for ${container}: $(get_container_health "$container")" "$RED"
                log "Logs for ${container}:\n$(get_container_logs "$container")" "$RED"
            done
            exit 1
        fi
        log "Stopped Docker Compose services started successfully." "$GREEN"
    else
        log "Starting Docker Compose services..." "$YELLOW"
        if ! docker-compose -f "$COMPOSE_FILE" up -d --build; then
            log "Failed to start Docker Compose." "$RED"
            for container in "${CONTAINERS[@]}"; do
                log "Status for ${container}: $(get_container_status "$container")" "$RED"
                log "Health for ${container}: $(get_container_health "$container")" "$RED"
                log "Logs for ${container}:\n$(get_container_logs "$container")" "$RED"
            done
            exit 1
        fi
        log "Docker Compose started successfully." "$GREEN"
    fi
}

# Monitor container health
monitor_containers() {
    log "Monitoring container health..." "$YELLOW"
    local start_time
    start_time=$(date +%s)
    local healthy_containers=()

    while [ $(( $(date +%s) - start_time )) -lt $TIMEOUT ]; do
        local all_healthy=true
        for container in "${CONTAINERS[@]}"; do
            # Skip if already healthy
            [[ " ${healthy_containers[*]} " =~ " ${container} " ]] && continue

            local status health restarts
            status=$(get_container_status "$container")
            health=$(get_container_health "$container")
            restarts=$(get_container_restarts "$container")

            log "Checking ${container}: status=${status}, health=${health}, restarts=${restarts}" "$YELLOW"

            if [[ "$status" == "not_found" ]]; then
                log "Container ${container} not found." "$RED"
                log "Logs for ${container}:\n$(get_container_logs "$container")" "$RED"
                cleanup
                exit 1
            fi

            if [[ "$status" != "running" ]]; then
                log "Container ${container} is not running (status: ${status})." "$RED"
                log "Logs for ${container}:\n$(get_container_logs "$container")" "$RED"
                cleanup
                exit 1
            fi

            if [[ "$restarts" -ge $MAX_RESTARTS ]]; then
                log "Container ${container} restarted too many times (${restarts})." "$RED"
                log "Logs for ${container}:\n$(get_container_logs "$container")" "$RED"
                cleanup
                exit 1
            fi

            if [[ "$health" == "healthy" ]]; then
                log "Container ${container} is healthy." "$GREEN"
                healthy_containers+=("$container")
            else
                all_healthy=false
                if [[ "$health" != "no_health" ]]; then
                    log "Container ${container} not healthy (health: ${health})." "$YELLOW"
                fi
            fi
        done

        [[ "$all_healthy" == true ]] && {
            log "All containers are healthy." "$GREEN"
            return 0
        }
        sleep $CHECK_INTERVAL
    done

    log "Timeout reached. Not all containers are healthy." "$RED"
    for container in "${CONTAINERS[@]}"; do
        local health
        health=$(get_container_health "$container")
        log "Final health for ${container}: ${health}" "$RED"
        if [[ "$health" != "healthy" && "$health" != "no_health" ]]; then
            log "Logs for ${container}:\n$(get_container_logs "$container")" "$RED"
        fi
    done
    cleanup
    exit 1
}

# Verify MySQL database
verify_mysql() {
    log "Verifying MySQL database..." "$YELLOW"
    for ((i=1; i<=10; i++)); do
        if docker exec mysql bash -c "mysql -u\"${PROJECT_USER}\" -p\"${PROJECT_PASSWORD}\" -e 'SHOW DATABASES LIKE \"${AIRFLOW_DB}\";'" >/dev/null 2>&1; then
            log "AIRFLOW database confirmed." "$GREEN"
            return 0
        fi
        log "AIRFLOW database not found, retrying (${i}/10)..." "$YELLOW"
        sleep 5
    done
    log "Failed to verify AIRFLOW database." "$RED"
    log "MySQL logs:\n$(get_container_logs mysql)" "$RED"
    cleanup
    exit 1
}

# Create Kafka topic
create_kafka_topic() {
    log "Creating Kafka topic '${KAFKA_TOPIC_NAME}'..." "$YELLOW"
    if docker exec kafka bash -c "/opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092 | grep -q '${KAFKA_TOPIC_NAME}'"; then
        log "Kafka topic '${KAFKA_TOPIC_NAME}' already exists." "$GREEN"
        return 0
    fi
    if ! docker exec kafka bash -c "/opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic '${KAFKA_TOPIC_NAME}'"; then
        log "Failed to create Kafka topic." "$RED"
        log "Kafka logs:\n$(get_container_logs kafka)" "$RED"
        cleanup
        exit 1
    fi
    log "Kafka topic created successfully." "$GREEN"
}

# Initialize Airflow
initialize_airflow() {
    log "Initializing Airflow..." "$YELLOW"

    # Wait for Airflow container
    for ((i=1; i<=30; i++)); do
        if [[ "$(get_container_status airflow)" == "running" ]]; then
            log "Airflow container is running." "$GREEN"
            break
        fi
        log "Airflow not ready, retrying (${i}/30)..." "$YELLOW"
        sleep 5
    done
    [[ "$(get_container_status airflow)" != "running" ]] && {
        log "Airflow container failed to start." "$RED"
        log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
        cleanup
        exit 1
    }

    # Initialize Airflow database
    log "Running airflow db init..." "$YELLOW"
    if docker exec airflow airflow db check >/dev/null 2>&1; then
        log "Airflow database already initialized." "$GREEN"
    else
        for ((i=1; i<=3; i++)); do
            if docker exec airflow airflow db init >/dev/null 2>&1; then
                log "Airflow db init completed." "$GREEN"
                break
            fi
            log "Failed to run airflow db init, retrying (${i}/3)..." "$YELLOW"
            sleep 5
        done
        [[ $? -ne 0 ]] && {
            log "Failed to run airflow db init after retries." "$RED"
            log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
            cleanup
            exit 1
        }
    fi

    # Upgrade Airflow database
    log "Running airflow db upgrade..." "$YELLOW"
    if docker exec airflow airflow db upgrade >/dev/null 2>&1; then
        log "Airflow db upgrade completed." "$GREEN"
    else
        log "Failed to run airflow db upgrade." "$RED"
        log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
        cleanup
        exit 1
    fi

    # Create Airflow admin user
    log "Creating Airflow admin user..." "$YELLOW"
    if docker exec airflow airflow users list | grep -q "${PROJECT_USER}"; then
        log "Airflow admin user already exists." "$GREEN"
    else
        if ! docker exec airflow airflow users create \
            --username "${PROJECT_USER}" \
            --password "${PROJECT_PASSWORD}" \
            --firstname "${AIRFLOW_FIRSTNAME}" \
            --lastname "${AIRFLOW_LASTNAME}" \
            --role Admin \
            --email "${AIRFLOW_EMAIL}" >/dev/null 2>&1; then
            log "Failed to create Airflow admin user." "$RED"
            log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
            cleanup
            exit 1
        fi
        log "Airflow admin user created." "$GREEN"
    fi

    # Start Airflow services
    log "Starting Airflow scheduler..." "$YELLOW"
    if ! docker exec -d airflow airflow scheduler; then
        log "Failed to start Airflow scheduler." "$RED"
        log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
        cleanup
        exit 1
    fi

    log "Starting Airflow webserver..." "$YELLOW"
    if ! docker exec -d airflow airflow webserver; then
        log "Failed to start Airflow webserver." "$RED"
        log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
        cleanup
        exit 1
    fi

    # Wait for Airflow webserver
    wait_for_webserver "airflow" "8080" || {
        log "Airflow webserver failed to start." "$RED"
        cleanup
        exit 1
    }
}

# Add Airflow connections
add_airflow_connections() {
    log "Adding Airflow connections..." "$YELLOW"

    # MySQL connection
    log "Adding MySQL connection..." "$YELLOW"
    if docker exec airflow airflow connections get mysql_project_connection >/dev/null 2>&1; then
        log "MySQL connection already exists." "$GREEN"
    else
        if ! docker exec airflow bash -c "
            airflow connections add 'mysql_project_connection' \
                --conn-type mysql \
                --conn-login '${PROJECT_USER}' \
                --conn-password '${PROJECT_PASSWORD}' \
                --conn-host '${DB_HOST}' \
                --conn-port '${DB_PORT}' \
                --conn-schema '${DB_NAME}' \
                --conn-description 'Project Connection' >/dev/null 2>&1"; then
            log "Failed to add MySQL connection." "$RED"
            log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
            cleanup
            exit 1
        fi
        log "MySQL connection added." "$GREEN"
    fi

    # MongoDB connection
    log "Adding MongoDB connection..." "$YELLOW"
    if docker exec airflow airflow connections get mongodb_project_connection >/dev/null 2>&1; then
        log "MongoDB connection already exists." "$GREEN"
    else
        if ! docker exec airflow bash -c "
            airflow connections add 'mongodb_project_connection' \
                --conn-type mongo \
                --conn-login '${PROJECT_USER}' \
                --conn-password '${PROJECT_PASSWORD}' \
                --conn-host '${MONGO_HOST}' \
                --conn-port '${MONGO_PORT}' \
                --conn-schema '${MONGO_DB}' \
                --conn-description 'Project Connection' >/dev/null 2>&1"; then
            log "Failed to add MongoDB connection." "$RED"
            log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
            cleanup
            exit 1
        fi
        log "MongoDB connection added." "$GREEN"
    fi

    # MinIO connection
    log "Adding MinIO connection..." "$YELLOW"
    if docker exec airflow airflow connections get minio_project_connection >/dev/null 2>&1; then
        log "MinIO connection already exists." "$GREEN"
    else
        if ! docker exec airflow bash -c "
            airflow connections add 'minio_project_connection' \
                --conn-type aws \
                --conn-login '${PROJECT_USER}' \
                --conn-password '${PROJECT_PASSWORD}' \
                --conn-host 'http://${MINIO_HOST}:${MINIO_PORT}' \
                --conn-extra '{\"aws_access_key_id\": \"${PROJECT_USER}\", \"aws_secret_access_key\": \"${PROJECT_PASSWORD}\", \"endpoint_url\": \"http://${MINIO_HOST}:${MINIO_PORT}\"}' \
                --conn-description 'Project Connection' >/dev/null 2>&1"; then
            log "Failed to add MinIO connection." "$RED"
            log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
            cleanup
            exit 1
        fi
        log "MinIO connection added." "$GREEN"
    fi

    # MinIO Bucket Variable
    log "Adding MinIO bucket variable..." "$YELLOW"
    if docker exec airflow airflow variables get minio_bucket >/dev/null 2>&1; then
        log "MinIO bucket variable already exists." "$GREEN"
    else
        if ! docker exec airflow bash -c "
            airflow variables set 'minio_bucket' '${MINIO_BUCKET}' >/dev/null 2>&1"; then
            log "Failed to add MinIO bucket variable." "$RED"
            log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
            cleanup
            exit 1
        fi
        log "MinIO bucket variable added." "$GREEN"
    fi

    # Kafka connection
    log "Adding Kafka connection..." "$YELLOW"
    if docker exec airflow airflow connections get kafka_project_connection >/dev/null 2>&1; then
        log "Kafka connection already exists." "$GREEN"
    else
        if ! docker exec airflow bash -c "
            airflow connections add 'kafka_project_connection' \
                --conn-type kafka \
                --conn-extra '{\"bootstrap.servers\": \"${KAFKA_BOOTSTRAP_SERVERS}\"}' \
                --conn-description 'Project Connection' >/dev/null 2>&1"; then
            log "Failed to add Kafka connection." "$RED"
            log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
            cleanup
            exit 1
        fi
        log "Kafka connection added." "$GREEN"
    fi
}

# Verify Airflow health
verify_airflow_health() {
    log "Verifying Airflow health..." "$YELLOW"
    for ((i=1; i<=60; i++)); do
        local health
        health=$(get_container_health airflow)
        log "Airflow health: ${health}" "$YELLOW"
        if [[ "$health" == "healthy" ]]; then
            log "Airflow is healthy." "$GREEN"
            return 0
        fi
        log "Airflow not healthy, retrying (${i}/60)..." "$YELLOW"
        sleep 5
    done
    log "Airflow failed to become healthy." "$RED"
    log "Airflow logs:\n$(get_container_logs airflow)" "$RED"
    cleanup
    exit 1
}

# Setup MinIO bucket
setup_minio() {
    log "Setting up MinIO bucket and notifications..." "$YELLOW"

    log "Setting MinIO alias and creating bucket '${MINIO_BUCKET}'..." "$YELLOW"
    if ! docker exec -t minio bash -c "
        mc --no-color alias set local http://localhost:9000 '${PROJECT_USER}' '${PROJECT_PASSWORD}' 2>/dev/null &&
        mc --no-color mb local/'${MINIO_BUCKET}' --ignore-existing 2>/dev/null"; then
        log "Failed to set alias or create bucket." "$RED"
        log "MinIO logs:\n$(get_container_logs minio)" "$RED"
        cleanup
        exit 1
    fi
    log "MinIO bucket '${MINIO_BUCKET}' created." "$GREEN"
}

# Prompt user to exit or stop containers
prompt_user_action() {
    log "All services are running. What would you like to do?" "$YELLOW"
    echo "1) Exit the script (keep containers running)"
    echo "2) Stop Docker containers"
    local choice
    read -p "Enter your choice (1 or 2): " choice
    choice=$(echo "$choice" | tr -d '[:space:]')  # Trim whitespace and newlines

    case "$choice" in
        1)
            log "Exiting script. Containers are still running." "$GREEN"
            exit 0
            ;;
        2)
            log "Stopping Docker containers..." "$YELLOW"
            cleanup
            log "Containers stopped. Exiting script." "$GREEN"
            exit 0
            ;;
        *)
            log "Invalid choice. Please enter 1 or 2." "$RED"
            prompt_user_action  # Retry on invalid input
            ;;
    esac
}

# Check existing connections and health
check_existing_setup() {
    log "Checking existing setup..." "$YELLOW"
    
    # Start containers if stopped
    start_compose
    
    # Monitor container health
    monitor_containers
    
    # Verify connections
    initialize_airflow
    verify_airflow_health
}

# Main execution
main() {
    # Trap signals for cleanup
    trap cleanup SIGINT SIGTERM

    # Load environment variables
    load_env

    # Check if containers exist but are stopped
    if check_stopped_compose; then
        log "Detected stopped containers. Verifying existing setup..." "$YELLOW"
        check_existing_setup
    else
        # Start Docker Compose
        start_compose

        # Monitor containers
        monitor_containers

        # Initialize services
        verify_mysql
        create_kafka_topic
        initialize_airflow
        add_airflow_connections
        verify_airflow_health
        setup_minio
    fi

    log "All services initialized successfully!" "$GREEN"

    # Prompt user for next action
    prompt_user_action
}

# Run main function
main
exit 0