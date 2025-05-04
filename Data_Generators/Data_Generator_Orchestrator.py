import asyncio
import random
import logging
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from aiohttp import web
import os
import threading

# Dynamic log file name with timestamp, stored in mounted volume
log_file_name = f"/app/logs/Data_Generator_Orchestrator_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_name),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Thread lock for safe status updates
status_lock = threading.Lock()

# Global status dictionary to track script execution
script_status: Dict[str, Dict[str, Any]] = {
    "CSV_Data_Generator": {
        "last_run": None,
        "last_input": None,
        "status": "Stopped",
        "last_error": None,
        "last_log_file": None,
        "last_duration": None,
        "next_run": None,
        "running": False,
        "task": None
    },
    "JSON_Data_Generator": {
        "last_run": None,
        "last_input": None,
        "status": "Stopped",
        "last_error": None,
        "last_log_file": None,
        "last_duration": None,
        "next_run": None,
        "running": False,
        "task": None
    },
    "XML_Data_Generator": {
        "last_run": None,
        "last_input": None,
        "status": "Stopped",
        "last_error": None,
        "last_log_file": None,
        "last_duration": None,
        "next_run": None,
        "running": False,
        "task": None
    },
    "Kafka_Stream_Processing": {
        "last_run": None,
        "last_input": "NA",
        "status": "Stopped",
        "last_error": None,
        "last_log_file": None,
        "last_duration": "NA",
        "next_run": "NA",
        "running": False,
        "thread": None,
        "process": None
    }
}

# HTML template with escaped curly braces for CSS
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Generator Workflow Monitor</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        h1 {{ color: #333; text-align: center; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; background-color: #fff; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        th, td {{ padding: 10px; text-align: left; border: 1px solid #ddd; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        tr:hover {{ background-color: #f1f1f1; }}
        .status-running {{ color: #FFA500; }}
        .status-completed {{ color: #008000; }}
        .status-failed {{ color: #FF0000; }}
        .status-stopped {{ color: #808080; }}
        a {{ color: #0066cc; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .toggle-btn {{ padding: 5px 10px; border: none; cursor: pointer; font-size: 14px; border-radius: 4px; }}
        .start-btn {{ background-color: #4CAF50; color: white; }}
        .stop-btn {{ background-color: #FF0000; color: white; }}
        .toggle-btn:hover {{ opacity: 0.9; }}
    </style>
</head>
<body>
    <h1>Data Generator Workflow Monitor</h1>
    <table>
        <tr>
            <th>Script Name</th>
            <th>Control</th>
            <th>Status</th>
            <th>Last Run</th>
            <th>Last Input</th>
            <th>Duration (s)</th>
            <th>Next Run In (s)</th>
            <th>Last Error</th>
            <th>Log File</th>
        </tr>
        {rows}
    </table>
    <script>
        function updateCountdown() {{
            const now = new Date().getTime() / 1000;
            document.querySelectorAll('.countdown').forEach(cell => {{
                const nextRun = parseFloat(cell.getAttribute('data-next-run'));
                const secondsLeft = isNaN(nextRun) ? 'N/A' : Math.max(0, Math.round(nextRun - now));
                cell.textContent = secondsLeft;
            }});
        }}
        setInterval(updateCountdown, 1000);
        updateCountdown();

        async function toggleScript(scriptName, action) {{
            const response = await fetch(`/toggle?script=${{scriptName}}&action=${{action}}`, {{ method: 'POST' }});
            if (response.ok) {{
                location.reload();
            }} else {{
                alert('Failed to toggle script');
            }}
        }}
    </script>
</body>
</html>
"""

async def run_script(script_name: str, input_value: int) -> None:
    """Execute a generator script with the given input and update its status."""
    script_path = f"/app/scripts/{script_name}.py"
    log_file = f"/app/logs/{script_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
    logger.info(f"Starting {script_name} with input {input_value}, logging to {log_file}")
    
    with status_lock:
        script_status[script_name]["status"] = "Running"
        script_status[script_name]["last_run"] = datetime.now(timezone.utc)
        script_status[script_name]["last_input"] = input_value
        script_status[script_name]["last_error"] = None
        script_status[script_name]["last_log_file"] = log_file
        script_status[script_name]["next_run"] = None

    start_time = datetime.now(timezone.utc)
    try:
        env = os.environ.copy()
        env["SCRIPT_LOG_FILE"] = log_file
        for var in ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD",
                    "MINIO_HOST", "MINIO_PORT", "MINIO_USER", "MINIO_PASSWORD", "MINIO_BUCKET",
                    "MONGO_HOST", "MONGO_PORT", "MONGO_DB", "MONGO_USER", "MONGO_PASSWORD",
                    "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC_NAME"]:
            if var in os.environ:
                env[var] = os.environ[var]

        process = await asyncio.create_subprocess_exec(
            "python", script_path, str(input_value),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
        )
        stdout, stderr = await process.communicate()
        
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        with status_lock:
            script_status[script_name]["last_duration"] = round(duration, 2)

        if process.returncode == 0:
            logger.info(f"{script_name} completed successfully in {duration:.2f} seconds")
            with status_lock:
                script_status[script_name]["status"] = "Completed"
        else:
            error_msg = stderr.decode().strip()
            logger.error(f"{script_name} failed with error: {error_msg}")
            with status_lock:
                script_status[script_name]["status"] = "Failed"
                script_status[script_name]["last_error"] = error_msg
    except Exception as e:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        with status_lock:
            script_status[script_name]["last_duration"] = round(duration, 2)
            logger.error(f"Exception while running {script_name}: {str(e)}")
            script_status[script_name]["status"] = "Failed"
            script_status[script_name]["last_error"] = str(e)

def run_Kafka_Stream_Processing(script_name: str) -> None:
    """Run Kafka_Stream_Processing script in a separate thread."""
    script_path = f"/app/scripts/{script_name}.py"
    log_file = f"/app/logs/{script_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
    logger.info(f"Starting {script_name}, logging to {log_file}")
    
    with status_lock:
        script_status[script_name]["status"] = "Running"
        script_status[script_name]["last_run"] = datetime.now(timezone.utc)
        script_status[script_name]["last_error"] = None
        script_status[script_name]["last_log_file"] = log_file

    try:
        env = os.environ.copy()
        env["SCRIPT_LOG_FILE"] = log_file
        for var in ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD",
                    "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC_NAME"]:
            if var in os.environ:
                env[var] = os.environ[var]

        process = subprocess.Popen(
            ["python", script_path],
            env=env,
            stdout=open(log_file, "a"),
            stderr=subprocess.STDOUT,
            text=True
        )
        with status_lock:
            script_status[script_name]["process"] = process
        
        return_code = process.wait()
        
        with status_lock:
            if return_code != 0:
                logger.error(f"{script_name} exited with code {return_code}")
                script_status[script_name]["status"] = "Failed"
                script_status[script_name]["last_error"] = f"Exited with code {return_code}"
            else:
                logger.info(f"{script_name} completed unexpectedly")
                script_status[script_name]["status"] = "Stopped"
    except Exception as e:
        logger.error(f"Exception while running {script_name}: {str(e)}")
        with status_lock:
            script_status[script_name]["status"] = "Failed"
            script_status[script_name]["last_error"] = str(e)
    finally:
        with status_lock:
            script_status[script_name]["running"] = False
            script_status[script_name]["process"] = None
            script_status[script_name]["thread"] = None

async def script_runner(script_name: str) -> None:
    """Run a script at random intervals with random inputs until stopped."""
    while script_status[script_name]["running"]:
        input_value = random.randint(100, 100_000)
        delay = random.randint(30, 300)

        await run_script(script_name, input_value)
        
        with status_lock:
            next_run_time = datetime.now(timezone.utc) + timedelta(seconds=delay)
            script_status[script_name]["next_run"] = next_run_time.timestamp()
        
        logger.info(f"{script_name} scheduled to run again in {delay} seconds")
        await asyncio.sleep(delay)
    
    logger.info(f"{script_name} stopped")
    with status_lock:
        script_status[script_name]["status"] = "Stopped"
        script_status[script_name]["next_run"] = None

async def index_handler(request: web.Request) -> web.Response:
    """Handle HTTP GET requests to the root (/), returning the monitor interface."""
    rows = ""
    for script_name, status in script_status.items():
        status_class = f"status-{status['status'].lower()}"
        log_link = f"<a href=\"/logs?file={status['last_log_file']}\">Download</a>" if status['last_log_file'] else "N/A"
        next_run = status['next_run'] if status['next_run'] != "NA" else 0
        
        last_run_str = (
            status['last_run'].strftime("%B %d, %Y %H:%M:%S %Z")
            if status['last_run'] else "N/A"
        )
        
        if status["running"]:
            button = f"<button class=\"toggle-btn stop-btn\" onclick=\"toggleScript('{script_name}', 'stop')\">Stop</button>"
        else:
            button = f"<button class=\"toggle-btn start-btn\" onclick=\"toggleScript('{script_name}', 'start')\">Start</button>"
        
        rows += (
            f"<tr>"
            f"<td>{script_name}</td>"
            f"<td>{button}</td>"
            f"<td class=\"{status_class}\">{status['status']}</td>"
            f"<td>{last_run_str}</td>"
            f"<td>{status['last_input']}</td>"
            f"<td>{status['last_duration']}</td>"
            f"<td class=\"countdown\" data-next-run=\"{next_run}\">{status['next_run']}</td>"
            f"<td>{status['last_error'] or 'None'}</td>"
            f"<td>{log_link}</td>"
            f"</tr>"
        )
    html = HTML_TEMPLATE.format(rows=rows)
    return web.Response(text=html, content_type="text/html")

async def log_handler(request: web.Request) -> web.Response:
    """Handle HTTP GET requests to /logs, serving the requested log file."""
    file_path = request.query.get("file")
    if not file_path or not os.path.exists(file_path) or not file_path.startswith("/app/logs/"):
        raise web.HTTPNotFound(text="Log file not found or invalid path")
    
    return web.FileResponse(
        path=file_path,
        headers={
            "Content-Disposition": f"attachment; filename={os.path.basename(file_path)}"
        }
    )

async def favicon_handler(request: web.Request) -> web.Response:
    """Handle requests for favicon.ico."""
    raise web.HTTPNotFound()

async def status_handler(request: web.Request) -> web.Response:
    """Handle HTTP GET requests to /status, returning the current status as JSON."""
    return web.json_response(script_status)

async def toggle_handler(request: web.Request) -> web.Response:
    """Handle HTTP POST requests to /toggle to start or stop a script."""
    script_name = request.query.get("script")
    action = request.query.get("action")
    
    if script_name not in script_status:
        raise web.HTTPBadRequest(text=f"Invalid script name: {script_name}")
    
    if action not in ["start", "stop"]:
        raise web.HTTPBadRequest(text=f"Invalid action: {action}")
    
    with status_lock:
        if action == "start" and not script_status[script_name]["running"]:
            logger.info(f"Starting {script_name}")
            script_status[script_name]["running"] = True
            if script_name == "Kafka_Stream_Processing":
                # Start Kafka_Stream_Processing in a separate thread
                thread = threading.Thread(target=run_Kafka_Stream_Processing, args=(script_name,))
                thread.start()
                script_status[script_name]["thread"] = thread
            else:
                # Start other scripts as asyncio tasks
                script_status[script_name]["task"] = asyncio.create_task(script_runner(script_name))
            return web.Response(text="Script started")
        
        if action == "stop" and script_status[script_name]["running"]:
            logger.info(f"Stopping {script_name}")
            script_status[script_name]["running"] = False
            if script_name == "Kafka_Stream_Processing" and script_status[script_name]["process"]:
                # Terminate Kafka_Stream_Processing process
                script_status[script_name]["process"].terminate()
                try:
                    script_status[script_name]["process"].wait(timeout=5)
                except subprocess.TimeoutExpired:
                    script_status[script_name]["process"].kill()
                script_status[script_name]["process"] = None
                # Thread will exit after process terminates
                if script_status[script_name]["thread"]:
                    script_status[script_name]["thread"].join(timeout=5)
                    script_status[script_name]["thread"] = None
            if script_status[script_name]["task"]:
                script_status[script_name]["task"].cancel()
                try:
                    await script_status[script_name]["task"]
                except asyncio.CancelledError:
                    pass
                script_status[script_name]["task"] = None
            script_status[script_name]["status"] = "Stopped"
            script_status[script_name]["next_run"] = "NA" if script_name == "Kafka_Stream_Processing" else None
            return web.Response(text="Script stopped")
    
    return web.Response(text="No action taken")

async def start_http_server() -> None:
    """Start an HTTP server on port 1212 to expose the monitor interface."""
    app = web.Application()
    app.add_routes([
        web.get('/', index_handler),
        web.get('/favicon.ico', favicon_handler),
        web.get('/status', status_handler),
        web.get('/logs', log_handler),
        web.post('/toggle', toggle_handler)
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 1212)
    await site.start()
    logger.info("HTTP server started on port 1212")

async def main() -> None:
    """Main function to start the HTTP server."""
    logger.info("Starting Data Generator Orchestrator")

    # Validate script presence
    scripts = ["CSV_Data_Generator", "JSON_Data_Generator", "XML_Data_Generator", "Kafka_Stream_Processing"]
    for script in scripts:
        script_path = f"/app/scripts/{script}.py"
        if not os.path.exists(script_path):
            logger.warning(f"Script {script_path} not found, skipping")
            with status_lock:
                script_status[script]["status"] = "Not Found"
                script_status[script]["last_error"] = "Script file missing"
        else:
            logger.info(f"Found script: {script_path}")

    # Start HTTP server
    await start_http_server()

    # Keep the event loop running
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())