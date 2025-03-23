"""Parent script to orchestrate execution of data generator scripts at random intervals.

This script runs CSV_Data_Generator.py, JSON_Data_Generator.py, and XML_Data_Generator.py
asynchronously with random natural number inputs and random delays between executions.
It exposes an HTTP server on port 1212 with a web interface resembling an Informatica Workflow Monitor,
showing script status, run details, countdown to next run, and links to download log files.

Example (run inside python-runner container):
    $ python /app/scripts/Data_Generator_Orchestrator.py
    # Starts the orchestrator and HTTP server.
    # Access monitor: http://localhost:1212/
"""

import asyncio
import random
import logging
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from aiohttp import web
import os
import json
import sys

# Dynamic log file name with timestamp, stored in mounted volume
log_file_name = f"/app/logs/Data_Generator_Orchestrator_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_name),
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)

# Global status dictionary to track script execution
script_status: Dict[str, Dict[str, Any]] = {
    "CSV_Data_Generator": {
        "last_run": None,
        "last_input": None,
        "status": "Idle",
        "last_error": None,
        "last_log_file": None,
        "last_duration": None,  # Duration in seconds
        "next_run": None        # Timestamp of next scheduled run
    },
    "JSON_Data_Generator": {
        "last_run": None,
        "last_input": None,
        "status": "Idle",
        "last_error": None,
        "last_log_file": None,
        "last_duration": None,
        "next_run": None
    },
    "XML_Data_Generator": {
        "last_run": None,
        "last_input": None,
        "status": "Idle",
        "last_error": None,
        "last_log_file": None,
        "last_duration": None,
        "next_run": None
    }
}

# HTML template with refresh button and countdown timer
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Generator Workflow Monitor</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
            text-align: center;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            background-color: #fff;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 10px;
            text-align: left;
            border: 1px solid #ddd;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        tr:hover {{
            background-color: #f1f1f1;
        }}
        .status-running {{ color: #FFA500; }}
        .status-completed {{ color: #008000; }}
        .status-failed {{ color: #FF0000; }}
        .status-idle {{ color: #808080; }}
        a {{
            color: #0066cc;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
        .refresh-btn {{
            margin: 10px auto;
            display: block;
            padding: 10px 20px;
            background-color: #4CAF50;
            color: white;
            border: none;
            cursor: pointer;
            font-size: 16px;
        }}
        .refresh-btn:hover {{
            background-color: #45a049;
        }}
    </style>
</head>
<body>
    <h1>Data Generator Workflow Monitor</h1>
    <button class="refresh-btn" onclick="location.reload()">Refresh</button>
    <table>
        <tr>
            <th>Script Name</th>
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
            const now = new Date().getTime() / 1000; // Current time in seconds
            document.querySelectorAll('.countdown').forEach(cell => {{
                const nextRun = parseFloat(cell.getAttribute('data-next-run'));
                const secondsLeft = Math.max(0, Math.round(nextRun - now));
                cell.textContent = secondsLeft > 0 ? secondsLeft : 'N/A';
            }});
        }}
        // Update countdown every second
        setInterval(updateCountdown, 1000);
        // Initial update
        updateCountdown();
    </script>
</body>
</html>
"""

async def run_script(script_name: str, input_value: int) -> None:
    """Execute a generator script with the given input and update its status."""
    script_path = f"/app/scripts/{script_name}.py"
    log_file = f"/app/logs/{script_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
    logger.info(f"Starting {script_name} with input {input_value}, logging to {log_file}")
    
    script_status[script_name]["status"] = "Running"
    script_status[script_name]["last_run"] = datetime.now(timezone.utc)  # Store as timezone-aware datetime
    script_status[script_name]["last_input"] = input_value
    script_status[script_name]["last_error"] = None
    script_status[script_name]["last_log_file"] = log_file
    script_status[script_name]["next_run"] = None  # Reset until scheduled

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
        script_status[script_name]["last_duration"] = round(duration, 2)

        if process.returncode == 0:
            logger.info(f"{script_name} completed successfully in {duration:.2f} seconds")
            script_status[script_name]["status"] = "Completed"
        else:
            error_msg = stderr.decode().strip()
            logger.error(f"{script_name} failed with error: {error_msg}")
            script_status[script_name]["status"] = "Failed"
            script_status[script_name]["last_error"] = error_msg
    except Exception as e:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        script_status[script_name]["last_duration"] = round(duration, 2)
        logger.error(f"Exception while running {script_name}: {str(e)}", exc_info=True)
        script_status[script_name]["status"] = "Failed"
        script_status[script_name]["last_error"] = str(e)

async def script_runner(script_name: str) -> None:
    """Run a script at random intervals with random inputs indefinitely."""
    while True:
        input_value = random.randint(1, 1000)  # Random natural number input (1 to 1000)
        delay = random.randint(30, 300)        # Random delay between 30 seconds and 5 minutes

        await run_script(script_name, input_value)
        
        # Schedule next run and store timestamp
        next_run_time = datetime.now(timezone.utc) + timedelta(seconds=delay)
        script_status[script_name]["next_run"] = next_run_time.timestamp()  # Unix timestamp in seconds
        
        logger.info(f"{script_name} scheduled to run again in {delay} seconds")
        await asyncio.sleep(delay)

async def index_handler(request: web.Request) -> web.Response:
    """Handle HTTP GET requests to the root (/), returning the monitor interface."""
    rows = ""
    for script_name, status in script_status.items():
        status_class = f"status-{status['status'].lower()}"
        log_link = f"<a href=\"/logs?file={status['last_log_file']}\">Download</a>" if status['last_log_file'] else "N/A"
        next_run = status['next_run'] if status['next_run'] else 0  # Default to 0 if not set
        
        # Format last_run as human-readable with timezone (e.g., "March 23, 2025 06:00:59 UTC")
        last_run_str = (
            status['last_run'].strftime("%B %d, %Y %H:%M:%S %Z")
            if status['last_run'] else "N/A"
        )
        
        rows += (
            f"<tr>"
            f"<td>{script_name}</td>"
            f"<td class=\"{status_class}\">{status['status']}</td>"
            f"<td>{last_run_str}</td>"
            f"<td>{status['last_input'] or 'N/A'}</td>"
            f"<td>{status['last_duration'] or 'N/A'}</td>"
            f"<td class=\"countdown\" data-next-run=\"{next_run}\">N/A</td>"
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

async def status_handler(request: web.Request) -> web.Response:
    """Handle HTTP GET requests to /status, returning the current status as JSON."""
    return web.json_response(script_status)

async def start_http_server() -> None:
    """Start an HTTP server on port 1212 to expose the monitor interface."""
    app = web.Application()
    app.add_routes([
        web.get('/', index_handler),
        web.get('/status', status_handler),
        web.get('/logs', log_handler)
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 1212)
    await site.start()
    logger.info("HTTP server started on port 1212")

async def main() -> None:
    """Main function to start the orchestrator and HTTP server."""
    logger.info("Starting Data Generator Orchestrator")

    # Validate script presence
    scripts = ["CSV_Data_Generator", "JSON_Data_Generator", "XML_Data_Generator"]
    for script in scripts:
        script_path = f"/app/scripts/{script}.py"
        if not os.path.exists(script_path):
            logger.error(f"Script {script_path} not found")
            sys.exit(1)

    # Start HTTP server
    await start_http_server()

    # Start script runners concurrently
    tasks = [asyncio.create_task(script_runner(script)) for script in scripts]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())