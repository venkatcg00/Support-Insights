"""
Parent script to orchestrate execution of data generator scripts with manual control.

This script runs an HTTP server on port 1212 with a web interface resembling an Informatica Workflow Monitor,
showing script status, run details, countdown to next run, and links to download log files.
Each script (CSV_Data_Generator.py, JSON_Data_Generator.py, XML_Data_Generator.py) has a start/stop toggle button.
When started, scripts run asynchronously with random inputs and delays. When stopped, execution halts.

Example (run inside python-runner container):
    $ python /app/scripts/Data_Generator_Orchestrator.py
    # Starts the HTTP server.
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
        "status": "Stopped",
        "last_error": None,
        "last_log_file": None,
        "last_duration": None,  # Duration in seconds
        "next_run": None,       # Timestamp of next scheduled run
        "running": False,       # Tracks if the script is actively running
        "task": None            # Stores asyncio Task for cancellation
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
    }
}

# HTML template with escaped curly braces and start/stop toggle buttons
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
        .status-stopped {{ color: #808080; }}
        a {{
            color: #0066cc;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
        .toggle-btn {{
            padding: 5px 10px;
            border: none;
            cursor: pointer;
            font-size: 14px;
            border-radius: 4px;
        }}
        .start-btn {{
            background-color: #4CAF50;
            color: white;
        }}
        .stop-btn {{
            background-color: #FF0000;
            color: white;
        }}
        .toggle-btn:hover {{
            opacity: 0.9;
        }}
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
    """Run a script at random intervals with random inputs until stopped."""
    while script_status[script_name]["running"]:
        input_value = random.randint(1, 1000)  # Random natural number input (1 to 1000)
        delay = random.randint(30, 300)        # Random delay between 30 seconds and 5 minutes

        await run_script(script_name, input_value)
        
        # Schedule next run and store timestamp
        next_run_time = datetime.now(timezone.utc) + timedelta(seconds=delay)
        script_status[script_name]["next_run"] = next_run_time.timestamp()
        
        logger.info(f"{script_name} scheduled to run again in {delay} seconds")
        await asyncio.sleep(delay)
    
    logger.info(f"{script_name} stopped")
    script_status[script_name]["status"] = "Stopped"
    script_status[script_name]["next_run"] = None

async def index_handler(request: web.Request) -> web.Response:
    """Handle HTTP GET requests to the root (/), returning the monitor interface."""
    rows = ""
    for script_name, status in script_status.items():
        status_class = f"status-{status['status'].lower()}"
        log_link = f"<a href=\"/logs?file={status['last_log_file']}\">Download</a>" if status['last_log_file'] else "N/A"
        next_run = status['next_run'] if status['next_run'] else 0
        
        # Format last_run as human-readable with timezone
        last_run_str = (
            status['last_run'].strftime("%B %d, %Y %H:%M:%S %Z")
            if status['last_run'] else "N/A"
        )
        
        # Start/Stop button
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
    
    if action == "start" and not script_status[script_name]["running"]:
        logger.info(f"Starting {script_name}")
        script_status[script_name]["running"] = True
        script_status[script_name]["task"] = asyncio.create_task(script_runner(script_name))
        return web.Response(text="Script started")
    
    if action == "stop" and script_status[script_name]["running"]:
        logger.info(f"Stopping {script_name}")
        script_status[script_name]["running"] = False
        if script_status[script_name]["task"]:
            script_status[script_name]["task"].cancel()
            try:
                await script_status[script_name]["task"]
            except asyncio.CancelledError:
                pass
            script_status[script_name]["task"] = None
        script_status[script_name]["status"] = "Stopped"
        script_status[script_name]["next_run"] = None
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
    scripts = ["CSV_Data_Generator", "JSON_Data_Generator", "XML_Data_Generator"]
    for script in scripts:
        script_path = f"/app/scripts/{script}.py"
        if not os.path.exists(script_path):
            logger.error(f"Script {script_path} not found")
            sys.exit(1)

    # Start HTTP server
    await start_http_server()

    # Keep the event loop running
    while True:
        await asyncio.sleep(3600)  # Sleep to prevent tight loop

if __name__ == "__main__":
    asyncio.run(main())