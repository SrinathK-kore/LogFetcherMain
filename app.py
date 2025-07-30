


from flask import Flask, request, jsonify, send_file, render_template
from concurrent.futures import ThreadPoolExecutor, Future
from cs_logs_webapp import run_log_fetch
from other_logs import run_other_logs_fetch
import os
import uuid
import threading
import time

app = Flask(__name__)

# Background task management
executor = ThreadPoolExecutor(max_workers=10)
tasks = {}  # task_id -> future
task_metadata = {}  # task_id -> {start_time, status, result_path, error, cancel_event}

def background_wrapper(task_id, func, cancel_event, *args):
    print(f"[DEBUG] Starting background task: {task_id}")
    task_metadata[task_id] = {
        "status": "in_progress",
        "start_time": time.time(),
        "cancel_event": cancel_event,
        "progress": 0,
        "result_path": None,
        "error": None
    }
    try:
        result_path = func(*args, cancel_event=cancel_event, task_id=task_id, task_metadata_ref=task_metadata)
        if cancel_event.is_set() or result_path is None:
            print(f"[DEBUG] Task {task_id} was cancelled or returned None")
            task_metadata[task_id]["status"] = "cancelled"
            task_metadata[task_id]["result_path"] = None
            return
        task_metadata[task_id]["status"] = "completed"
        task_metadata[task_id]["result_path"] = result_path
        print(f"[DEBUG] Task {task_id} completed successfully")
    except Exception as e:
        if cancel_event.is_set():
            print(f"[DEBUG] Task {task_id} was cancelled with exception: {e}")
            task_metadata[task_id]["status"] = "cancelled"
        else:
            print(f"[ERROR] Task {task_id} failed with error: {e}")
            task_metadata[task_id]["status"] = "failed"
            task_metadata[task_id]["error"] = str(e)

def submit_task(func, *args):
    task_id = str(uuid.uuid4())
    print(f"[DEBUG] Submitting new task: {task_id}")
    cancel_event = threading.Event()
    future = executor.submit(background_wrapper, task_id, func, cancel_event, *args)
    tasks[task_id] = future
    return task_id

@app.route("/")
def index():
    print("[DEBUG] Serving index page")
    return render_template("form.html")

@app.route("/download_cs")
def download_cs_logs():
    date = request.args.get("date")
    env = int(request.args.get("env"))
    sidcid = request.args.get("sidcid")
    lang = int(request.args.get("lang"))

    print(f"[DEBUG] Received CS logs request - Date: {date}, Env: {env}, SIDCID: {sidcid}, Lang: {lang}")
    task_id = submit_task(run_log_fetch, date, env, sidcid, lang)
    return jsonify({"task_id": task_id, "status": "in_progress"})

@app.route("/download_other")
def download_other_logs():
    date = request.args.get("date")
    env = int(request.args.get("env"))
    selected_logs = request.args.getlist("logs")

    print(f"[DEBUG] Received Other logs request - Date: {date}, Env: {env}, Logs: {selected_logs}")
    if not selected_logs:
        print("[ERROR] No log types selected for other logs")
        return jsonify({"error": "No log types selected."}), 400

    task_id = submit_task(run_other_logs_fetch, date, env, selected_logs)
    return jsonify({"task_id": task_id, "status": "in_progress"})

@app.route("/status/<task_id>")
def get_status(task_id):
    meta = task_metadata.get(task_id)
    if not meta:
        print(f"[WARN] Status requested for unknown task ID: {task_id}")
        return jsonify({"status": "not_found"}), 404

    elapsed = int(time.time() - meta["start_time"])
    print(f"[DEBUG] Status check for task {task_id} - Status: {meta['status']}, Elapsed: {elapsed}s")
    return jsonify({
        "task_id": task_id,
        "status": meta["status"],
        "elapsed_seconds": elapsed,
        "result": meta.get("result_path"),
        "error": meta.get("error"),
        "progress": meta.get("progress", 0)
    })

@app.route("/download/<task_id>")
def download_result(task_id):
    meta = task_metadata.get(task_id)
    if not meta:
        print(f"[ERROR] Download requested for invalid task ID: {task_id}")
        return "Invalid task ID", 404
    if meta["status"] != "completed" or not meta.get("result_path"):
        print(f"[WARN] Download attempted before completion for task: {task_id}")
        return "Task not completed or result unavailable", 400

    print(f"[DEBUG] Serving download for task: {task_id}")
    return send_file(
        meta["result_path"],
        as_attachment=True,
        download_name=os.path.basename(meta["result_path"]),
        mimetype='application/zip'
    )

@app.route("/cancel/<task_id>", methods=["POST"])
def cancel_task(task_id):
    future = tasks.get(task_id)
    if not future:
        print(f"[ERROR] Cancel requested for unknown task: {task_id}")
        return jsonify({"error": "Task not found."}), 404

    meta = task_metadata.get(task_id)
    if not meta:
        print(f"[ERROR] Task metadata missing for cancellation: {task_id}")
        return jsonify({"error": "Task metadata missing."}), 404

    if meta["status"] in ["completed", "failed", "cancelled"]:
        print(f"[INFO] Cancellation ignored. Task {task_id} already {meta['status']}")
        return jsonify({"message": f"Task already {meta['status']}"})

    cancel_event = meta.get("cancel_event")
    if cancel_event:
        cancel_event.set()
        print(f"[DEBUG] Cancellation triggered for task: {task_id}")
        return jsonify({"message": "Task cancellation initiated."})
    else:
        print(f"[ERROR] Cancel event missing for task: {task_id}")
        return jsonify({"error": "Cancel event not available."}), 500

if __name__ == "__main__":
    print("[DEBUG] Starting Flask app on port 5051")
    app.run(debug=True, port=5051, threaded=True)
