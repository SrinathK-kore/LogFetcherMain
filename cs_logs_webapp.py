import os
import subprocess
import shutil
import multiprocessing as mp
from datetime import datetime
import S3_accessCS
import shlex
import re
from dotenv import load_dotenv
import concurrent.futures
import atexit

# Load environment variables from .env
load_dotenv()

executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
atexit.register(executor.shutdown)

def safe_submit(fn, *args, **kwargs):
    try:
        return executor.submit(fn, *args, **kwargs)
    except RuntimeError as e:
        print(f"[ERROR] Could not submit job: {e}")
        return None

def is_valid_sidcid(sidcid):
    return re.match(r"^st-[a-f0-9\-]+\+u-[a-f0-9\-]+$", sidcid) is not None

def extract_userid_from_sidcid(sidcid):
    return sidcid

def list_folders(bucket, path):
    result = subprocess.run([
        "aws", "s3", "ls", f"s3://{bucket}/{path}/"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print(f"[ERROR] Failed to list S3 path: s3://{bucket}/{path}/")
        print(f"[STDERR] {result.stderr}")
    return [line.split()[-1].rstrip("/") for line in result.stdout.splitlines() if line.endswith("/")]

def getLanguages(bucketName, cslogsPath, firstFolder):
    output = subprocess.run([
        "aws", "s3", "ls", f"s3://{bucketName}/{cslogsPath}/{firstFolder}/"
    ], stdout=subprocess.PIPE, text=True)
    return [line.split()[-1][:-1] for line in output.stdout.splitlines() if line.endswith("/")]

def downloadLogsAllLanguages(bucketName, cslogsPath, ipFolder, folderPath, pattern):
    target_path = os.path.join(folderPath, ipFolder)
    os.makedirs(target_path, exist_ok=True)
    cmd = f'aws s3 sync "s3://{bucketName}/{cslogsPath}/{ipFolder}" "{target_path}" --exclude "*" --include "*{pattern}*"'
    print(f"[DEBUG] Executing sync for IP: {ipFolder} with pattern: {pattern}")
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.stdout.strip():
        print(f"[MATCH] {ipFolder} - files matched")
        return True
    else:
        shutil.rmtree(target_path)
        print(f"[NO MATCH] {ipFolder} - no match")
        return False

def downloadSingleLanguage(bucketName, cslogsPath, langFolder, ipFolder, folderPath, pattern):
    target_path = os.path.join(folderPath, ipFolder, langFolder)
    os.makedirs(target_path, exist_ok=True)
    cmd = f'aws s3 cp --recursive "s3://{bucketName}/{cslogsPath}/{ipFolder}/{langFolder}" "{target_path}" --exclude "*" --include "*{pattern}*"'
    print(f"[DEBUG] Executing copy for IP: {ipFolder}, Lang: {langFolder} with pattern: {pattern}")
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.stdout.strip():
        print(f"[MATCH] {ipFolder}/{langFolder} - files matched")
        return True
    else:
        shutil.rmtree(target_path)
        print(f"[NO MATCH] {ipFolder}/{langFolder} - no match")
        return False

def parallel_download(jobs):
    futures = []
    for job in jobs:
        future = safe_submit(job[0], *job[1])
        if future:
            futures.append(future)
    for future in futures:
        try:
            future.result()
        except Exception as e:
            print(f"[ERROR] Job failed: {e}")

def searchAllLanguages(bucket, path, ipFolders, folder, pattern):
    jobs = [(downloadLogsAllLanguages, (bucket, path, ip, folder, pattern)) for ip in ipFolders]
    parallel_download(jobs)
    return any(os.path.exists(os.path.join(folder, ip)) for ip in ipFolders)

def searchSingleLanguage(lang, bucket, path, ipFolders, folder, pattern):
    jobs = [(downloadSingleLanguage, (bucket, path, lang, ip, folder, pattern)) for ip in ipFolders]
    parallel_download(jobs)
    return any(os.path.exists(os.path.join(folder, ip, lang)) for ip in ipFolders)

def run_log_fetch(date_str, env_id, sidcid, lang_choice, cancel_event=None, task_id=None, task_metadata_ref=None):
    print(f"[DEBUG] Date: {date_str}, Env: {env_id}, SID+CID: {sidcid}, Lang: {lang_choice}")
    mp.freeze_support()

    if not is_valid_sidcid(sidcid):
        raise ValueError("Invalid Bot ID + User ID format. Expected format: st-<uuid>+u-<uuid>  but you got is :" , sidcid )

    pattern = shlex.quote(sidcid)

    environments = ["US-PROD", "DE-PROD", "JP-PROD", "AU-PROD", "EU-PROD", "NTT Data PROD"]
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    selectedAccount = S3_accessCS.getEnvironment(date_obj, environments[env_id - 1])

    bucket = selectedAccount["bucketName"]
    root_cslogs_path = selectedAccount["cslogsPath"]

    folder_formats = [
        date_obj.strftime("%d-%b-%Y"),
        date_obj.strftime("%Y-%m-%d"),
        date_obj.strftime("%d_%b_%Y")
    ]

    if task_metadata_ref and task_id:
        task_metadata_ref[task_id]["progress"] = 10

    found_path = None
    folder = None
    for fmt in folder_formats:
        if cancel_event and cancel_event.is_set():
            print(f"[CANCELLED] Task {task_id} cancelled while checking paths")
            return
        full_path = f"{root_cslogs_path}{fmt}"
        print(f"[DEBUG] Checking path: s3://{bucket}/{full_path}/")
        ipFolders = list_folders(bucket, full_path)
        if ipFolders:
            found_path = full_path
            folder = fmt
            print(f"[INFO] Valid CS Logs path found: {found_path}")
            break

    if not found_path:
        raise Exception("No valid CSLogs path found in S3 for given date format.")

    subprocess.run(["aws", "configure", "set", "aws_access_key_id", selectedAccount["accessKey"]])
    subprocess.run(["aws", "configure", "set", "aws_secret_access_key", selectedAccount["secretKey"]])

    folderPath = f"/tmp/{folder.replace('/', '-').replace(' ', '_')}_CS_Logs_{sidcid}"
    os.makedirs(folderPath, exist_ok=True)

    print(f"[DEBUG] Using path: {found_path}")
    print(f"[DEBUG] IP folders: {ipFolders}")
    if not ipFolders:
        raise Exception("No IP folders found in S3 path.")

    langFolders = getLanguages(bucket, found_path, ipFolders[0])
    print(f"[DEBUG] Language Folders: {langFolders}")

    if cancel_event and cancel_event.is_set():
        print(f"[CANCELLED] Task {task_id} cancelled before download started")
        shutil.rmtree(folderPath, ignore_errors=True)
        return

    if task_metadata_ref and task_id:
        task_metadata_ref[task_id]["progress"] = 30

    matched = False
    if lang_choice == len(langFolders) + 1:
        print("[INFO] Fetching all languages...")
        matched = searchAllLanguages(bucket, found_path, ipFolders, folderPath, pattern)
    else:
        langFolder = langFolders[lang_choice - 1]
        print(f"[INFO] Fetching single language: {langFolder}")
        matched = searchSingleLanguage(langFolder, bucket, found_path, ipFolders, folderPath, pattern)

    if cancel_event and cancel_event.is_set():
        print(f"[CANCELLED] Task {task_id} cancelled after download")
        shutil.rmtree(folderPath, ignore_errors=True)
        return

    if not matched:
        shutil.rmtree(folderPath)
        raise Exception("No logs matched the provided Bot ID + User ID.")

    if task_metadata_ref and task_id:
        task_metadata_ref[task_id]["progress"] = 70

    sorted_output = os.path.join(folderPath, "sorted.json")
    sort_cmd = f'find "{folderPath}" -name "log-*" -print0 | xargs -0 cat | sed -rn "s/Respond:.*\\\"volley\\\": ([0-9]*), .*/\\1 <===> \\0/p" | sort -k1,1 -n > "{sorted_output}"'
    subprocess.run(sort_cmd, shell=True, executable="/bin/bash")

    zip_name = f"{folder} CS Logs {sidcid}.zip"
    zip_path = os.path.join("/tmp", zip_name)
    shutil.make_archive(zip_path.replace(".zip", ""), 'zip', folderPath)
    shutil.rmtree(folderPath)
    print(f"[DEBUG] Zip created at: {zip_path}")

    if task_metadata_ref and task_id:
        task_metadata_ref[task_id]["progress"] = 100

    return zip_path
