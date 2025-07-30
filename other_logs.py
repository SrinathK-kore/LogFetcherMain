

# import os
# import boto3
# import shutil
# import uuid
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import s3_access
# from dotenv import load_dotenv


# # Load environment variables from .env
# load_dotenv()

# # Access the values
# aws_key = os.getenv("AWS_ACCESS_KEY_ID")
# aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")


# def get_s3_client(access_key, secret_key, region):
#     print(f"[DEBUG] Creating S3 client for region: {region}")
#     return boto3.client(
#         's3',
#         aws_access_key_id=access_key,
#         aws_secret_access_key=secret_key,
#         region_name=region
#     )

# def list_folders(s3, bucket, prefix):
#     print(f"[DEBUG] Listing folders in bucket: {bucket}, prefix: {prefix}")
#     paginator = s3.get_paginator('list_objects_v2')
#     result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
#     folders = []
#     for page in result:
#         if 'CommonPrefixes' in page:
#             for cp in page['CommonPrefixes']:
#                 folder_name = cp['Prefix'].rstrip('/').split('/')[-1]
#                 print(f"[DEBUG] Found folder: {folder_name}")
#                 folders.append(folder_name)
#     return folders

# def list_matching_keys(s3, bucket, prefix, pattern, cancel_event=None, task_id=None):
#     print(f"[DEBUG] Scanning S3 keys in {bucket}/{prefix} for pattern: {pattern}")
#     paginator = s3.get_paginator('list_objects_v2')
#     pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
#     matched_keys = []
#     for page_index, page in enumerate(pages, 1):
#         if cancel_event and cancel_event.is_set():
#             print(f"[CANCELLED] Task {task_id} cancelled during S3 scan")
#             return []
#         contents = page.get('Contents', [])
#         for obj in contents:
#             if cancel_event and cancel_event.is_set():
#                 print(f"[CANCELLED] Task {task_id} cancelled during object iteration")
#                 return []
#             key = obj['Key']
#             if pattern in key:
#                 print(f"[DEBUG] Matched Key: {key}")
#                 matched_keys.append(key)
#     print(f"[DEBUG] Total matched keys: {len(matched_keys)}")
#     return matched_keys

# def download_keys(s3, bucket, matched_keys, prefix, local_path, max_files, cancel_event=None, task_id=None):
#     print(f"[DEBUG] Downloading {len(matched_keys)} keys to: {local_path}")
#     os.makedirs(local_path, exist_ok=True)
#     downloaded = 0
#     for key in matched_keys:
#         if cancel_event and cancel_event.is_set():
#             print(f"[CANCELLED] Task {task_id} cancelled while downloading")
#             shutil.rmtree(local_path, ignore_errors=True)
#             return None
#         rel_path = key.replace(prefix, '').lstrip('/')
#         local_file_path = os.path.join(local_path, rel_path)
#         print(f"[DEBUG] Downloading {key} to {local_file_path}")
#         print(f"[DEBUG] AWS CLI equivalent: aws s3 cp s3://{bucket}/{key} {local_file_path}")
#         os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
#         try:
#             s3.download_file(bucket, key, local_file_path)
#             downloaded += 1
#             if downloaded >= max_files:
#                 print(f"[DEBUG] Max file limit {max_files} reached.")
#                 return True
#         except Exception as e:
#             print(f"[ERROR] Failed to download {key}: {e}")
#     return True if downloaded > 0 else False

# def download_log(s3, bucket, prefix, pattern, local_path, max_files=500, cancel_event=None, task_id=None):
#     matched_keys = list_matching_keys(s3, bucket, prefix, pattern, cancel_event, task_id)
#     if cancel_event and cancel_event.is_set():
#         print(f"[CANCELLED] Task {task_id} cancelled before download")
#         return None
#     if not matched_keys:
#         print(f"[DEBUG] No keys matched for prefix: {prefix}, pattern: {pattern}")
#         shutil.rmtree(local_path, ignore_errors=True)
#         return False
#     return download_keys(s3, bucket, matched_keys, prefix, local_path, max_files, cancel_event, task_id)

# def download_api_access_logs(s3, bucket, base_prefix, pattern, local_path, max_files=500, cancel_event=None, task_id=None):
#     print(f"[DEBUG] Scanning API access logs under: {bucket}/{base_prefix}")
#     ip_folders = list_folders(s3, bucket, base_prefix + '/')
#     matched_keys = []
#     with ThreadPoolExecutor(max_workers=8) as executor:
#         future_to_ip = {
#             executor.submit(list_matching_keys, s3, bucket, f"{base_prefix}/{ip}/", pattern, cancel_event, task_id): ip for ip in ip_folders
#         }
#         for future in as_completed(future_to_ip):
#             if cancel_event and cancel_event.is_set():
#                 print(f"[CANCELLED] Task {task_id} cancelled during IP scan")
#                 return None
#             try:
#                 keys = future.result()
#                 matched_keys.extend(keys)
#             except Exception as e:
#                 print(f"[ERROR] Exception while scanning IP folder: {e}")
#             if len(matched_keys) >= max_files:
#                 break
#     print(f"[DEBUG] Total matched API logs: {len(matched_keys)}")
#     if not matched_keys:
#         shutil.rmtree(local_path, ignore_errors=True)
#         return False
#     return download_keys(s3, bucket, matched_keys, base_prefix + '/', local_path, max_files, cancel_event, task_id)

# def run_other_logs_fetch(date_str, env_id, selected_logs, cancel_event=None, task_id=None, task_metadata_ref=None):
#     # environments = ["US-PROD", "DE-PROD", "JP-PROD", "AU-PROD", "EU-PROD", "NTT Data PROD"]
#     environments = ["US-PROD", "DE-PROD", "AU-PROD", "EU-PROD"]

#     date_obj = datetime.strptime(date_str, "%Y-%m-%d")
#     formatted_date = date_obj.strftime("%d-%b-%Y")
#     session_id = uuid.uuid4().hex

#     selected_account = s3_access.getEnvironment(date_obj, environments[env_id - 1])
#     bucket = selected_account["bucketName"]
#     region = selected_account["awsRegion"]

#     print(f"[DEBUG] Task {task_id} - Using bucket: {bucket}, region: {region}")

#     s3 = get_s3_client(selected_account["accessKey"], selected_account["secretKey"], region)

#     folder_path = f"/tmp/{formatted_date}_OtherLogs_{session_id}"
#     print(f"[DEBUG] Task {task_id} - Temp download folder: {folder_path}")
#     os.makedirs(folder_path, exist_ok=True)

#     log_type_paths = {
#         "BotsServiceAdmin Logs": ("bsa_ka", "BotsServiceAdmin"),
#         "BotsServiceRuntime Logs": ("botServiceRuntimePath", "BotsServiceRuntime"),
#         "Koreapp Logs": ("bsa_ka", "koreapp"),
#         "FAQ Logs": ("faqlogsPath", ""),
#         "ML Logs": ("mlLogsPath", ""),
#         "app-logs": ("appAndProfileLogsPath", f"app-logs.log.{formatted_date[:2]}"),
#         "profile-logs": ("appAndProfileLogsPath", f"profile-logs.log.{formatted_date[:2]}"),
#         "Nginx Logs": ("appAndProfileLogsPath", "nginx"),
#         "Api Access Logs": ("appAndProfileLogsPath", "api_access.log"),
#         "Error Logs": ("appAndProfileLogsPath", "error.log")
#     }

#     downloaded_types = []
#     jobs = []

#     for i, index in enumerate(selected_logs):
#         if cancel_event and cancel_event.is_set():
#             print(f"[CANCELLED] Task {task_id} cancelled before job dispatch")
#             shutil.rmtree(folder_path, ignore_errors=True)
#             return None

#         log_name = selected_account["ListOfAvailableLogs"][int(index) - 1]
#         s3_key_field, pattern = log_type_paths[log_name]
#         prefix = selected_account[s3_key_field] + formatted_date
#         print(f"[DEBUG] Task {task_id} - Preparing job for: {log_name}, Prefix: {prefix}, Pattern: {pattern}")

#         local_log_path = os.path.join(folder_path, log_name.replace(" ", "_"))

#         if log_name == "Api Access Logs":
#             jobs.append((download_api_access_logs, (s3, bucket, prefix, pattern, local_log_path, 500, cancel_event, task_id)))
#         else:
#             jobs.append((download_log, (s3, bucket, prefix, pattern, local_log_path, 500, cancel_event, task_id)))

#     with ThreadPoolExecutor(max_workers=8) as executor:
#         futures = [executor.submit(fn, *args) for fn, args in jobs]
#         for i, future in enumerate(futures):
#             if cancel_event and cancel_event.is_set():
#                 print(f"[CANCELLED] Task {task_id} cancelled during execution")
#                 shutil.rmtree(folder_path, ignore_errors=True)
#                 return None
#             result = future.result()
#             if cancel_event and cancel_event.is_set():
#                 print(f"[CANCELLED] Task {task_id} cancelled post job")
#                 shutil.rmtree(folder_path, ignore_errors=True)
#                 return None
#             if result:
#                 downloaded_types.append(selected_account["ListOfAvailableLogs"][int(selected_logs[i]) - 1])
#             if task_metadata_ref and task_id:
#                 progress = int((i + 1) / len(futures) * 100)
#                 task_metadata_ref[task_id]["progress"] = progress
#                 print(f"[DEBUG] Task {task_id} - Progress: {progress}%")

#     if cancel_event and cancel_event.is_set():
#         print(f"[CANCELLED] Task {task_id} cancelled after download")
#         shutil.rmtree(folder_path, ignore_errors=True)
#         return None

#     if not downloaded_types:
#         print(f"[ERROR] Task {task_id} - No logs matched")
#         shutil.rmtree(folder_path, ignore_errors=True)
#         raise Exception("No logs matched the given criteria.")

#     zip_filename = f"{formatted_date}_{'_'.join([t.replace(' ', '_') for t in downloaded_types])}.zip"
#     zip_path = os.path.join("/tmp", zip_filename)
#     shutil.make_archive(zip_path.replace(".zip", ""), 'zip', folder_path)
#     shutil.rmtree(folder_path)

#     if task_metadata_ref and task_id:
#         task_metadata_ref[task_id]["progress"] = 100
#         print(f"[DEBUG] Task {task_id} - Final progress: 100%")

#     print(f"[DEBUG] Task {task_id} - Log download completed. Zip path: {zip_path}")
#     return zip_path



# .................latest working one..............
import os
import boto3
import shutil
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import s3_access
from dotenv import load_dotenv
import botocore.exceptions
import time

# Load environment variables from .env
load_dotenv()


def get_s3_client(access_key, secret_key, region):
    print(f"[DEBUG] Creating S3 client for region: {region}")
    return boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )

def list_folders(s3, bucket, prefix):
    print(f"[DEBUG] Listing folders in bucket: {bucket}, prefix: {prefix}")
    paginator = s3.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
    folders = []
    for page in result:
        if 'CommonPrefixes' in page:
            for cp in page['CommonPrefixes']:
                folder_name = cp['Prefix'].rstrip('/').split('/')[-1]
                print(f"[DEBUG] Found folder: {folder_name}")
                folders.append(folder_name)
    return folders

def list_matching_keys(s3, bucket, prefix, pattern, cancel_event=None, task_id=None):
    print(f"[DEBUG] Scanning S3 keys in {bucket}/{prefix} for pattern: {pattern}")
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    matched_keys = []
    for page_index, page in enumerate(pages, 1):
        if cancel_event and cancel_event.is_set():
            print(f"[CANCELLED] Task {task_id} cancelled during S3 scan")
            return []
        contents = page.get('Contents', [])
        for obj in contents:
            if cancel_event and cancel_event.is_set():
                print(f"[CANCELLED] Task {task_id} cancelled during object iteration")
                return []
            key = obj['Key']
            if pattern in key:
                print(f"[DEBUG] Matched Key: {key}")
                matched_keys.append(key)
    print(f"[DEBUG] Total matched keys: {len(matched_keys)}")
    return matched_keys

def safe_download_file(s3, bucket, key, local_path, retries=3):
    for attempt in range(1, retries + 1):
        try:
            s3.download_file(bucket, key, local_path)
            return True
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidObjectState':
                print(f"[WARNING] Skipping {key} due to invalid state.")
                return False
            elif 'ETag' in str(e):
                print(f"[RETRY] ETag mismatch on {key}, attempt {attempt}/{retries}")
                time.sleep(1.5 * attempt)
            else:
                print(f"[ERROR] Unexpected error downloading {key}: {e}")
                return False
        except Exception as e:
            print(f"[ERROR] Failed to download {key}: {e}")
            return False
    print(f"[FAILED] Giving up on {key} after {retries} attempts")
    return False

def download_keys(s3, bucket, matched_keys, prefix, local_path, max_files, cancel_event=None, task_id=None):
    print(f"[DEBUG] Downloading {len(matched_keys)} keys to: {local_path}")
    os.makedirs(local_path, exist_ok=True)
    downloaded = 0
    for key in matched_keys:
        if cancel_event and cancel_event.is_set():
            print(f"[CANCELLED] Task {task_id} cancelled while downloading")
            shutil.rmtree(local_path, ignore_errors=True)
            return None
        rel_path = key.replace(prefix, '').lstrip('/')
        local_file_path = os.path.join(local_path, rel_path)
        print(f"[DEBUG] Downloading {key} to {local_file_path}")
        print(f"[DEBUG] AWS CLI equivalent: aws s3 cp s3://{bucket}/{key} {local_file_path}")
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        if safe_download_file(s3, bucket, key, local_file_path):
            downloaded += 1
            if downloaded >= max_files:
                print(f"[DEBUG] Max file limit {max_files} reached.")
                return True
    return True if downloaded > 0 else False


def download_log(s3, bucket, prefix, pattern, local_path, max_files=500, cancel_event=None, task_id=None):
    matched_keys = list_matching_keys(s3, bucket, prefix, pattern, cancel_event, task_id)
    if cancel_event and cancel_event.is_set():
        print(f"[CANCELLED] Task {task_id} cancelled before download")
        return None
    if not matched_keys:
        print(f"[DEBUG] No keys matched for prefix: {prefix}, pattern: {pattern}")
        shutil.rmtree(local_path, ignore_errors=True)
        return False
    return download_keys(s3, bucket, matched_keys, prefix, local_path, max_files, cancel_event, task_id)

def download_api_access_logs(s3, bucket, base_prefix, pattern, local_path, max_files=500, cancel_event=None, task_id=None):
    print(f"[DEBUG] Scanning API access logs under: {bucket}/{base_prefix}")
    ip_folders = list_folders(s3, bucket, base_prefix + '/')
    matched_keys = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_ip = {
            executor.submit(list_matching_keys, s3, bucket, f"{base_prefix}/{ip}/", pattern, cancel_event, task_id): ip for ip in ip_folders
        }
        for future in as_completed(future_to_ip):
            if cancel_event and cancel_event.is_set():
                print(f"[CANCELLED] Task {task_id} cancelled during IP scan")
                return None
            try:
                keys = future.result()
                matched_keys.extend(keys)
            except Exception as e:
                print(f"[ERROR] Exception while scanning IP folder: {e}")
            if len(matched_keys) >= max_files:
                break
    print(f"[DEBUG] Total matched API logs: {len(matched_keys)}")
    if not matched_keys:
        shutil.rmtree(local_path, ignore_errors=True)
        return False
    return download_keys(s3, bucket, matched_keys, base_prefix + '/', local_path, max_files, cancel_event, task_id)

def run_other_logs_fetch(date_str, env_id, selected_logs, cancel_event=None, task_id=None, task_metadata_ref=None):
    environments = ["US-PROD", "DE-PROD", "AU-PROD", "EU-PROD"]

    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d-%b-%Y")
    session_id = uuid.uuid4().hex

    selected_account = s3_access.getEnvironment(date_obj, environments[env_id - 1])
    bucket = selected_account["bucketName"]
    region = selected_account["awsRegion"]

    print(f"[DEBUG] Task {task_id} - Using bucket: {bucket}, region: {region}")

    s3 = get_s3_client(selected_account["accessKey"], selected_account["secretKey"], region)

    folder_path = f"/tmp/{formatted_date}_OtherLogs_{session_id}"
    print(f"[DEBUG] Task {task_id} - Temp download folder: {folder_path}")
    os.makedirs(folder_path, exist_ok=True)

    log_type_paths = {
        "BotsServiceAdmin Logs": ("bsa_ka", "BotsServiceAdmin"),
        "BotsServiceRuntime Logs": ("botServiceRuntimePath", "BotsServiceRuntime"),
        "Koreapp Logs": ("bsa_ka", "koreapp"),
        "FAQ Logs": ("faqlogsPath", ""),
        "ML Logs": ("mlLogsPath", ""),
        "app-logs": ("appAndProfileLogsPath", f"app-logs.log.{formatted_date[:2]}"),
        "profile-logs": ("appAndProfileLogsPath", f"profile-logs.log.{formatted_date[:2]}"),
        "Nginx Logs": ("appAndProfileLogsPath", "nginx"),
        "Api Access Logs": ("appAndProfileLogsPath", "api_access.log"),
        "Error Logs": ("appAndProfileLogsPath", "error.log")
    }

    downloaded_types = []
    jobs = []

    for i, index in enumerate(selected_logs):
        if cancel_event and cancel_event.is_set():
            print(f"[CANCELLED] Task {task_id} cancelled before job dispatch")
            shutil.rmtree(folder_path, ignore_errors=True)
            return None

        log_name = selected_account["ListOfAvailableLogs"][int(index) - 1]
        s3_key_field, pattern = log_type_paths[log_name]
        prefix = selected_account[s3_key_field] + formatted_date
        print(f"[DEBUG] Task {task_id} - Preparing job for: {log_name}, Prefix: {prefix}, Pattern: {pattern}")

        local_log_path = os.path.join(folder_path, log_name.replace(" ", "_"))

        if log_name == "Api Access Logs":
            jobs.append((download_api_access_logs, (s3, bucket, prefix, pattern, local_log_path, 500, cancel_event, task_id)))
        else:
            jobs.append((download_log, (s3, bucket, prefix, pattern, local_log_path, 500, cancel_event, task_id)))

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(fn, *args) for fn, args in jobs]
        for i, future in enumerate(futures):
            if cancel_event and cancel_event.is_set():
                print(f"[CANCELLED] Task {task_id} cancelled during execution")
                shutil.rmtree(folder_path, ignore_errors=True)
                return None
            result = future.result()
            if cancel_event and cancel_event.is_set():
                print(f"[CANCELLED] Task {task_id} cancelled post job")
                shutil.rmtree(folder_path, ignore_errors=True)
                return None
            if result:
                downloaded_types.append(selected_account["ListOfAvailableLogs"][int(selected_logs[i]) - 1])
            if task_metadata_ref and task_id:
                progress = int((i + 1) / len(futures) * 100)
                task_metadata_ref[task_id]["progress"] = progress
                print(f"[DEBUG] Task {task_id} - Progress: {progress}%")

    if cancel_event and cancel_event.is_set():
        print(f"[CANCELLED] Task {task_id} cancelled after download")
        shutil.rmtree(folder_path, ignore_errors=True)
        return None

    if not downloaded_types:
        print(f"[ERROR] Task {task_id} - No logs matched")
        shutil.rmtree(folder_path, ignore_errors=True)
        raise Exception("No logs matched the given criteria.")

    zip_filename = f"{formatted_date}_{'_'.join([t.replace(' ', '_') for t in downloaded_types])}.zip"
    zip_path = os.path.join("/tmp", zip_filename)
    shutil.make_archive(zip_path.replace(".zip", ""), 'zip', folder_path)
    shutil.rmtree(folder_path)

    if task_metadata_ref and task_id:
        task_metadata_ref[task_id]["progress"] = 100
        print(f"[DEBUG] Task {task_id} - Final progress: 100%")

    print(f"[DEBUG] Task {task_id} - Log download completed. Zip path: {zip_path}")
    return zip_path
