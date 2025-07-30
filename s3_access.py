import os

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")



def getEnvironment(date, selected_env):
    year = date.strftime("%Y")
    env_region = selected_env[:2]

    # Determine root folder
    if env_region == "US":
        root_folder = "SegBots"
    else:
        root_folder = f"SegBots-{env_region}"

    # Map region code to AWS region
    # region_map = {
    #     "US": "us-east-1",
    #     "DE": "eu-central-1",
    #     "JP": "ap-northeast-1",
    #     "AU": "ap-southeast-2",
    #     "EU": "eu-west-1",
    #     "NT": "ap-south-1"  # Adjust as needed for NTT
    # }
    region_map = {
        "US": "us-east-1",
        "DE": "eu-central-1",
        "AU": "ap-southeast-2",
        "EU": "eu-west-1"
    }


    aws_region = region_map.get(env_region, "us-east-1")

    result = dict()

    if selected_env != "NTT Data PROD":
        result["Environment"] = selected_env
        result["accessKey"] = aws_access_key_id
        result["secretKey"] = aws_secret_access_key
        result["bucketName"] = f"{env_region.lower()}-prod-chatscriptlogs"
        result["awsRegion"] = aws_region

        # Paths for CS Logs
        result["cslogsPath"] = f"{root_folder}/CSLogs/{year}/"
        result["otherlogsPath"] = f"{root_folder}/OtherLogs/{year}/"
        
        if selected_env in ("DE-PROD", "EU-PROD" , "AU-PROD" ):
       # Paths for specific other logs
            result["botServiceRuntimePath"] = f"{root_folder}/NodeServiceLogs/BotsApp/{year}/"
            result["bsa_ka"] = f"{root_folder}/NodeServiceLogs/BotsApp/{year}/"

        else :
          result["botServiceRuntimePath"] = f"{root_folder}/NodeServiceLogs/BotsApp/{year}/"
          result["bsa_ka"] = f"{root_folder}/NodeServiceLogs/BotsAnalytics/{year}/" 


          
        result["faqlogsPath"] = f"{root_folder}/FAQ/{year}/"
        result["mlLogsPath"] = f"{root_folder}/ML-Logs/{year}/"
        result["appAndProfileLogsPath"] = f"{root_folder}/NodeServiceLogs/BotsApp/{year}/"

    



    # Override US-PROD bucket if needed
    if selected_env == "US-PROD":
        result["bucketName"] = "Prod-ChatScriptLogs"
        result["cslogsPath"] = f"SegBots/CSLogs/{year}/"
        result["otherlogsPath"] = f"SegBots/OtherLogs/{year}/"

    # Optional: List of available log types for UI
    result["ListOfAvailableLogs"] = [
        "BotsServiceAdmin Logs",
        "BotsServiceRuntime Logs",
        "Koreapp Logs",
        "FAQ Logs",
        "ML Logs",
        "app-logs",
        "profile-logs",
        "Nginx Logs",
        "Api Access Logs",
        "Error Logs"
    ]

    return result


