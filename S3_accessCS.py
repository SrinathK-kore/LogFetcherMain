import os

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")




def getEnvironment(date, selected_env):
    year = date.strftime("%Y")
    env_region = selected_env[:2]
    result = dict()

    if selected_env != "NTT Data PROD":
        result["Environment"] = selected_env
        result["accessKey"] = aws_access_key_id
        result["secretKey"] = aws_secret_access_key
        result["bucketName"] = env_region.lower() + "-prod-chatscriptlogs"
        result["cslogsPath"] = "SegBots-" + env_region + "/CSLogs/" + year + "/"
        result["awsRegion"] = "ap-south-1"  # default
    

    if selected_env == "US-PROD":
        result["bucketName"] = "Prod-ChatScriptLogs"
        result["cslogsPath"] = "SegBots/CSLogs/" + year + "/"
        result["awsRegion"] = "us-east-1"  

    return result
