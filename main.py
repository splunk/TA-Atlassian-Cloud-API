import os
import sys
import time
import datetime
import json
import requests
from datetime import datetime,timedelta,timezone



checkpoints = []
splDataList = []

def main_api_handler(url,params):

    # print(len(splDataList))
    # print(params)

    tempParams = params

    if "crucor" in params:
        params.pop("to", None)

    global_access_token = "ATCTT3xFfGN0vakFV6RCTNAQkXaLqVd-GGt57xsPE0QZLwZNrWAyjFMfsZ2aT4orNAG0xtSga7R-61BFCZSA6Bo5hotIIqOSvcnhJzAOJhiU_QZXs9WuXKzsmEjtdVHfK9EhBxnLSw-cYrQ4hK8p8fVQ57k74P7erAChLYlOyCy3Fhf1G1UTAlw=4F3A5CE9"

    headers = {"Accept": "application/json","Authorization": f'Bearer {global_access_token}'}

    res = requests.get(url=url,headers=headers,params=params)

    params["to"] = tempParams["to"]
    r_json = res.json()

    # if res.status_code != 400:

    if res.status_code == 429:
        countdown_timer(1,url,params)


    if r_json != 200:
        res.raise_for_status()

    dataShortingHandler(url,params,r_json)
        
def countdown_timer(minutes,url,params):
    seconds = minutes * 60

    while seconds > 0:
        print(f"Time remaining: {seconds // 60} minutes {seconds % 60} seconds")
        time.sleep(1)
        seconds -= 1

    main_api_handler(url,params)

def dataShortingHandler(url,params,r_json):

    if len(r_json["data"]) == 0:
        utc_time = epoch_to_time_Handler(params["to"])
        result_params = time_to_epoch_Handler(utc_time)
        main_api_handler(url,result_params)
    else:
        sorted_events = sorted(r_json["data"], key=lambda x: x["attributes"]["time"])
        
        r_json["data"] = sorted_events

        checkPointHandler(r_json,params)

def checkPointHandler(res,params):
    provided_time_epoch =""
    for item in res["data"]:
        provided_time_obj = datetime.strptime(item["attributes"]["time"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        provided_time_epoch = int(provided_time_obj.timestamp() * 1000)
        if provided_time_epoch not in checkpoints:
            checkpoints.append(provided_time_epoch)
            WriteDataHandler(item)

           
    nextPageCheckHandler(res,params,provided_time_epoch)

def WriteDataHandler(res):
    splDataList.append(res)

def nextPageCheckHandler(res,params,provided_time_epoch):
    if res["meta"]["next"] is not None:
        url = res["links"]["next"]
        result_params = {"crusor":res["meta"]["next"],"to":provided_time_epoch}
        main_api_handler(url,result_params)
    else:
        # current_utc_time = datetime.now(timezone.utc)
        # current_epoch_time_ms = int(current_utc_time.timestamp() * 1000)
        
        # print("current_utc_time..........")
        # print(current_epoch_time_ms)
        # print("provided_time_epoch.....")
        # print(provided_time_epoch)

        # if(current_epoch_time_ms >= provided_time_epoch):

        #     opt_org_id = "k7d2b4kj-0748-1kbb-7a32-a23j1cc2537d"
        #     url = f'https://api.atlassian.com/admin/v1/orgs/{opt_org_id}/events'
          
        #     utc_time = epoch_to_time_Handler(params["to"])
        #     result_params = time_to_epoch_Handler(utc_time)

        #     main_api_handler(url,result_params)



        # target_epoch_time_ms = "1708588058473"

        current_time_sec = time.time()
        current_time_ms = round(current_time_sec * 1000)

        print("target_epoch_time_sec....")
        print(params["to"])

        print("current_time_ms.......")
        print(current_time_ms)

        if current_time_ms >= params["to"]:

            opt_org_id = "k7d2b4kj-0748-1kbb-7a32-a23j1cc2537d"
            url = f'https://api.atlassian.com/admin/v1/orgs/{opt_org_id}/events'
        
            utc_time = epoch_to_time_Handler(params["to"])
            result_params = time_to_epoch_Handler(utc_time)

            main_api_handler(url,result_params)
            

def epoch_to_time_Handler(epoch_time_ms):
    utc_datetime = datetime.utcfromtimestamp(epoch_time_ms / 1000).replace(tzinfo=timezone.utc)

    return utc_datetime

def time_to_epoch_Handler(timestamp_str):
    timestamp_dt = datetime.strptime(str(timestamp_str), "%Y-%m-%d %H:%M:%S.%f%z")
    from_epoch = int(timestamp_dt.timestamp() * 1000)

    after_interval = timestamp_dt + timedelta(minutes=7200)
    to_epoch = int(after_interval.timestamp() * 1000)
    
    param = {"from":from_epoch,"to":to_epoch}

    return param


def main():
    opt_org_id = "k7d2b4kj-0748-1kbb-7a32-a23j1cc2537d"
    url = f'https://api.atlassian.com/admin/v1/orgs/{opt_org_id}/events'

    # timestamp_str = "2023-10-04 08:23:38.482000+00:00"
    timestamp_str = "2024-02-22 07:47:38.473000+00:00"

    params = time_to_epoch_Handler(timestamp_str)

    main_api_handler(url,params)
    
if __name__ == "__main__":
    main()