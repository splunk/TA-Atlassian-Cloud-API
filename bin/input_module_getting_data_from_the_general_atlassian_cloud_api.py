
# encoding = utf-8

import os
import sys
import time
import datetime
import json
import requests
from datetime import datetime,timedelta,timezone



def validate_input(helper, definition):
    pass



def main_api_handler(helper,ew,params):
    
    params["limit"] = 500 # The value in "limit" represents the number of events we want on each page.
    
    
    # current_utc_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # data = UTC_time_to_epoch(current_utc_time)
    # There is a value in "data" that represents the current UTC time converted into epoch time in milliseconds
    # if(params["to"] > data):
    #     params["to"] =  data
    

    to_value = None
    if 'cursor' in params:
        to_value = params.pop('to', None)
    
    global_access_token = helper.get_global_setting("access_token")
    opt_org_id = helper.get_arg('org_id')
    url = f'https://api.atlassian.com/admin/v1/orgs/{opt_org_id}/events'

    
    headers = {"Accept": "application/json","Authorization": f'Bearer {global_access_token}'}
    
    res = requests.get(url=url, headers=headers, params=params)

    if to_value is not None:
        params["to"] = to_value
        
    r_json = res.json()
    
    
    if r_json != 200:
        res.raise_for_status()
        
    data_shorting_handler(helper,ew,params,r_json)
    
def data_shorting_handler(helper,ew,params,r_json):

    if len(r_json["data"]) == 0:
        result_params = add_minutes_to_epoch(params["to"])
        main_api_handler(helper,ew,result_params)
    else:
        check_point_handler(helper,ew,r_json,params)
        
def check_point_handler(helper,ew,r_json,params):
    #    # set check point as per event
    for item in r_json["data"]:
        key = "{}_Atlassian_audit_test1_logs_{}".format(helper.get_input_stanza_names(),item["attributes"]["time"])

        if helper.get_check_point(key) is None:
            write_data_handler(helper,ew,item)
            helper.save_check_point(key,item["attributes"]["time"])
            
    if r_json["meta"]["next"] is not None:
        result_params = {"cursor":r_json["meta"]["next"],"to":params["to"]}
        main_api_handler(helper,ew,result_params)
        
    else:
        result_params = add_minutes_to_epoch(params["to"])
        main_api_handler(helper,ew,result_params)

    # helper.delete_check_point(key) # i am using it only for testing
    
# def check_point_handler(helper,ew,r_json,params):
#    # set check point as per query
    
#     key = "{}_Atlassian_audit_test1_logs_{}".format(helper.get_input_stanza_names(),params["to"])

#     if helper.get_check_point(key) is None:
#         for item in r_json["data"]:
#             write_data_handler(helper,ew,item)
            
#         if r_json["meta"]["next"] is not None:
#             result_params = {"cursor":r_json["meta"]["next"],"to":params["to"]}
#             main_api_handler(helper,ew,result_params)
            
#         else:
#             helper.save_check_point(key,params["to"])
#             result_params = add_minutes_to_epoch(params["to"])
#             main_api_handler(helper,ew,result_params)

#     # helper.delete_check_point(key) # i am using it only for testing
    
def write_data_handler(helper,ew,res):
    
    event = helper.new_event(json.dumps(res), time=None, host=None, index=helper.get_output_index(), source=None, sourcetype=None, done=True, unbroken=True)
    ew.write_event(event)
        
def UTC_time_to_epoch(original_date_string):
    utc_datetime = datetime.strptime(original_date_string, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return int(utc_datetime.timestamp() * 1000)
    
def add_minutes_to_epoch(value):
    distance_minutes = 5  # This value in distance_minutes represents the desired time interval or distance between the 'from' time and the 'to' time.
    from_time = int(value)
    to_time = from_time + (distance_minutes * 60 * 1000)

    return {"from":from_time,"to":to_time}


def collect_events(helper,ew):
    
    opt_from = helper.get_arg('from')   
    
    # timestamp_str = "2024-02-21T07:47:38.473000Z" #1708501658473
    # params = add_minutes_to_epoch(UTC_time_to_epoch(timestamp_str))
    
    params = add_minutes_to_epoch(opt_from)

    main_api_handler(helper,ew,params)
    
