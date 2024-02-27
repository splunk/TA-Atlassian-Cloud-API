
# encoding = utf-8

import os
import sys
import time
import datetime
import json


def validate_input(helper, definition):
    pass


def main_api_handler(helper,ew,url):
    
    global_access_token = helper.get_global_setting("access_token")

    headers = {"Accept": "application/json","Authorization": f'Bearer {global_access_token}'}

    response = helper.send_http_request(url, "GET", parameters=None, payload=None,headers=headers, cookies=None, verify=True, cert=None,timeout=None, use_proxy=True)
    
    r_json = response.json()
    
    r_status = response.status_code
    
    if r_status != 200:
        response.raise_for_status()
        
    check_point_handler(helper,ew,r_json)


def check_point_handler(helper,ew,r_json):

    if r_json["meta"]["prev"] is not None:
        check_point_id = r_json["meta"]["prev"]
        status = helper.get_check_point(str(check_point_id))
        if status is None:
            helper.save_check_point(str(check_point_id), "Indexed")
            write_data_handler(helper,ew,r_json)
        # helper.delete_check_point(str(check_point_id)) # i am using it only for testing


    
def write_data_handler(helper,ew,res):
    
    for item in res["data"]:
        event = helper.new_event(json.dumps(item), time=None, host=None, index=helper.get_output_index(), source=None, sourcetype=None, done=True, unbroken=True)
        ew.write_event(event)
        
    
    if res["meta"]["next"] is not None:
        url = res["links"]["next"]
        main_api_handler(helper,ew,url)
    

def collect_events(helper, ew):
    
    opt_org_id = helper.get_arg('org_id')
    url = f'https://api.atlassian.com/admin/v1/orgs/{opt_org_id}/events'
 
    main_api_handler(helper,ew,url)
    
