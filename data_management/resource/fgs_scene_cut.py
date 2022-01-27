# -*- coding:utf-8 -*-
import json
import requests
import datetime
import os
from urllib import parse
from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdksmn.v2.region.smn_region import SmnRegion
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdksmn.v2 import *


'''
{
    'Records': [
        {
            'eventVersion': '2.0', 
            'eventSource': 'aws:s3', 
            'awsRegion': 'cn-south-1', 
            'eventTime': '2021-12-25T08:02:30.389Z',
            'eventName': 'ObjectCreated:Put', 
            'userIdentity': {'principalId': '879c657349c44a9c9ac345867ea999d1'},
            'requestParameters': {'sourceIPAddress': '10.48.51.83'},
            'responseElements': {'x-amz-request-id': '35a9e0041b7a4bff8903365011f81fe2',
                              'x-amz-id-2': 'FJZA3Hq0hNm8ZQAECeAjlglzI4iBNSG/Y347hRAIjtlzR4AnHjs/JfEy1+bOi8tE'},
            's3': {
                's3SchemaVersion': '1.0', 
                'configurationId': 'obs-event-18t4',
                'bucket': {
                    'name': 'autopolit', 
                    'ownerIdentity': {
                        'PrincipalId': '8c1d78bc75bf4154a4d0c73a28c3e0b5'

                    },
                    'arn': 'arn:aws:s3:::autopolit'
                },
                'object': {
                    'key': 'setup.py', #'dir1%2F__init__.py'
                    'eTag': 'a6e683a165f9443a3224a010923cd240', 
                    'size': 1149, 
                    'versionId': 'null',
                    'sequencer': '0000000017DF09CD77FD0596C0000000'
                }
            }
         }
    ]
}
'''


class S3Object:
    region = ""
    bucket_name = ""
    object_name = ""
    size = ""


def handler(event, context):
    logg = context.getLogger()
    ak = context.getAccessKey()
    sk = context.getSecretKey()
    context_status = True
    error_msg = ""
    api_url = context.getUserData('apiSceneCut')   # "http://172.16.0.125:5000/api/scene/cut"
    if not api_url or not api_url.startswith("http://"):
        context_status = False
        error_msg = "{}{}\n".format(error_msg, "invalid context, Parameter: apiSceneCut.")

    topic_urn = context.getUserData('topicUrn')    # "urn:smn:cn-south-1:2647a2479d35405084a0e32ed20ed04b:autopolit"
    tmp_topic = topic_urn.split(":")
    if len(tmp_topic) != 5:
        context_status = False
        error_msg = "{}{}\n".format(error_msg, "invalid context, Parameter: topicUrn.")
    topic_region = topic_urn.split(":")[2]

    output_obs = context.getUserData('outputObs')
    if not output_obs:
        context_status = False
        error_msg = "{}{}\n".format(error_msg, "invalid context, Parameter: outputObs.")

    if not context_status:
        raise Exception(error_msg)

    if 'Records' not in event.keys():
        logg.error("invalid param, Set parameters according to the template.")
        raise Exception("invalid param, Set parameters according to the template.")

    s3_object = get_obs_obj_info(event)
    if s3_object.size == 0:
        logg.warning("invalid object, size was 0.")
        return "OK"

    bag_path = "obs://{}/{}".format(s3_object.bucket_name, s3_object.object_name)
    obj_path = "/".join(s3_object.object_name.split("/")[:-1])
    obs_output_dir = "obs://{}".format(output_obs)
    if obj_path:
        obs_output_dir = "{}/{}".format(obs_output_dir, obj_path)
    logg.info("***bag_path:{}".format(bag_path))
    logg.info("***obs_output_dir:{}".format(obs_output_dir))
    date_str = datetime.datetime.today().strftime("%Y%m%d%H%M%S")

    data = {"bag_id": date_str, "bag_path": bag_path, "obs_output_dir": obs_output_dir}
    headers = {"Content-Type": "application/json"}
    logg.info(data)
    rep = requests.post(url=api_url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'), headers=headers)
    rep_text = rep.text
    logg.info("bag_path:"+bag_path)
    """
    d = os.popen('''curl -H "Content-Type:application/json" -X POST -d '{"bag_id":"%s","bag_path":"%s","obs_output_dir":"%s"}' %s'''%(date_str, bag_path,obs_output_dir,api_url))
    # d = os.popen("locale")
    rep_text = d.read()
    """
    logg.info("****rep_text:"+rep_text)
    rep_json = json.loads(rep_text)

    credentials = BasicCredentials(ak, sk)
    smn_smn_client = SmnClient.new_builder() \
        .with_credentials(credentials) \
        .with_region(SmnRegion.value_of(topic_region)) \
        .build()

    
    # job_id = rep_json["data"]["task_id"]
    status_str = "成功"
    if rep_json["errno"] != 0:
        status_str = "失败"

    message = """尊敬的华为云用户：
    您于{data}上传的数据{bagPath}，调用禾多数据平台任务{status}，请关注。
    详细信息：{errmsg}""".format(data=date_str, bagPath=bag_path, status=status_str, errmsg=rep_json["errmsg"])

    smn_publish_message(smn_smn_client, topic_urn, message, logg)
    logg.info("the task is finish.")
    return "OK"


def smn_publish_message(smn_client, topic_urn, msg, logg):
    try:
        request = PublishMessageRequest()
        request.topic_urn = topic_urn
        request.body = PublishMessageRequestBody(
            message=msg,
            subject="场景提取任务通知"
        )
        logg.info(request)
        response = smn_client.publish_message(request)
        logg.info(response)
    except exceptions.ClientRequestException as e:
        logg.error(e.status_code)
        logg.error(e.request_id)
        logg.error(e.error_code)
        logg.error(e.error_msg)


def get_obs_obj_info(event):
    tmp_object = S3Object()
    tmp_object.region = event['Records'][0]['awsRegion']
    # Obtains a bucket name.
    tmp_object.bucket_name = parse.unquote(event['Records'][0]['s3']['bucket']["name"])
    # Obtains the name of an uploaded object.
    tmp_object.object_name = parse.unquote(event['Records'][0]['s3']['object']["key"])
    tmp_object.size = int(event['Records'][0]['s3']['object']["size"])

    return tmp_object
