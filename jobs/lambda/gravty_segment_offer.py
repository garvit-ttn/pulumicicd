import json
import requests
import boto3
from requests.auth import HTTPBasicAuth
from botocore.exceptions import ClientError
import ast


def get_secret():
    secret_name = "gravty_secretsmanager"
    region_name = "eu-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    secretvalues = json.loads(secret)
    apikey = secretvalues['apikey']
    username = secretvalues['username']
    password = secretvalues['password']
    return apikey, username, password


# username='uhU0IA5pbPMnftu5Hp6baIArZbj4fzmE'
# password='oI1H8nLVwGtyulCk'
# apikey= 'GfqP7b2I99sUMkbxGEk5Xk56RscaWRuo'
# signedurl = "https://maf-holding-dev.apigee.net/v1/gravty/signed-url"
# segmentsurl = "https://maf-holding-dev.apigee.net/v1/gravty/segments-files"
# handbackurl = "https://maf-holding-dev.apigee.net/v1/gravty/handback-files/"
#
# Making a get request
def basic_auth(username, password):
    resp = requests.get(url='https://maf-holding-dev.apigee.net/v1/oauth/generate?grant_type=client_credentials',
                        auth=HTTPBasicAuth(username, password))
    print(resp.text)
    access_token = json.loads(resp.text)['access_token']
    print(access_token)
    return access_token


def create_signed_url(access_token, apikey):
    payload = json.dumps({
        "subfolder": "waleed",
        "file_name": "waleed.csv",
        "file_type": "text/csv"
    })
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': apikey,
        'Authorization': 'Bearer' + " " + access_token
    }
    response = requests.request("POST", url="https://maf-holding-dev.apigee.net/v1/gravty/signed-url", headers=headers,
                                data=payload)
    signed_request = json.loads(response.text)['signed_request']
    filename = json.loads(response.text)['url']
    print("signed_request=", signed_request)
    print("url=", filename)

    return signed_request, filename


def process_segment_files(filename, access_token, apikey):
    payload = json.dumps({
        "filename": filename
    })
    headers = {
        'x-api-key': apikey,
        'Authorization': 'Bearer' + " " + access_token,
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url="https://maf-holding-dev.apigee.net/v1/gravty/segments-files",
                                headers=headers, data=payload)
    print(response.text)
    id = json.loads(response.text)['id']
    print(id)
    return id


def get_handback_files(idapi, access_token, apikey):
    payload = {}
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': apikey,
        'Authorization': 'Bearer' + " " + access_token
    }

    response = requests.request("GET", url="https://maf-holding-dev.apigee.net/v1/gravty/handback-files/" + str(idapi),
                                headers=headers, data=payload)
    print(response.text)


def create_member_segment(filename, access_token, apikey, name, member_segment_code="name"):
    payload = json.dumps({
        "member_segment_name": name,
        "dynamic_segmentation": False,
        "member_segment_code": member_segment_code,
        "active": True,
        "sponsor": 1,
        "member_target_json": None,
        "member_target_xml": None,
        "member_target_output_file": filename,
        "member_target_type": "file"
    })
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': apikey,
        'Authorization': 'Bearer' + " " + access_token
    }

    response = requests.request("POST", url="https://maf-holding-dev.apigee.net/v1/gravty/members-segments",
                                headers=headers, data=payload)
    # print(response.text)
    return member_segment_code


def get_member_segment(member_segment_code, access_token, apikey):
    payload = {}
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': apikey,
        'Authorization': 'Bearer' + " " + access_token
    }

    response = requests.request("GET",
                                url="http://maf-holding-dev.apigee.net/v1/gravty/members-segments/" + member_segment_code,
                                headers=headers, data=payload)
    print(response.text)


def s3_upload(signed_request):
    url = signed_request
    payload = "<file contents here>"
    headers = {
        'Content-Type': 'text/csv'
    }
    response = requests.request("PUT", url, headers=headers, data=payload)
    print(response.text)


def create_offer(access_token, target_segment="name"):
    url = "https://maf-holding-dev.apigee.net/v1/gravty/template-offer"

    payload = json.dumps({
        "base_offer": False,
        "program_id": 29,
        "sponsors": [
            1
        ],
        "offer_name": "Gravty QA Individualized Offer Test 3",
        "offer_description": "Gravty QA Individualized Offer Test 3",
        "offer_external_id": None,
        "subtitle": "Gravty QA Individualized Offer Test 1",
        "is_template_based": True,
        "template_name": "PERSONALIZED_OFFER_TEMPLATE",
        "member_visibility": False,
        "offer_type": "reward",
        "member_target_type": "segment",
        "targeted_segment": "SHR_test1_06072022",
        "start_date": "2023-04-18T02:00:00+04:00",
        "end_date": "2023-04-19T23:59:59+04:00",
        "limit_per_member": None,
        "acceptance_required": False,
        "locations": [],
        "location_category": "ALL_LOCATIONS",
        "status": "planning",
        "priority": 0,
        "non_cumulative": False,
        "desktop_image": None,
        "non_cumulative_level": None,
        "template_params": {
            "targeted_segment": "SHR_test1_06072022"
        },
        "products": []
    })
    headers = {
        'Authorization': 'Bearer' + " " + access_token,
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)
    program_id = json.loads(response.text)['id']
    print(program_id)
    return program_id


def launch_offer(program_id, access_token):
    url = "https://maf-holding-dev.apigee.net/v1/gravty/status"

    payload = json.dumps({
        "offer_id": program_id,
        "status": "launched",
        "comment": "Launched"
    })
    headers = {
        'Authorization': 'Bearer' + " " + access_token,
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)


def lambda_handler(event, context):
    print("event")
    print(event)
    message_dict = json.dumps(event[0]['body'])
    # message_dict = json.loads(string_message)
    # print(type(string_message))
    # print(string_message)
    # print("dict")
    print(message_dict)

    # res = ast.literal_eval(message_dict)
    # print(type(res))
    # print(str(res))
    # res1 = eval(dict)
    # print(type(res1))
    # print(str(res1))

    id = message_dict['id']
    user_id = message_dict['user_id']
    touchpoints = message_dict['touchpoints']
    source = message_dict['source']
    name = message_dict['name']
    audiences = message_dict['audiences']
    audiences_id = message_dict['audiences']['id']
    audiences_campaign_name = message_dict['audiences']['campaign_name']
    audiences_campaign_type = message_dict['audiences']['campaign_type']
    audiences_campaign_query = message_dict['audiences']['campaign_query']
    audiences_date_created = message_dict['audiences']['date_created']
    audiences_last_modified = message_dict['audiences']['last_modified']
    audiences_business_unit = message_dict['audiences']['business_unit']
    audiences_concat = message_dict['audiences_concat']
    offer = message_dict['offer']
    offer_id = message_dict['offer']['id']
    offer_base_offer = message_dict['offer']['base_offer']
    offer_program_id = message_dict['offer']['program_id']
    offer_template_name = message_dict['offer']['template_name']
    offer_concat = message_dict['offer_concat']

    print(id)
    print(user_id)
    print(touchpoints)
    print(source)
    print(name)
    print(audiences)
    print(audiences_concat)
    print(audiences_campaign_name)
    print("offer")
    print(offer_id)
    print(offer_base_offer)
    print(offer_program_id)
    print(template_name)
    print(audiences_campaign_name)

    # access_token = basic_auth(get_secret()[1], get_secret()[2])
    # create_signed_url1 = create_signed_url(access_token, get_secret()[0])
    # signed_url = create_signed_url1[0]
    # filename = create_signed_url1[1]
    # idapi = process_segment_files(filename, access_token, get_secret()[0])
    # get_handback_files(idapi, access_token, get_secret()[0])
    # member_segment_code = create_member_segment(filename, access_token, get_secret()[0], name,member_segment_code=name)
    # get_member_segment(member_segment_code, access_token, get_secret()[0])
    # s3_upload(signed_url)
    # program_id = create_offer(access_token,target_segment=name)
    # launch_offer(program_id, access_token)
