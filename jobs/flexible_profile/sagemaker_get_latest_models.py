import boto3
import json
from datetime import datetime
import re
from dateutil.parser import parse


def find_date(string):
    date_regex = r"\d{4}-\d{2}-\d{2}"  # Assuming the date format is YYYY-MM-DD
    match = re.search(date_regex, string)
    if match:
        date_str = match.group(0)
        try:
            date = parse(date_str).date()
            return date
        except ValueError:
            return parse("1970-01-01").date()
    else:
        return parse("1970-01-01").date()


def find_date_fuzzy(string):
    try:
        date = parse(string, fuzzy=True).date()
        return date
    except ValueError:
        parse("1970-01-01").date()


def get_most_recent_model(input_name, models_list):
    input_name = str(input_name).replace('.', '-')
    matching_models = [model for model in models_list if f"{input_name}" in model]

    if not matching_models:
        return None

    print("Found models for:", input_name)
    for x in matching_models:
        print(x)

    try:
        sorted_models = sorted(matching_models, key=lambda x: find_date(x), reverse=True)
        return sorted_models[0]
    except ValueError:
        return None


def lambda_handler(event, context):
    sm_client = boto3.client('sagemaker')

    results = (
        sm_client.get_paginator('list_models')
        .paginate()
        .build_full_result()
    )

    model_names = [model['ModelName'] for model in results['Models']]
    print("Model list:")
    for name in model_names:
        print(name)

    datacontext = event["input"]["data_filter"]["context"]
    print("Context:", datacontext)
    recentmodel = get_most_recent_model(datacontext, model_names)

    print("Recent Model:", recentmodel)
    event["input"]["data_filter"]["ModelName"] = recentmodel
    print("response:", event)

    return event