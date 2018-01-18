#!/usr/bin/env python

# Copyright 2016 Datawire. All rights reserved.

"""scout2csv

Exports a DynamoDB table to CSV

Usage:
    scout2csv export [options]
    scout2csv query [options] <start_date> <end_date>
    scout2csv (-h | --help)
    scout2csv --version

Options:
    -a --app=<name>         Set the app to filter for.
    -o --output-file=<path> Set the output file for an export operation
    -r --region=<id>        Set the AWS region [default: us-east-1]
    -t --table=<name>       The name of the database table
    --include-prerelease    Include pre-release tracking information [default: false]
    --version               Show the version.
"""

import arrow
import boto3
import json
from boto3.dynamodb.conditions import Key, Attr

from docopt import docopt
from . import __version__

dynamo = None
table_name = None

ignore_ids = {
    # MISC (bad users / known issues)
    "f45cec26-54cf-47d1-8502-aef0c4ff1233",   # (running ambassador in a loop: Kelsey)
    "7c5b0d60-5474-4c52-ac78-af94ab836a05",   # (unknown: Kelsey)
    
    # KUBERNAUT
    "5f7557d4-f40d-447e-94d0-f215aa3c2ce7",   # phil
    "b7520e6d-c568-4d77-9210-32fdb3648d48",   # richard
    "03059e6f-004a-404f-9af5-bc6306b884cc",   # rafi
    "b793ee2a-1180-4987-aaa4-aaf05f3f42af",   # rafi (new laptop)
    "d9e1e5bc-1317-455c-8c35-091e30cee7bf",   # abhay
    "0fe139be-811a-41eb-a00f-599943b9927a",   # itamar
    "63d58097-a459-417e-9760-0938b4d3434b",   # flynn (laptop)
    "1a9d56b2-93e6-493f-88fc-ba7763531aee",   # jess hawks (ci)

    # TELEPRESENCE
    "1609113e-38e9-4001-84c7-4a4cf33c492a",   # phil
    "7e5b36f2-d3e0-4e2b-a37b-be14fd2549ad",   # rafi
    "8f590a1e-27df-4f92-b6e6-f8b3e5a8a41e",   # rafi (new laptop)
    "2931722c-fcd8-438c-a55d-246176ccbfa8",   # richard
    "2136a030-d50e-4c7d-bc21-2e1d59b283a8",   # abhay
    "58606e78-231e-4a13-b2cb-d0b04dd30f3e",   # itamar
    "38a51201-50ff-4797-b1f4-8be023c286e1",   # temp cloud
    "89536d13-bec7-4a59-af01-20090ef6202a",   # jess hawks (ci)
    "51e5f54e-14fa-457c-8e65-64ff30cefaeb",   # flynn (laptop)
    
    # FORGE
    "e24f7027-7fae-458d-b7f1-01f20f0a2f14",   # phil (work)
    "ba0625d2-3ba5-4893-9ca1-a47a05198736",   # phil (home)
    "b177a58c-5115-4fd0-aa42-deb4a93e2247",   # rafi
    "c522b1d4-41dc-43ed-a0c4-67d52e2cad94",   # rafi (new laptop)
    "18c16ac9-945e-4d88-8bbf-706de6d76448",   # richard

    # AMBASSADOR
    "efd66fbb-f92d-4871-8d8a-3d51a617b316",   # flynn (laptop)
    "0db93bcc-29f3-4db8-b1ec-578a56760f9a",   # flynn (GKE)
    "288ef8d9-c1f8-450e-940a-779ab2aedd7d",   # richard (GKE)
    "2cf290ed-1f32-451b-9ed9-f4483405398d",   # rafi (laptop)   
}


applications = {
    "ambassador",
    "forge",
    "kubernaut",
    "kubernaut-api",
    "loom",
    "telepresence",
}


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def normalize_metadata_keys(metadata):
    return {k.lower(): v for k, v in metadata.items()}


def merge_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z


def normalize_item(item):
    metadata = {}
    for k, v in item["metadata"].items():
        metadata["meta_{}".format(k)] = v

    scouted = {
        "report_id":   item["report_id"],
        "report_time": arrow.get(item["report_time"]).datetime,
        "application": item["application"],
        "install_id":  item["install_id"],
        "version":     item["version"],
        "user_agent":  item["user_agent"],
        "client_ip":   item.get("client_ip", {"S": "unknown"})
    }

    return merge_dicts(scouted, metadata), metadata.keys()


def normalize_item2(item):
    metadata = {}
    for k, v in item["metadata"]["M"].items():
        metadata[k.lower()] = v["S"]

    scouted = {
        "report_id":   item["report_id"]["S"],
        "report_time": arrow.get(item["report_time"]["S"]).datetime,
        "application": item["application"]["S"],
        "install_id":  item["install_id"]["S"],
        "version":     item["version"]["S"],
        "user_agent":  item["user_agent"]["S"],
        "metadata":    metadata
    }

    return scouted


def get_keen_collection(app_name):
    app_name = str(app_name).lower()

    if app_name in applications and app_name.endswith("-api"):
        return "api_access"
    elif app_name in applications:
        return "app_session"
    else:
        return "unknown"


def build_event(collection, payload):
    result = {
        "app": payload.get("application"),
        "app_version": payload.get("version"),
        "app_id":  payload.get("install_id"),
        "user_agent": payload.get("user_agent")
    }

    if collection == "api_access":
        result["api_id"] = payload.get("api_id")
        result["user_id"] = payload.get("user_id"),
        result["user_email"] = payload.get("user_email")

    result["metadata"] = normalize_metadata_keys(payload.get("metadata", {}))
    del result["metadata"]["user-agent"]

    return result


def export(args):
    import csv
    import time

    items, meta_keys = scan()
    total_items = len(items)

    ignored_items_by_id = 0
    ignored_items_by_prerelease = 0
    final_items = []

    for it in items:

        if it["install_id"] in ignore_ids:
            ignored_items_by_id += 1
            continue

        a = ["+", "-", "_"]
        if any(x in it.get("version") for x in a) and not bool(args["--include-prerelease"]):
            ignored_items_by_prerelease += 1
            continue

        if args["--app"] and it.get("application") == args["--app"]:
            print(args["--app"])
            print(it.get("application"))
            final_items.append(it)
        else:
            final_items.append(it)

    file = args["--output-file"] or "scout-{}.csv".format(time.time())

    with open(file, "w+") as csv_file:
        fieldnames = ["application",
                      "report_id",
                      "report_time",
                      "install_id",
                      "user_agent",
                      "version",
                      "client_ip"
                      ] + list(meta_keys)

        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(final_items)

    print("Total Items                      = {}".format(total_items))
    print("Legitimate Items                 = {}".format(len(final_items)))
    print("Ignored Items (id)               = {}".format(ignored_items_by_id))
    print("Ignored Items (pre-release)      = {}".format(ignored_items_by_prerelease))
    print("Ignored Items (id + pre-release) = {}".format(ignored_items_by_id + ignored_items_by_prerelease))


def export_by_query(args):
    import csv
    import datetime
    import time

    q_start_time = datetime.datetime.now()

    items, meta_keys = [], []
    ignored_items_by_id = 0
    ignored_items_by_prerelease = 0
    final_items = []

    app = args.get("--app")
    if app:
        app = [app]
    else:
        app = applications

    for a in app:
        q_items, q_meta_keys = query(a, args)
        for it in q_items:

            if it["install_id"] in ignore_ids:
                ignored_items_by_id += 1
                continue

            a = ["+", "-", "_"]
            if any(x in it.get("version") for x in a) and not bool(args["--include-prerelease"]):
                ignored_items_by_prerelease += 1
                continue

            final_items.append(it)

        meta_keys.extend(q_meta_keys)

    q_end_time = datetime.datetime.now()
    q_total_time = q_end_time - q_start_time

    file = args["--output-file"] or "scout-{}.csv".format(time.time())

    with open(file, "w+") as csv_file:
        fieldnames = ["application",
                    "report_id",
                    "report_time",
                    "install_id",
                    "user_agent",
                    "version",
                    "client_ip"
                    ] + meta_keys

        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(final_items)

    print("Query Time (seconds)             = {}".format(int(q_total_time.total_seconds())))
    print("Legitimate Items                 = {}".format(len(final_items)))
    print("Ignored Items (id)               = {}".format(ignored_items_by_id))
    print("Ignored Items (pre-release)      = {}".format(ignored_items_by_prerelease))
    print("Ignored Items (id + pre-release) = {}".format(ignored_items_by_id + ignored_items_by_prerelease))


def scan():
    items = []
    meta_keys = set()

    last_key = None

    while True:
        result = None
        if last_key is None:
            result = dynamo.scan(TableName=table_name)
        else:
            result = dynamo.scan(TableName=table_name, ExclusiveStartKey=last_key)

        for i in result["Items"]:
            normalized, mk = normalize_item(i)
            meta_keys.update(mk)
            items.append(normalized)

        last_key = result.get("LastEvaluatedKey", None)
        if last_key is None:
            break

    items.sort(key=lambda item: item["report_time"], reverse=True)
    return items, meta_keys


def query(app, args):
    start_date = args["<start_date>"]
    end_date = args["<end_date>"]

    print("Query 'table = {}' between 'start_date = {}' and 'end_date = {}'".format(table_name, start_date, end_date))

    db = boto3.resource('dynamodb', region_name='us-east-1')
    table = db.Table(table_name)

    items = []
    meta_keys = set()

    last_key = None

    while True:
        result = None
        if last_key is None:
            result = table.query(
                KeyConditionExpression=Key("application").eq(app) &
                                       Key("report_time").between(start_date, end_date),
                Limit=1000
            )
        else:
            result = table.query(
                ExclusiveStartKey=last_key,
                KeyConditionExpression=Key("application").eq(app) &
                                       Key("report_time").between(start_date, end_date),
                Limit=1000
            )

        for i in result["Items"]:
            normalized, mk = normalize_item(i)
            meta_keys.update(mk)
            items.append(normalized)

        last_key = result.get("LastEvaluatedKey", None)
        if last_key is None:
            break

    items.sort(key=lambda item: item["report_time"], reverse=True)
    return items, meta_keys

    # response = table.query(
    #     Limit=1000,
    #     KeyConditionExpression=Key("application").eq("") & Key("report_time").between(start_date, end_date)
    # )
    #
    # for i in response['Items']:
    #     print(json.dumps(i, cls=DecimalEncoder))
    #
    # while 'LastEvaluatedKey' in response:
    #     response = table.query(
    #         Limit=1000,
    #         KeyConditionExpression=Key("application").eq("ambassador") & Key("report_time").between(start_date, end_date),
    #         ExclusiveStartKey=response['LastEvaluatedKey']
    #     )
    #
    # for i in response['Items']:
    #     print(json.dumps(i, cls=DecimalEncoder))
    #
    # print(response)


def run(args):
    global dynamo
    dynamo = boto3.client("dynamodb", region_name=args["--region"])

    global table_name
    table_name = args["--table"]

    if args["export"]:
        export(args)
    if args["query"]:
        export_by_query(args)


def main():
    exit(run(docopt(__doc__, version="scout2csv {0}".format(__version__))))


if __name__ == "__main__":
    main()
