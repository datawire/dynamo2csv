#!/usr/bin/env python

# Copyright 2016 Datawire. All rights reserved.

"""scout2csv

Exports a DynamoDB table to CSV

Usage:
    scout2csv export [options]
    scout2csv (-h | --help)
    scout2csv --version

Options:
    -o --output-file=<path> Set the output file for an export operation
    -r --region=<id>        Set the AWS region [default: us-east-1]
    -t --table=<name>       The name of the database table
    --version               Show the version.
"""

import arrow
import boto3

from docopt import docopt
from . import __version__

dynamo = None
table_name = None

ignore_ids = {
    "5f7557d4-f40d-447e-94d0-f215aa3c2ce7",  # phil
    "b7520e6d-c568-4d77-9210-32fdb3648d48",  # richard
    "03059e6f-004a-404f-9af5-bc6306b884cc"   # rafi
}


def merge_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z


def normalize_item(item):
    metadata = {}
    for k, v in item["metadata"]["M"].items():
        metadata["meta_{}".format(k)] = v["S"]

    scouted = {
        "report_id":   item["report_id"]["S"],
        "report_time": arrow.get(item["report_time"]["S"]).datetime,
        "application": item["application"]["S"],
        "install_id":  item["install_id"]["S"],
        "user_agent":  item["user_agent"]["S"],
    }

    return merge_dicts(scouted, metadata), metadata.keys()


def export(args):
    import csv
    import time

    items, meta_keys = scan()

    file = args["--output-file"] or "scout-{}.csv".format(time.time())

    with open(file, "w+") as csv_file:
        fieldnames = ["application", "report_id", "report_time", "install_id", "user_agent"] + list(meta_keys)
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(items)


def scan():
    items = []
    meta_keys = set()

    last_key = None
    while True:
        result = None
        if last_key is None:
            result = dynamo.scan(TableName=table_name)
        else:
            result = dynamo.scan(table_name=table_name, ExclusiveStartKey=last_key)

        for i in result["Items"]:
            normalized, mk = normalize_item(i)
            meta_keys.update(mk)

            if normalized['install_id'] not in ignore_ids:
                items.append(normalized)

        last_key = result.get("LastEvaluatedKey", None)
        if last_key is None:
            break

    items.sort(key=lambda item: item["report_time"], reverse=True)
    return items, meta_keys


def run(args):
    global dynamo
    dynamo = boto3.client("dynamodb", region_name=args["--region"])

    global table_name
    table_name = args["--table"]

    if args["export"]:
        export(args)


def main():
    exit(run(docopt(__doc__, version="scout2csv {0}".format(__version__))))


if __name__ == "__main__":
    main()
