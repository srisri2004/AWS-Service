"""Deletes everything created by deploy.sh (stack + both buckets incl. object versions).
Usage: python scripts/cleanup.py salesdl"""
import sys
import boto3

project = sys.argv[1] if len(sys.argv) > 1 else "salesdl"
session = boto3.session.Session()
region = session.region_name
account = boto3.client("sts").get_caller_identity()["Account"]
buckets = [f"{project}-data-{account}-{region}", f"{project}-artifacts-{account}-{region}"]
s3 = boto3.resource("s3")

for name in buckets:                       # empty first (versioned buckets need version delete)
    b = s3.Bucket(name)
    try:
        b.object_versions.delete()
        print("emptied", name)
    except Exception as exc:
        print("skip", name, exc)

cf = boto3.client("cloudformation")
cf.delete_stack(StackName=f"{project}-stack")
cf.get_waiter("stack_delete_complete").wait(StackName=f"{project}-stack")
print("stack deleted")
for name in buckets:
    try:
        s3.Bucket(name).delete()
        print("deleted", name)
    except Exception as exc:
        print("skip", name, exc)
