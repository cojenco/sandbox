import requests
import json
import http
import uuid
import os

from google.cloud import storage
from google.auth.credentials import AnonymousCredentials

# Uncomment line below to enable logging.DEBUG
# http.client.HTTPConnection.debuglevel=5


# Emulator host
_API_ACCESS_ENDPOINT = os.getenv("STORAGE_EMULATOR_HOST", "http://127.0.0.1:9000")

# Fake project and service accounts for emulator use
_CONF_TEST_PROJECT_ID = "my-project-id"
_CONF_TEST_SERVICE_ACCOUNT_EMAIL = "my-service-account@my-project-id.iam.gserviceaccount.com"


# === Interact with Retry Test API === #


def _create_retry_test(method_name, instructions):
    preflight_post_uri = _API_ACCESS_ENDPOINT + "/retry_test"
    headers = {
        'Content-Type': 'application/json',
    }
    data_dict = {
        'instructions': {
            method_name: instructions
        }
    }
    data = json.dumps(data_dict)
    r = requests.post(preflight_post_uri, headers=headers, data=data)
    (r.text)
    return r.json()


def _get_retry_test(id):
    status_get_uri = "{base}{retry}/{id}".format(base=_API_ACCESS_ENDPOINT, retry="/retry_test", id=id)
    r = requests.get(status_get_uri)
    (r.text)
    return r.json()


def _run_retry_test(id, func, _preconditions, **resources):
    # create a new client to send the retry test ID using the x-retry-test-id header in each request
    client = storage.Client(
        project=_CONF_TEST_PROJECT_ID,
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": _API_ACCESS_ENDPOINT},
    )
    client._http.headers.update({"x-retry-test-id": id})
    func(client, _preconditions, **resources)



def _delete_retry_test(id):
    status_get_uri = "{base}{retry}/{id}".format(base=_API_ACCESS_ENDPOINT, retry="/retry_test", id=id)
    r = requests.delete(status_get_uri)


# === Populate Resources/Fixtures === #


def _populate_resource_bucket(client, resources):
    bucket = client.bucket(uuid.uuid4().hex)
    client.create_bucket(bucket)
    resources["bucket"] = bucket

def _populate_resource_object(client, resources):
    bucket_name = resources["bucket"].name
    blob = client.bucket(bucket_name).blob(uuid.uuid4().hex)
    blob.upload_from_string("hello world")
    blob.reload()
    resources["object"] = blob

def _populate_resource_notification(client, resources):
    bucket_name = resources["bucket"].name
    bucket = client.get_bucket(bucket_name)
    notification = bucket.notification()
    notification.create()
    notification.reload()
    resources["notification"] = notification

def _populate_resource_hmackey(client, resources):
    hmac_key, secret = client.create_hmac_key(
        service_account_email=_CONF_TEST_SERVICE_ACCOUNT_EMAIL, 
        project_id=_CONF_TEST_PROJECT_ID
    )
    resources["hmac_key"] = hmac_key 

resource_mapping = {
    "BUCKET": _populate_resource_bucket,
    "OBJECT": _populate_resource_object,
    "NOTIFICATION": _populate_resource_notification,
    "HMAC_KEY": _populate_resource_hmackey,
}

def _populate_resource(client, json_resource):
    resources = {}

    for r in json_resource:
        try: 
            func = resource_mapping[r]
            func(client, resources)
        except Exception as e:
            ("log warning here: {}".format(e))

    return resources


# === Library Methods for Sandbox Use=== #


def client_get_bucket(client, _preconditions, **resources):
    bucket = resources.get("bucket")
    client.get_bucket(bucket.name)

def bucket_create(client, _preconditions, **_):
    bucket = client.bucket(uuid.uuid4().hex)
    bucket.create()

def client_list_buckets(client, _preconditions, **_):
    buckets = client.list_buckets()
    for b in buckets:
        pass

def bucket_copy_blob(client, _preconditions, **resources):
    object = resources.get("object")
    bucket = client.bucket(resources.get("bucket").name)
    destination = client.create_bucket(uuid.uuid4().hex)
    if _preconditions:
        bucket.copy_blob(
            object, destination, new_name=uuid.uuid4().hex, if_generation_match=0
        )
    else:
        bucket.copy_blob(object, destination)

def bucket_get_notification(client, _preconditions, **resources):
    bucket = resources.get("bucket")
    notification = resources.get("notification")
    client.bucket(bucket.name).get_notification(notification.notification_id)

def blob_delete(client, _preconditions, **resources):
    bucket = resources.get("bucket")
    object = resources.get("object")
    blob = client.bucket(bucket.name).blob(object.name)
    if _preconditions:
        blob.delete(if_generation_match=object.generation)
    else:
        blob.delete()


# === Run Sandbox Main Test Function === #


def test_emulator_retry_test_api():

    ###### 1 - replace the correct endpoint(str) and instructions
    method_name = "storage.buckets.get"
    instructions = ["return-503", "return-503"]

    ###### 2 - fill in the correct preconditions(bool)
    _preconditions = True

    ###### 3 - default to populate all resources or enter only the needed resources
    json_resource = ["BUCKET", "OBJECT", "NOTIFICATION", "HMAC_KEY"]

    r = _create_retry_test(method_name, instructions)                      # create retry test and get unique identifier
    id = r["id"]

    client = storage.Client(                                               # create a client with anonymous credentials to populate resources
        project=_CONF_TEST_PROJECT_ID,
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": _API_ACCESS_ENDPOINT},
    )
    resources = _populate_resource(client, json_resource)                  # populate resources

    ###### 4 - replace get_bucket with the correct corresponding libarary method 
    _run_retry_test(id, client_get_bucket, _preconditions, **resources)    # run library test

    status_response = _get_retry_test(id)                                  # get status with unique identifier
    print(status_response)

    _delete_retry_test(id)                                                     # delete retry test


test_emulator_retry_test_api()
