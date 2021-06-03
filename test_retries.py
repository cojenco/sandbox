import requests
import io
import json
import http
import uuid
http.client.HTTPConnection.debuglevel=5

from google.cloud import storage


_API_ACCESS_ENDPOINT = "http://127.0.0.1:9000"
_CONF_TEST_PROJECT_ID = "my-project-id"
_CONF_TEST_SERVICE_ACCOUNT_EMAIL = "my-service-account@my-project-id.iam.gserviceaccount.com"


# === Interaction with Retry Test API === #


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
    print(r.text)
    return r.json()


def _get_retry_test(id):
    status_get_uri = "{base}{retry}/{id}".format(base=_API_ACCESS_ENDPOINT, retry="/retry_test", id=id)
    r = requests.get(status_get_uri)
    print(r.text)
    return r.json()


def _run_retry_test(id, func, _preconditions, **resources):
    client = storage.Client(client_options={"api_endpoint": _API_ACCESS_ENDPOINT})
    client._http.headers.update({"x-retry-test-id": id})
    func(client, _preconditions, **resources)
    # try:
    #     func(client, _preconditions, **resources)
    #     print("we made it!")
    # except Exception as e:
    #     print("uhoh")
    #     print(type(e))
    #     print(e)


def _delete_retry_test(id):
    status_get_uri = "{base}{retry}/{id}".format(base=_API_ACCESS_ENDPOINT, retry="/retry_test", id=id)
    r = requests.delete(status_get_uri)
    print(r.text)


# === Populate Resources/Fixtures === #


def _populate_resource_bucket(client, resources):
    bucket = client.bucket(uuid.uuid4().hex)
    client.create_bucket(bucket)
    resources["bucket"] = bucket

def _populate_resource_object(client, resources):
    bucket_name = resources["bucket"].name
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(uuid.uuid4().hex)
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
            print("log warning here: {}".format(e))

    return resources


# === Library Methods === #


def test_iam_permissions(client, _preconditions, bucket):
    bucket = client.bucket(bucket.name)
    permissions = ["storage.buckets.get", "storage.buckets.create"]
    bucket.test_iam_permissions(permissions)

def list_blobs(client, _preconditions, bucket, **_):
    blobs = client.list_blobs(bucket.name)
    for b in blobs:
        break

def get_blob(client, _preconditions, bucket, **_):
    bucket = client.bucket(bucket.name)
    bucket.get_blob(object.name)

def get_bucket(client, _preconditions, bucket):
    bucket = client.get_bucket(bucket.name)


def make_bucket_public(client, _preconditions, bucket):
    bucket = client.bucket(bucket.name)
    bucket.make_public()

def make_blob_public(client, _preconditions, bucket, object):
    blob = client.bucket(bucket.name).blob(object.name)
    blob.make_public()

def get_bucket_acl(client, _preconditions, bucket):
    bucket = client.bucket(bucket.name)
    bucket_acl = bucket._acl
    entity = bucket_acl.entity("allUsers")
    bucket_acl.reload()
    print(bucket_acl.has_entity(entity))

def get_service_account_email(client, _preconditions, **_):
    client.get_service_account_email()


def patch_bucket(client, _preconditions, bucket):
    bucket = client.bucket(bucket.name)
    metadata = {"foo": "bar"}
    bucket.metadata = metadata
    if _preconditions:
        bucket.reload()
        metageneration = bucket.metageneration
        print("!!!!!")
        print(metageneration)
        bucket.patch(if_metageneration_match=metageneration)
    else:
        bucket.patch()


def list_buckets(client, _preconditions, **_):
    buckets = client.list_buckets()
    for b in buckets:
        print(b)

# storage.objects.copy
def rename_blob(client, _preconditions, bucket, object):
    bucket = client.bucket(bucket.name)
    new_name = "new_name"
    if _preconditions:
        generation = object.generation
        print(generation)
        bucket.rename_blob(object, new_name, if_generation_match=generation)
    else:
        bucket.rename_blob(object, new_name)

# storage.objects.copy
def copy_blob(client, _preconditions, bucket, object):
    bucket = client.bucket(bucket.name)
    dest = client.bucket("bucket")
    new_name = "new_name"
    if _preconditions:
        generation = object.generation
        print(generation)
        bucket.copy_blob(object, dest, new_name=new_name, if_generation_match=object.generation)
    else:
        bucket.copy_blob(object, dest, new_name=new_name)

# storage.objects.delete
def delete_blob(client, _preconditions, bucket, object):
    bucket = client.bucket(bucket.name)
    if _preconditions:
        generation = object.generation
        print(generation)
        bucket.delete_blob(object.name, if_generation_match=generation)
    else:
        bucket.delete_blob(object.name)

# storage.buckets.lockRententionPolicy
def lock_retention_policy(client, _preconditions, bucket):
    bucket2 = client.bucket("bucket")
    bucket2.retention_period = 60
    bucket2.patch()
    bucket2.lock_retention_policy()

# storage.objects.insert
def upload_from_string(client, _preconditions, bucket):
    blob = client.bucket(bucket.name).blob(uuid.uuid4().hex)
    if _preconditions:    
        blob.upload_from_string("upload from string", if_metageneration_match=0)
    else:
        blob.upload_from_string("upload from string")

# Q: object generation is randomly created in the emulator
# storage.objects.copy
def copy_blob(client, _preconditions, bucket, object):
    bucket = client.bucket(bucket.name)
    destination = client.bucket("bucket")
    new_name = "new_name"
    if _preconditions:
        bucket.copy_blob(object, destination, new_name=new_name, if_generation_match=0)
    else:
        bucket.copy_blob(object, destination)

# Q: google.protobuf.json_format.ParseError
# storage.objects.compose
def compose_blob(client, _preconditions, bucket, object):
    blob_2 = bucket.blob("blob2")
    blob_2.upload_from_string("second blob coming")
    sources = [blob_2]
    generations = [blob_2.generation]
    
    if _preconditions:
        object.compose(sources, if_generation_match=generations)
    else:
        object.compose(sources)

# storage.buckets.setIamPolicy
def set_iam_policy(client, _preconditions, bucket):
    bucket = client.get_bucket("bucket")
    role = "roles/storage.objectViewer"
    member = _CONF_TEST_SERVICE_ACCOUNT_EMAIL

    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.bindings.append({
        "role": role, 
        "members": {
            member
        }
    })

    if _preconditions:
        bucket.set_iam_policy(policy)
    else:
        bucket.set_iam_policy(policy)


# === Run Main Function === #


def test_emul_api():

    ###### 1 - replace the correct endpoint(str)
    method_name = "storage.objects.copy"
    instructions = ["return-503", "return-503"]

    ###### 2 - fill in the correct preconditions(bool)
    _preconditions = True

    ###### 3 - enter the correct resources needed
    json_resource = ["BUCKET", "OBJECT"]

    r = _create_retry_test(method_name, instructions)   # create retry test and get unique identifier
    id = r["id"]

    client = storage.Client(client_options={"api_endpoint": _API_ACCESS_ENDPOINT})
    resources = _populate_resource(client, json_resource)   # populate resources
    print(resources)

    ###### 4 - replace get_bucket with the correct corresponding libarary method 
    _run_retry_test(id, copy_blob, _preconditions, **resources)    # run library test

    # get status with unique identifier
    status_response = _get_retry_test(id)

test_emul_api()
