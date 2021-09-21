import logging
import base64
import json
import os
import requests
import azure.functions as func
from azure.storage.blob import BlockBlobService



# workspace_username=os.environ['workspace_username']
# workspace_location=os.environ['workspace_location']
# token = os.environ['workspace_token']
# storage_account=os.environ['storage_account']
# storage_access_key=os.environ['storage_access_key']
# container=os.environ['container']

workspace_username = "divyansh.jain@yashtechnologies841.onmicrosoft.com"
workspace_location = "adb-537667343616597.17"
token = "dapi4b4cc457063a7127aaf3b48e51c6b5cd-3"
storage_account = "bankingsaadls"
storage_access_key = "UKBq6IplkV25D4KYq0NOyue7CxMtCuTzbQc41dSn6F/T397iqA27Eq0aPp6To7QlL0nqBld9v7mfNQCfCg+1FA=="
container = "demodb"


# def main(req: func.HttpRequest) -> func.HttpResponse:
logging.info('Python HTTP trigger function processed a request')

dct = dict()
lst = ["Notebook1.py", "Notebook2.py"]

for notebookname in lst:
    blob = BlockBlobService(account_name=storage_account, account_key=storage_access_key)

    notebooknamewithoutext = notebookname.split(".")
    notebooknamewithoutext = notebooknamewithoutext[0]
    path = "/Users/"+workspace_username+"/"+notebooknamewithoutext
    dct[notebooknamewithoutext+"path"] = path
    filedata = blob.get_blob_to_bytes(container, 'notebooks/'+notebookname).content
    base64_two = base64.b64encode(filedata).decode('ascii')
    url = "https://"+workspace_location+".azuredatabricks.net/api/2.0/workspace/import"
    data = {
        "content": base64_two,
        "path": path,
        "language": "PYTHON",
        "overwrite": True,
        "format": "SOURCE"
        }
    headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}
    resp = requests.post(url, data=json.dumps(data), headers=headers)


result = json.dumps(dct)
print(result)

    # return func.HttpResponse(result)
