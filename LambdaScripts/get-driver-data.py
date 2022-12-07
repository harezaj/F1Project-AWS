import json
import boto3
from datetime import datetime, timezone
import urllib3
from os import listdir
from os.path import isfile, join
import io

local_file = "/tmp"
s3_client = boto3.client("s3")
s3_bucket = "drivers-lamda-test"
chunk_size = 1000
headers = {
    'user-agent': 'JacobHareza'
}


def _get_key():
    dt_now = datetime.now(tz=timezone.utc)
    KEY = (
        dt_now.strftime("%Y-%m-%d")
        + "/"
        + dt_now.strftime("%H")
        + "/"
        + dt_now.strftime("%M")
        + "/"
    )
    return KEY

def get_data(
    get_path="http://ergast.com/api/f1/drivers.json?limit=1000"
):
    http = urllib3.PoolManager()
    try:
        response = http.request(
            "GET",
            get_path,
            retries=urllib3.util.Retry(3),
        )
        
        
        all_data = json.loads(response.data)
        responses = []

        responses.append(all_data)
        driverList = responses[0]['MRData']['DriverTable']['Drivers']
        
    except KeyError as e:
        print(f"Wrong format url {get_path}", e)
    except urllib3.exceptions.MaxRetryError as e:
        print(f"API unavailable at {get_path}", e)
    return driverList
    
def parse_data(json_data):
    return f'{json_data.get("driverId")},{json_data["url"]},{json_data["givenName"]},{json_data["familyName"]},{json_data["dateOfBirth"]},{json_data["nationality"]}\n'
    
def write_to_local(data, loc=local_file):
    file_name = loc + "/" + "test"
    with open(file_name, "w") as file:
        for elt in data:
            file.write(parse_data(elt))
    return file_name    

def download_data():
    data = get_data()
    write_to_local(data)
    return data

def lambda_handler(event, context):
    testdata = download_data()
    key = _get_key()
    
    files = [f for f in listdir(local_file) if isfile(join(local_file, f))]
    for f in files:
        s3_client.upload_file(local_file + "/" + f, s3_bucket, key + f)
        
    return {
        'statusCode': 200,
        'body': testdata
    }