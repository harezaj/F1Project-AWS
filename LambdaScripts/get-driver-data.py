import json
import boto3
from datetime import datetime, timezone
import urllib3
from os import listdir
from os.path import isfile, join
import io

local_file = "/tmp"
s3_client = boto3.client("s3")
s3_bucket = "jh-drivers-bucket"
chunk_size = 1000

#Required for API
headers = {
    'user-agent': 'JacobHareza'
}

#Creating key for S3 Bucket folder
def _get_key():
    dt_now = datetime.now(tz=timezone.utc)
    KEY = (
        dt_now.strftime("%Y-%m-%d")
        + "/"
    )
    return KEY

#Retrieving data from API
def get_data(c):
    
    get_path="https://ergast.com/api/f1/drivers.json?limit={}".format(c)
    http = urllib3.PoolManager()
    try:
        response = http.request(
            "GET",
            get_path,
            retries=urllib3.util.Retry(3),
            headers = headers,
        )
        
        
        all_data = json.loads(response.data)
        responses = []

        #Appending to a list
        responses.append(all_data)
        
        #Drilling down to Driver Data
        driverList = responses[0]['MRData']['DriverTable']['Drivers']
        
        for x in range(len(driverList)):
            driverList[x]['driverId'] = driverList[x]['driverId'].upper()
            if 'permanentNumber' not in driverList[x].keys():
                driverList[x]['permanentNumber'] = None
            if 'code' not in driverList[x].keys():
                driverList[x]['code'] = None
            driverList[x].pop('url')
        
    #Error Handling
    except KeyError as e:
        print(f"Wrong format url {get_path}", e)
    except urllib3.exceptions.MaxRetryError as e:
        print(f"API unavailable at {get_path}", e)
    return driverList
    
#Parsing Data for S3
def parse_data(json_data):
    
    return f'{json_data.get("driverId")},{json_data["permanentNumber"]},{json_data["givenName"]},{json_data["familyName"]},{json_data["dateOfBirth"]},{json_data["nationality"]},{json_data["code"]}\n'
    
#Saving JSON Data to Local file
def write_to_local(data, loc=local_file):
    file_name = loc + "/" + "drivers"
    with open(file_name, "w") as file:
        for elt in data:
            file.write(parse_data(elt))
    return file_name    

def download_data():
    data = get_data(chunk_size)
    write_to_local(data)
    return data

def lambda_handler(event, context):
    testdata = download_data()
    key = _get_key()
    
    #Loading Local File to S3 Bucket
    #Loops through directory
    files = [f for f in listdir(local_file) if isfile(join(local_file, f))]
    for f in files:
        s3_client.upload_file(local_file + "/" + f, s3_bucket, key + f)
        
        
    