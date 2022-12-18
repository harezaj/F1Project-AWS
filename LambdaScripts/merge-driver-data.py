import json
import boto3
from botocore.exceptions import ClientError
import psycopg2
from datetime import date

s3_bucket = 'jh-drivers-bucket'
s3_object = str(date.today()) + '/drivers'
s3_iam_role = 'arn:aws:iam::623002344831:role/redshift_s3_full_access'


def get_secret():

    secret_name = "redshift_f1_cluster"

    client = boto3.client('secretsmanager')

    try:
        response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = json.loads(response['SecretString'])
    return secret
    
def connect_redshift(secret):
    # establishing connection to redshift
    try:
        conn = psycopg2.connect(dbname = 'formulaone', 
                                host = secret['host'], 
                                port = secret['port'], 
                                user = secret['username'], 
                                password = secret['password'])  
        return conn
    except Exception as e:
        print(f"Unable to connect to Redshift. Error {e}")
        sys.exit(1)
        
def data_queries(rs_conn):
      # creating temp table to load date into
    # will merge from this table into driver_info table
    create_temp_table_sql = '''
    
    Drop Table if Exists drivers.loaddrivers;
    
    CREATE TABLE drivers.loaddrivers
    (
        ref_name text
        ,permanent_number text
    	,first_name text
    	,last_name text
    	,date_of_birth text
    	,nationality text
    	,code text
    );
    
    '''

    # copying data from s3 to redshift temp table created above
    copy_sql = '''
    copy drivers.loaddrivers
    from 's3://{}/{}'
    iam_role '{}'
    delimiter ','
    ;
    '''.format(s3_bucket,s3_object,s3_iam_role)
    
    # merging data into driver_info table
    merge_sql = '''
    INSERT INTO drivers.driver_info (
    	ref_name
    	,first_name
    	,last_name
    	,date_of_birth
    	,nationality
    	,permanent_number
    	,code
    	)
    SELECT di.ref_name
    	,di.first_name
    	,di.last_name
    	,CAST(di.date_of_birth AS DATE)
    	,di.nationality
    	,case when di.permanent_number = 'None' then null else CAST(di.permanent_number AS INTEGER) end
    	,case when di.code = 'None' then null else di.code end
    FROM drivers.loaddrivers di
    LEFT JOIN drivers.driver_info d ON d.ref_name = di.ref_name
    WHERE d.ref_name IS NULL
    ;
    '''
    
    drop_temp_table_sql = '''
    Drop Table if Exists loaddrivers;
    '''
    
    
    with rs_conn:
        cursor = rs_conn.cursor()
        
        cursor.execute(create_temp_table_sql)
        cursor.execute(copy_sql)
        cursor.execute(merge_sql)
        cursor.execute(drop_temp_table_sql)
    
        rs_conn.commit()

def lambda_handler(event, context):
    db_secret = get_secret()
    conn = connect_redshift(db_secret)
    data_queries(conn)