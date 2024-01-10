import csv
import psycopg2
import configparser
import os.path as path
import os
import boto3

"""
VARIABLES
"""
# list of tables in postgreSQL server
table_names = ["actor", "address", "category", "city", "country", "customer", "film", "film_actor",
"film_category", "inventory", "language", "payment", "rental", "staff", "store"]

suffix = "extract.csv"


"""
EXTRACT DATA FROM POSTGRESQL DATABASE
"""
# get postgreSQL configuration
parser = configparser.ConfigParser()
parser.read(path.dirname(__file__) + "\..\pipeline.conf")
dbname = parser.get("postgreSQL_config", "dbname")
host = parser.get("postgreSQL_config", "host")
port = parser.get("postgreSQL_config", "port")
user = parser.get("postgreSQL_config", "user")
password = parser.get("postgreSQL_config", "password")

# create a connection to postgreSQL server using psycopg2
conn = psycopg2.connect(
    database=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

if conn is None:
    print("Could not connect to postgreSQL server.")
else:
    print("Connection established!")

# extract data from sql server and save it in local machine as csv files
cursor = conn.cursor()
file_path = path.dirname(__file__) + "\..\data"

# create folder if not exist
if not path.exists(file_path):
    os.makedirs(file_path)

for i in range(len(table_names)):
    query = "SELECT * FROM {}".format(table_names[i])
    local_filename = "{0}_{1}".format(table_names[i], suffix)

    # execute sql query and get results 
    cursor.execute(query)
    results = cursor.fetchall()

    # save table as csv files
    with open(file_path + "\\" + local_filename, 'w') as f:
        csv_w = csv.writer(f, delimiter='|')
        csv_w.writerows(results)

cursor.close()
conn.close()


"""
UPLOAD TO AWS S3 STORAGE
"""
# get aws s3 configuration
parser = configparser.ConfigParser()
parser.read(path.dirname(__file__) + "\..\pipeline.conf")
access_key = parser.get("aws_s3_config", "access_key")
secret_key = parser.get("aws_s3_config", "secret_key")
bucket_name = parser.get("aws_s3_config", "bucket_name")

s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

for i in range(len(table_names)):
    local_filename = "{0}_{1}".format(table_names[i], suffix)
    s3_filename = local_filename

    # upload to s3
    s3.upload_file(file_path + "\\" + local_filename, bucket_name, "data/{}".format(s3_filename))

print("Task completed!")
    



