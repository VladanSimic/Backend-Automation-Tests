#connection to AWS
def lambda_handler(event, context):
    # Retrieve environment variables
    username = os.getenv('SF_USER_NAME')
    password = os.getenv('SF_PASSWORD')
    account = os.getenv('SF_ACCOUNT')
    database = os.getenv('SF_DATABASE_NAME')
    schema = os.getenv('SF_SCHEMA')
    warehouse = os.getenv('SF_WAREHOUSE')
    sender = os.getenv('SENDER_EMAIL')
    recipient = os.getenv('RECIPIENT_EMAIL')
    smtp_server = os.getenv('SMTP_SERVER')
    smtp_port = int(os.getenv('SMTP_PORT', 587))
    smtp_user = os.getenv('SMTP_USER')
    smtp_password = os.getenv('SMTP_PASSWORD')

def execute_query(query):
    try:
        # Open a connection to Snowflake
        conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account,
            database=database,
            schema=schema,
            warehouse=warehouse
        )
        
        # Create a cursor to execute the query, A SORT OF CONSTRUCTOR FOR QUERY
        cursor = conn.cursor()
        
        # Execute the query
        cursor.execute(query)
        
        # Fetch the results
        results = cursor.fetchall()
        
        # Convert results to an array (list of tuples)
        data = [list(row) for row in results]
        
        # Print or process the results
        return data

    except Exception as e:
        print(f"Error occurred: {e}")
   
    # Always close the cursor and connection
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def calculate_long_jobs(array_b, array_a):
    array = []
    for row_b in array_b:
        for row_a in array_a:
            # Match based on common keys (first three elements),
            # b - AVG
            # a - CUR
            #looking at only first 3 rows se if they match each other
            if row_b[:3] == row_a[:3]:
                # Get the values to compare the values
                b_last_value = row_b[-1] * 1.25
                a_last_value = row_a[-1]
                
                # Check condition
                if b_last_value > a_last_value:
                    array.append(row_b)
    return array
    
def format_report(report):
    job_type_data = {}

    # Group report data by job_type
    for row in report:
        #get the first element from the current row
        job_type = row[0]

    # Construct the email body
    email_body = "Hello,\n\ntoday we have issues with:\n\n"

    for job_type, rows in job_type_data.items():
        email_body += f"'{job_type}'. For this job we see issues on:\n\n"

        # Create a table for the rows
        table = PrettyTable()
        table.field_names = ["INSTANCE_ID", "TENANT_ID", "CURRENT_DURATION"]
        for row in rows:
            table.add_row(row)

        # Add table to the email body
        email_body += table.get_string() + "\n\n"

    email_body += "Regards"
    return email_body

def send_email(subject, body, sender, recipient, smtp_server, smtp_port):
    # Create a MIME message
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = subject

    # Attach the email body
    msg.attach(MIMEText(body, 'plain'))

    # Connect to the SMTP server and send the email
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()  # Upgrade the connection to secure
        server.sendmail(sender, recipient, msg.as_string())


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from prettytable import PrettyTable

import snowflake.connector
import os
import time

#for JA properties file reading values
import configparser

# Specify the path to your properties file
file_path = "files/JA.properties"

#create an empty dictionary
credentials = {}

with open(file_path, 'r') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#"):  # Skip empty lines and comments
            key, value = line.split("=", 1)  # Split key-value pairs
            credentials[key.strip()] = value.strip()

# Access credentials
host = credentials.get('SF_HOST_NAME')
username = credentials.get('SF_USER_NAME')
password = credentials.get('SF_PASSWORD')
#gets first 2 elements separated by dot not counting the last following dot in the host
account = '.'.join(host.split('.')[:2])
database = credentials.get('SF_DATABASE_NAME')
schema = credentials.get('SF_JA_CONTAINER')
warehouse = credentials.get('SF_WAREHOUSE_XSMALL_NAME')

#print credentials just to if it works and if we get real values
print(f"Username: {username}")
print(f"Password: {password}")
print(f"Host: {host}")
print(f"DB: {database}")
print(f"Schema: {schema}")
print(f"Warehouse: {warehouse}")
print(f"Account: {account}")

# Query you want to execute - it must have triple quotes
query = f"""select job_type, instance_id, tenant_id, avg(datediff(minute,start_time,finish_time)) as avg_duration  from {database}.{schema}.job_log where
start_time > timeadd(day,-10, CURRENT_TIMESTAMP)
and job_state = 'FINISH'
group by job_type, instance_id, tenant_id
order by avg_duration desc"""

avg_duration_of_success_finished_jobs = execute_query(query)

query = f"""select job_type, instance_id, tenant_id, job_state, datediff(minute,start_time,CURRENT_TIMESTAMP) as duration from {database}.{schema}.job_log where
start_time > timeadd(minute,-18000,CURRENT_TIMESTAMP) and start_time < timeadd(minute,-60,CURRENT_TIMESTAMP)
and finish_time is null
order by start_time asc"""

current_status_of_the_jobs = execute_query(query)

fl_success_array, cte_success_array, fl_bu_success_array, sts_psd_success_array, sts_mat_success_array = [], [], [], [], []
fl_current_array, cte_current_array, fl_bu_current_array, sts_psd_current_array, sts_mat_current_array = [], [], [], [], []

for job in avg_duration_of_success_finished_jobs:
    if job[0] == 'FULL_LOAD':
        fl_success_array.append(job)
    elif job[0] == 'STS_PSD_REFRESH':
        sts_psd_success_array.append(job)
    elif job[0] == 'STS_MAT_REFRESH':
        sts_mat_success_array.append(job)
    elif job[0] == 'FULL_LOAD_BU':
        fl_bu_success_array.append(job)
    elif job[0] == 'CUSTOM_TABLE_EXPORT':
        cte_success_array.append(job)

for job in current_status_of_the_jobs:
    if job[0] == 'FULL_LOAD':
        fl_current_array.append(job)
    elif job[0] == 'STS_PSD_REFRESH':
        sts_psd_current_array.append(job)
    elif job[0] == 'STS_MAT_REFRESH':
        sts_mat_current_array.append(job)
    elif job[0] == 'FULL_LOAD_BU':
        fl_bu_current_array.append(job)
    elif job[0] == 'CUSTOM_TABLE_EXPORT':
        cte_current_array.append(job)

total_length = len(cte_current_array) + len(fl_bu_current_array) + len(sts_mat_current_array) + len(sts_psd_current_array) + len(fl_current_array)

if total_length > 0:
    reporting_array = []
    if len(cte_current_array) > 0:
        print('Usao sam u cte_current_array')
        results = calculate_long_jobs(cte_current_array, cte_success_array)
        reporting_array.extend(results)
    elif len(fl_bu_current_array) > 0:
        print('Usao sam u fl_bu_current_array')
        results = calculate_long_jobs(fl_bu_current_array, fl_bu_success_array)
        reporting_array.extend(results)

    elif len(sts_mat_current_array) > 0:
        print('Usao sam u sts_mat_current_array')
        results = calculate_long_jobs(sts_mat_current_array, sts_mat_success_array)
        reporting_array.extend(results)

    elif len(sts_psd_current_array) > 0:
        print('Usao sam u sts_psd_current_array')
        results = calculate_long_jobs(sts_psd_current_array, sts_psd_success_array)
        reporting_array.extend(results)
        
    elif len(fl_current_array) > 0:
        print('Usao sam u fl_current_array')
        results = calculate_long_jobs(fl_current_array, fl_success_array)
        reporting_array.extend(results)

    print(f'We have the issue with jobs from the list:\n {reporting_array} \n')
    email_body = format_report(reporting_array)

    # Email details
    subject = "Daily Long Running Job Report Issues"
    sender = "svukelic@my_mail.com"
    recipient = "svukelic@my_mail.com"
    smtp_server = "localhost"
    smtp_port = 25

    # Send the email
    send_email(subject, email_body, sender, recipient, smtp_server, smtp_port)
    print("Email sent successfully!")
else:
    print("No issues found, no email sent.")