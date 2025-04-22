import cx_Oracle
import snowflake.connector
import csv
import pandas as pd
import time
from colorama import Fore
from colorama import Style
from colorama import init
from sv_crypt import sv_crypt
init()

start_time = time.time()
customer = '01000'
instance = 'BA212A'

#IMPORT ORACLE CREDENTIALS FROM .TXT FILE
oracle_credentials_dict = {}
oracle_credentials = open('oracle_connection_param.txt','r')
for line in oracle_credentials:
    key, value = line.split(':')
    oracle_credentials_dict[key] = value.strip()
oracle_credentials.close()

#IMPORT SNOWFLAKE CREDENTIALS FROM .TXT FILE
sf_credentials_dict = {}
sf_credentials = open('sf_connection_params.txt', 'r')
for line in sf_credentials:
    key, value = line.split(':')
    sf_credentials_dict[key]= value.strip()
sf_credentials.close()
decrypted_sf_username = sv_crypt(sf_credentials_dict['sf_username']).decrypt()
decrypted_sf_password = sv_crypt(sf_credentials_dict['sf_password']).decrypt()

#IMPORT VIEWS DATA FROM EXCEL FILE

views = open('JA_PSD_Views_doc.csv', encoding='utf-8', errors = 'ignore')
data_from_views = csv.reader(views)
lines_data_from_views = list(data_from_views)

#POPULATE EMPTY FIELDS AND FILTER SPECIFIED COLUMNS
index = 0
first_separation = []
filled_with_view =[]
for i, j in enumerate(lines_data_from_views):

    if j[0] != '' and j[0] != 'View  Name' and j[0] != 'JAGGAERAdvantage 21.2 PSD Views (v1.1)' and j[0] != 'IDX':
        index = i
        break
    else:
        pass

for i, j in enumerate(lines_data_from_views):
    if i < index:
        pass
    else:
        if j[0] == '' or j[0] == 'IDX':
            j[0] = lines_data_from_views[index][0]
            
            if j[2] != '' and j[2] != 'Column Name':
                separate_string = j[3].split(' ')
                temp_array = [j[0], j[2], separate_string[0]]
                first_separation.append(temp_array)                    
            else:
                pass
        else:
            index = i
for i in first_separation:
    separate_string = i[2].split('(')
    temp_array = [i[0], i[1], separate_string[0]]
    filled_with_view.append(temp_array)
    
#TAKE STRUCTURE DATA FROM SF      
ctx = snowflake.connector.connect(
                user = decrypted_sf_username,
                password = decrypted_sf_password,
                account = sf_credentials_dict['sf_account']
        )
cs = ctx.cursor()
sf_rows = []
try:
    cs.execute("""select table_name as SF_Table_Name, column_name as SF_Column_Name, data_type as SF_Data_Type_Name from JA_ANALYTICS_DEV_DB.INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '{0}BIZ{1}'""".format(instance, customer))
    sf_rows = cs.fetchall()
    #print(sf_rows)
finally:
    cs.close()
ctx.close()

#TAKE STRUCTURE DATA FROM ORACLE
oracle_username = 'username_' + instance.lower()
oracle_password = 'password_' + instance.lower()
decrypted_oracle_username = sv_crypt(oracle_credentials_dict[oracle_username]).decrypt()
decrypted_oracle_password = sv_crypt(oracle_credentials_dict[oracle_password]).decrypt()
#print(f'username: {oracle_username}')
#print(f'password: {oracle_password}')
dsn_tns = cx_Oracle.makedsn(oracle_credentials_dict['host'], oracle_credentials_dict['port'], oracle_credentials_dict['service_name']) # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=decrypted_oracle_username, password=decrypted_oracle_password, dsn=dsn_tns)

sql = """SELECT table_name as ORACLE_Table_Name, column_name as ORACLE_Column_Name, data_type as ORACLE_Data_Type_Name FROM ALL_TAB_COLUMNS WHERE table_name NOT LIKE ('BV_%') AND table_name NOT LIKE ('V_%') AND table_name NOT LIKE ('MV_%')""".format(j, instance, customer)
query = pd.read_sql_query(sql, conn)
query_array = query.to_numpy()

for i  in filled_with_view:
    if i[2] == 'NVARCHAR2' or i[2] == 'VARCHAR2' or i[2] == 'VARCHAR' or i[2] == 'NVARCHAR' or i[2] == 'NCHAR' or i[2] == 'CHAR' :
        i[2] = 'TEXT'
    elif i[2] == 'DATE':
        i[2] = 'DATETIME'
    elif i[2] == 'TIMESTAMP WITH TIME ZONE':
        i[2] = 'TIMESTAMP_TZ'
    elif i[2] == 'TIMESTAMP':
        i[2] = 'TIMESTAMP_TZ'
    elif i[2] == 'XMLTYPE':
        i[2] = 'VARIANT'
    elif i[2] == 'BINARY_DOUBLE':
        i[2] = 'NUMBER'
    elif i[2] == 'CLOB':
        i[2] = 'TEXT(16777216)'
    else:
        pass

for i  in query_array:
    if i[2] == 'NVARCHAR2' or i[2] == 'VARCHAR2' or i[2] == 'VARCHAR' or i[2] == 'NVARCHAR' or i[2] == 'NCHAR' or i[2] == 'CHAR' :
        i[2] = 'TEXT'
    elif i[2] == 'DATE':
        i[2] = 'DATETIME'
    elif i[2] == 'TIMESTAMP(6) WITH TIME ZONE':
        i[2] = 'TIMESTAMP_TZ'
    elif i[2] == 'TIMESTAMP':
        i[2] = 'TIMESTAMP_TZ'
    elif i[2] == 'XMLTYPE':
        i[2] = 'VARIANT'
    elif i[2] == 'BINARY_DOUBLE':
        i[2] = 'NUMBER'
    elif i[2] == 'CLOB':
        i[2] = 'TEXT(16777216)'
    else:
        pass

good_structure_array = []
bad_structure_array = []
for i in filled_with_view:
    for j in query_array:
        if i[0] == j[0] and i[1] == j[1] and i[2] == j[2]:
            temp_array = [i[0], i[1], i[2], j[2], 1]
            good_structure_array.append(temp_array)
        elif i[0] == j[0] and i[1] == j[1] and i[2] != j[2]:
            temp_array = [i[0], i[1], i[2], j[2], 0]
            good_structure_array.append(temp_array)
            bad_structure_array.append(temp_array)
        elif i[0] == j[0] and i[1] != j[1] and i[2] != j[2]:
            temp_array = [i[0], i[1], i[2], j[2], 0]
            good_structure_array.append(temp_array)
            pass
        elif i[0] != j[0] and i[1] == j[1] and i[2] != j[2]:
            temp_array = [i[0], i[1], i[2], j[2], 0]
            good_structure_array.append(temp_array)
            pass
        else:
            pass
        
#TAKE STRUCTURE DATA FROM SNOWFLAKE
ctx = snowflake.connector.connect(
                user = decrypted_sf_username,
                password = decrypted_sf_password,
                account = sf_credentials_dict['sf_account']
        )
cs = ctx.cursor()
sf_rows = []
try:
    cs.execute("""select table_name as SF_Table_Name, column_name as SF_Column_Name, data_type as SF_Data_Type_Name
from JA_ANALYTICS_DEV_DB.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = '{0}BIZ{1}'""".format(instance, customer))
    sf_rows = cs.fetchall()
finally:
    cs.close()
ctx.close()

complete_array = []
for (a,b,c) in sf_rows:
    for i in (good_structure_array):
        if a == i[0] and b == i[1] and c == i[2] and i[4] == 1:
            complete_array.append(i)
        else:
            pass

print('/////////Bad data structure/////////')
for i,j in enumerate(bad_structure_array):
    print(i)
    print('********************************')
    print(f'Table: {j[0]} || Column: {j[1]} || Requirement file: {j[2]} || ORACLE: {j[3]}')
    print('********************************')
            
end_time = time.time()
print(end_time - start_time)

