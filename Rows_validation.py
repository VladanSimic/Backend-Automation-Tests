import cx_Oracle
import snowflake.connector
import csv
import pandas as pd
import time
from colorama import Fore
from colorama import Style
from colorama import init
from sv_crypt_test import sv_crypt
init()


def compare_data(sf_active_customers):
    #IMPORT VIEWS DATA FROM EXCEL FILE
    start_time = time.time()
    views = open('/home/svukelic/py_scripts/Rows_validation/views_and_types_from_documentation.csv', encoding='utf-8')
    data_from_views = csv.reader(views)
    lines_data_from_views = list(data_from_views)
    #print(lines_data_from_views)

    #SELECT UNIQUE VIEW NAMES FROM EXCEL FILE
    view_name_list = []
    for line in lines_data_from_views:
        #print(line[0])
        if line[0] != '\ufeffview_name' and line[0] != 'view_name' and line[0] != 'SPEND_HIERARCHY':
            view_name_list.append(line[0])
        else: 
            pass
            #print(line)
    unique_list_of_views = set(view_name_list)
    
    for (instance, customer) in sf_active_customers:
        print(f'For instance {instance}')
        print(f'and customer {customer}')
        oracle_username = 'username_' + instance.lower()
        oracle_password = 'password_' + instance.lower()
        decrypted_oracle_username = sv_crypt(oracle_credentials_dict[oracle_username]).decrypt()
        decrypted_oracle_password = sv_crypt(oracle_credentials_dict[oracle_password]).decrypt()
        #print(f'username: {oracle_username}')
        #print(f'password: {oracle_password}')
        dsn_tns = cx_Oracle.makedsn(oracle_credentials_dict['host'], oracle_credentials_dict['port'], oracle_credentials_dict['service_name']) # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
        conn = cx_Oracle.connect(user=decrypted_oracle_username, password=decrypted_oracle_password, dsn=dsn_tns)
    
        rows_and_tables_list = []
        #COUNT ROWS FROM ORACLE FOR ALL VIEWS
        for i,j in enumerate(unique_list_of_views):
            start_time = time.time()
            end_time = time.time()
            elapsed_time = end_time - start_time
        
            if j != 'SPEND_HIERARCHY':
        
                sql = """select '{0}', count(*) from {1}BIZ.{0} where source_id = '{2}'""".format(j, instance, customer)
                query = pd.read_sql_query(sql, conn)
                print(f'Quering data is DONE for table: {j}')
                rows_and_tables_list.append(query)
            else:
                sql = """select '{0}', count(*) from {1}BIZ.{0} where group_id = '{2}'""".format(j, instance, customer)
                query = pd.read_sql_query(sql, conn)
                print(f'Querying data is DONE for table: {j}')
                rows_and_tables_list.append(query)
        #RENAME OPTIMIZED TABLES
        optimized_rows_and_tables = []
        for row in rows_and_tables_list:
            if str(row).split()[3] == 'AUCTION_BIDS_ITEM_': 
                optimized_rows_and_tables.append(str(row).replace('AUCTION_BIDS_ITEM_','AUCTION_BIDS_ITEM'))
            elif str(row).split()[3] == 'RFX_MULTICOLUMN_RESPONSE_':
                optimized_rows_and_tables.append(str(row).replace('RFX_MULTICOLUMN_RESPONSE_','RFX_MULTICOLUMN_RESPONSE'))
            elif str(row).split()[3] == 'RFX_COMM_RESPONSE_':
                optimized_rows_and_tables.append(str(row).replace('RFX_COMM_RESPONSE_','RFX_COMM_RESPONSE'))
            elif str(row).split()[3] == 'AUCTION_PRE_BIDS_ITEM_':
                optimized_rows_and_tables.append(str(row).replace('AUCTION_PRE_BIDS_ITEM_','AUCTION_PRE_BIDS_ITEM'))
            elif str(row).split()[3] == 'SUPPLIER_RISK_ASMT_':
                optimized_rows_and_tables.append(str(row).replace('SUPPLIER_RISK_ASMT_','SUPPLIER_RISK_ASMT'))
            elif str(row).split()[3] == 'SUPPLIER_CATEGORY_ASMT_':
                optimized_rows_and_tables.append(str(row).replace('SUPPLIER_CATEGORY_ASMT_','SUPPLIER_CATEGORY_ASMT'))
            elif str(row).split()[3] == 'SUPPLIER_QUALIFICATION_ASMT_':
                optimized_rows_and_tables.append(str(row).replace('SUPPLIER_QUALIFICATION_ASMT_','SUPPLIER_QUALIFICATION_ASMT'))
            elif str(row).split()[3] == 'SUPPLIER_SEGMENTATION_ASMT_':
                str(row).replace('SUPPLIER_SEGMENTATION_ASMT_','SUPPLIER_SEGMENTATION_ASMT')
                optimized_rows_and_tables.append(str(row).replace('SUPPLIER_SEGMENTATION_ASMT_','SUPPLIER_SEGMENTATION_ASMT'))
            elif str(row).split()[3] == 'RFX_SUPPLIER_BID_ITEM_':
                str(row).replace('RFX_SUPPLIER_BID_ITEM_','RFX_SUPPLIER_BID_ITEM')
                optimized_rows_and_tables.append(str(row).replace('RFX_SUPPLIER_BID_ITEM_','RFX_SUPPLIER_BID_ITEM'))
            elif str(row).split()[3] == 'RFX_TECH_RESPONSE_':
                optimized_rows_and_tables.append(str(row).replace('RFX_TECH_RESPONSE_','RFX_TECH_RESPONSE'))
            elif str(row).split()[3] == 'RFX_QUAL_RESPONSE_':
                optimized_rows_and_tables.append(str(row).replace('RFX_QUAL_RESPONSE_','RFX_QUAL_RESPONSE'))
            else:
                optimized_rows_and_tables.append(row)
        #print(f'Row now have value : {optimized_rows_and_tables}')
        #COUNT ROWS FROM SNOWFLAKE FOR ALL VIEWS
        ctx = snowflake.connector.connect(
                user = decrypted_sf_username,
                password = decrypted_sf_password,
                account = sf_credentials_dict['sf_account']
        )
        cs = ctx.cursor()
        sf_rows = []
        try:
            cs.execute("""select table_name, row_count from JA_ANALYTICS_PREP_DB.information_schema.tables where table_schema = '{0}BIZ{1}'""".format(instance, customer))
            sf_rows = cs.fetchall()
            print(sf_rows)
        finally:
            cs.close()
        ctx.close()
    #COMPARE DATA
        for (a,b) in sf_rows:
            for row in optimized_rows_and_tables:
                if a == str(row).split()[3] and b == int(str(row).split()[4]):
                    print(f'For {a} from SF existing rows are {b}')
                    print(f'For {str(row).split()[3]} from ORACLE existing rows are {int(str(row).split()[4])}')
                    print('All rows are copied succesfully')
                elif a != str(row).split()[3]:
                    pass
                else:
                    print(f'{Fore.RED}For {a} from SF existing rows are {b}')
                    print(f'For {str(row).split()[3]} from ORACLE existing rows are {int(str(row).split()[4])}')
                    print(f'Transfer failed{Style.RESET_ALL}')
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'Script is finished in {elapsed_time} seconds')
    
#IMPORT ORACLE CREDENTIALS FROM .TXT FILE
oracle_credentials_dict = {}
oracle_credentials = open('/home/svukelic/py_scripts/Rows_validation/oracle_connection_param.txt','r')
for line in oracle_credentials:
    key, value = line.split(':')
    oracle_credentials_dict[key] = value.strip()
oracle_credentials.close()

#IMPORT SNOWFLAKE CREDENTIALS FROM .TXT FILE
sf_credentials_dict = {}
sf_credentials = open('/home/svukelic/py_scripts/Rows_validation/sf_connection_params.txt', 'r')
for line in sf_credentials:
    key, value = line.split(':')
    sf_credentials_dict[key]= value.strip()
sf_credentials.close()
decrypted_sf_username = sv_crypt(sf_credentials_dict['sf_username']).decrypt()
decrypted_sf_password = sv_crypt(sf_credentials_dict['sf_password']).decrypt()

print('1. Test all instances and customers')
print('2. Test specific Instance')
print('3. Test specific customer')

sf_active_customers = []
is_true = 1
while is_true == 1:
    
    to_test = input('Please select type of testing from list above (input the number of line): ')
    
    if to_test == '1':
        print('All instances and all customers are selected')
        print('Processing')
        ctx = snowflake.connector.connect(
                user = decrypted_sf_username,
                password = decrypted_sf_password,
                account=sf_credentials_dict['sf_account']
        )
        cs = ctx.cursor()
        try:
            cs.execute("""select JA_INSTANCE_ID, JA_GROUP_ID from J1_PIPELINE_EUUIT_DB.J1_METADATA.J1_CUSTOMER where JA_STATUS = 1""")
            sf_active_customers = cs.fetchall()
        finally:
            cs.close()
        ctx.close()
        is_true == 0
        break
    elif to_test == '2':
        instance_input = input('Please indicate instance for testing: ')
        ctx = snowflake.connector.connect(
                user = decrypted_sf_username,
                password = decrypted_sf_password,
                account=sf_credentials_dict['sf_account']
        )
        cs = ctx.cursor()
        try:   
            cs.execute("""select JA_INSTANCE_ID, JA_GROUP_ID from J1_PIPELINE_EUUIT_DB.J1_METADATA.J1_CUSTOMER where JA_INSTANCE_ID = '{0}' and JA_STATUS = 1""".format(instance_input))
            sf_active_customers = cs.fetchall()
        finally:
            cs.close()
        ctx.close()
        is_true == 0
        break
    elif to_test == '3':
        instance_input = input('Please indicate instance for testing: ')
        customer_input = input('Please indicate customer for testing: ')
        ctx = snowflake.connector.connect(
                user = decrypted_sf_username,
                password = decrypted_sf_password,
                account=sf_credentials_dict['sf_account']
        )
        cs = ctx.cursor()
        try:
            cs.execute("""select JA_INSTANCE_ID, JA_GROUP_ID from J1_PIPELINE_EUUIT_DB.J1_METADATA.J1_CUSTOMER where JA_INSTANCE_ID = '{0}' and JA_GROUP_ID = '{1}' and JA_STATUS = 1""".format(instance_input, customer_input))
            sf_active_customers = cs.fetchall()
        finally:
            cs.close()
        ctx.close()
        is_true == 0
        break
    else:
        print('Your input is not correct, please input values 1,2 or 3')
        is_true = 1
#print(sf_active_customers)  
compare_data(sf_active_customers)


