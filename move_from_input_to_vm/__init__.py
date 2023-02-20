import logging

import azure.functions as func
import pyodbc
import csv
import pandas as pd
import datetime
import dateutil.relativedelta
import configparser

from azure.storage.filedatalake import DataLakeServiceClient

# Constants
LOCAL_FILE_PATH = '/tmp/'
FILE_EXTENSION = '.tsv'
ODBC_DRIVER= '{ODBC Driver 17 for SQL Server}'

DATE_FORMAT = '%Y-%m-%d'
DATE_FORMAT_MONTH = '%Y-%m'
SYNC_DURATION = 13

VM_DIR = "vm"
    
sync_table_list =[
    # "aop",
    # "channel",
    # "storestock",
    # "input",
    # "planogram",
    # "style",
     "sales",
    "ru_sales",
    "partner_pin_wh_map",
    "sku",
    "parent_child_sku",
    "new_store_mapping",
    "whstock",
    "ru_region_whstock",
    "ru_region_whcapacity",
    "ru_mother_whstock",
    "ru_wh_priority",
    "group",
    "cat_size_sets",
    "cat_style_size_qty",
    "key_size_override",
    "decision_matrix",
    "guard_rails",
    "style_discount",
    "oa_whstock",
    "dc_group",
    "dc_cat_size_sets",
    "dc_style",
    "dc_sku",
    "online_analytics"
]



def initialize_synapse_db_connection(server, database, username, password):
    cnxn = pyodbc.connect('DRIVER=' + ODBC_DRIVER + ';SERVER=' +
                      server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    return cnxn

def get_storage_account(storage_account_name, storage_account_key):
    try:
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
        return service_client
    except Exception as e:
        print(e)

def initialize_storage_account(storage_account_name, storage_account_key):
    try:
        global service_client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    except Exception as e:
        print(e)


def upload_file_to_directory(vm_directory, file_name, project_dir):
    try:
        file_system_client = service_client.get_file_system_client(file_system=project_dir)

        directory_client = file_system_client.get_directory_client(vm_directory)
        file_client = directory_client.create_file(file_name + FILE_EXTENSION)

        local_file = open(LOCAL_FILE_PATH + sync_file_prefix + file_name + FILE_EXTENSION, 'r')

        file_contents = local_file.read()
        # file_contents = local_file.read()

        file_client.upload_data(file_contents, overwrite=True)

        # file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        # file_client.flush_data(len(file_contents))

        local_file.close()
        file_client.close()
        print("File " + file_name + " upload complete")

    except Exception as e:
        print(e)

def download_using_query(cursor, query, params, file_name):
    split_query = query.split(';')

    fp = open(LOCAL_FILE_PATH + sync_file_prefix + file_name + FILE_EXTENSION, 'w')

    # done to remove error https://stackoverflow.com/questions/7753830/mssql2008-pyodbc-previous-sql-was-not-a-query
    cursor.execute("SET NOCOUNT ON;")
    cursor.commit()

    for split_query_part in split_query:
        split_query_part = split_query_part.strip()
        if len(split_query_part) <= 1:
            continue
        
        split_query_part = split_query_part.replace("\n", " ")
        cursor.execute(split_query_part)
        rows = cursor.fetchall()
        file = csv.writer(fp, delimiter='\t')
        file.writerows(rows)
    fp.close()

def download_file_from_directory(service_client, project_dir, sub_directory, file_prefix, file_name):
    file_system_client = service_client.get_file_system_client(file_system=project_dir)
    directory_client = file_system_client.get_directory_client(sub_directory)
    file_client = directory_client.get_file_client(file_name)
    download = file_client.download_file()
    downloaded_bytes = download.readall()

    try:
        local_file = open(LOCAL_FILE_PATH + file_prefix + file_name, 'wb')
        local_file.write(downloaded_bytes)
        local_file.close()
    except Exception as e:
        print("Writing Data inside File Failed")
        print(e)

    print("File " + file_name + " download complete")

def get_files_from_directory_as_stream(service_client, project_dir, sub_directory, file_name):
    try:
        file_system_client = service_client.get_file_system_client(file_system=project_dir)
        directory_client = file_system_client.get_directory_client(sub_directory)
        file_client = directory_client.get_file_client(file_name)
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        file_client.close()
        return downloaded_bytes

    except Exception as e:
        print(file_name)
        print(e)
        raise e


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    params = req.params

    try:
        req_body = dict(req.get_json())
    except:
        logging.warning('Request body cannot be converted to dict. Unsupported object passed')
    else:
        query_params = set(params.keys())
        body_params = set(req_body.keys())
        intersection = query_params.intersection(body_params)
        if len(intersection) > 0:
            return func.HttpResponse('Duplicate parameters passed in query params and request body', status_code=400)

        params = {**params, **req_body}

    project_dir = params.get('project_dir', None)

    python_config = configparser.ConfigParser()
    python_config.read('config.ini')

  
    config_account_name = python_config["sync_query_props"]["account_name"]
    config_account_key = python_config["sync_query_props"]["account_key"]
    config_container = python_config["sync_query_props"]["container"]
    config_sql_path = python_config["sync_query_props"]["path"]

   
    storage_account_name = params.get('storage_account_name', None)
    storage_account_key = params.get('storage_account_key', None)

    server = params.get('server', None)
    database = params.get('database', None)
    table_name = params.get('table_name', "")


    username = python_config["client_serverless_db_creds"]["username"]
    password = python_config["client_serverless_db_creds"]["password"]

    # --- #
    
    end_date_list = []

    end_date_in_a_input = datetime.date.today()
    week_start = 1

    global sync_file_prefix

    file_prefix =  project_dir + '_'
    input_file_prefix = file_prefix + table_name + '_'
    sync_file_prefix = 'sync_' + file_prefix

    initialize_storage_account(storage_account_name, storage_account_key)
    
    print("Storage account" + storage_account_name)
    download_file_from_directory(service_client, project_dir, VM_DIR, input_file_prefix, "input.tsv")
    

    config_storage_account = get_storage_account(config_account_name, config_account_key)

    inputMap = dict()

    with open(LOCAL_FILE_PATH + input_file_prefix + "input.tsv", "r") as input_tsv_file:
        input_df = pd.read_csv(input_tsv_file, delimiter='\t', index_col=False)
        for index, input in input_df.iterrows():
            if input['name'] == "startDate":
                inputMap['startDate'] = datetime.datetime.strptime(str(input['value']), DATE_FORMAT_MONTH).date()
            if input['name'] == "endDate":
                inputMap['endDate'] = datetime.datetime.strptime(str(input['value']), DATE_FORMAT_MONTH).date()
            if input['name'] == "ru_analysis_last_date":
                inputMap['ru_analysis_last_date'] = datetime.datetime.strptime(str(input['value']), DATE_FORMAT).date()
            if input['name'] == "ru_days":
                inputMap['ru_days'] = input['value']
            if input['name'] == "distribution_startDate":
                inputMap['distribution_startDate'] = datetime.datetime.strptime(str(input['value']), DATE_FORMAT_MONTH).date()
            if input['name'] == "distribution_endDate":
                inputMap['distribution_endDate'] = datetime.datetime.strptime(str(input['value']), DATE_FORMAT_MONTH).date()
        
            if input['name'] == "ist_start_date":
                inputMap['ist_start_date'] = datetime.datetime.strptime(str(input['value']), DATE_FORMAT_MONTH).date()
            if input['name'] == "ist_end_date":
                inputMap['ist_end_date'] = datetime.datetime.strptime(str(input['value']), DATE_FORMAT_MONTH).date()
            if input['name'] == "repl_sales_days":
                inputMap['repl_sales_days'] = input['value']
            # if input['name'] == "dm_start_date":
            #     inputMap['dm_start_date'] == datetime.datetime.strptime(str(input['value']), DATE_FORMAT).date()
            # if input['name'] == "dm_end_date":
            #     inputMap['dm_end_date'] == datetime.datetime.strptime(str(input['value']), DATE_FORMAT).date()


            
            # if input['name'] == "week_start":
            #     week_start = input['value']

            
        input_tsv_file.close()

    final_end_date = end_date_in_a_input

    start_date = final_end_date - dateutil.relativedelta.relativedelta(
        months=SYNC_DURATION) - dateutil.relativedelta.relativedelta(days=1)
    stock_start_date = end_date_in_a_input - dateutil.relativedelta.relativedelta(days=30)

    print(start_date)
    print(stock_start_date)

    sales_query = "select 'day', 'store', 'sku', 'disc_value', 'revenue', 'qty';\n" + \
                  "select day, store, sku, sum(disc_value), sum(revenue), sum(qty) from sales where store" + \
                  " in (select store from a_store_config where enabled = 1) and ( (day between '" + \
                  str(start_date - dateutil.relativedelta.relativedelta(days=start_date.isoweekday() - int(week_start))) + \
                  "' and '" + \
                  str(final_end_date + dateutil.relativedelta.relativedelta(days=8 - final_end_date.isoweekday() - int(week_start))) + \
                  "')"

  

    sales_query += " ) group by day, store, sku"

    print("Sales Query")
    print(sales_query)
    
    print(str(stock_start_date))
    print(str(end_date_in_a_input))

    failed_table_names = []

    if table_name == "sales":
        with pyodbc.connect('DRIVER=' + ODBC_DRIVER + ';SERVER=tcp:' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
            with conn.cursor() as cursor:
               
                download_using_query(cursor, sales_query, None, "sales")
                upload_file_to_directory(VM_DIR, "sales", project_dir)

    elif table_name != "":
        with pyodbc.connect('DRIVER=' + ODBC_DRIVER + ';SERVER=tcp:' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
            with conn.cursor() as cursor:
                sql = get_files_from_directory_as_stream(config_storage_account, config_container,
                                config_sql_path, table_name + ".sql").decode()
                sql = sql.replace("\n", " ")
                sql = sql.replace("${startDate}", str(start_date))
                sql = sql.replace("${endDate}", str(end_date_in_a_input))
                sql = sql.replace("${stockStartDate}", str(stock_start_date))

                download_using_query(cursor, sql, None, table_name)
                upload_file_to_directory(VM_DIR, table_name, project_dir)

                # cursor.close()
                cursor.commit()
            conn.commit()
            
    else:
        for table in sync_table_list:
            with pyodbc.connect('DRIVER=' + ODBC_DRIVER + ';SERVER=tcp:' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
                with conn.cursor() as cursor:
                    sql = get_files_from_directory_as_stream(config_storage_account, config_container,
                                    config_sql_path, table + ".sql").decode()
                    sql = sql.replace("\n", " ")
                    sql = sql.replace("${startDate}", str(inputMap['startDate']))
                    sql = sql.replace("${endDate}", str(inputMap['endDate']))
            
                    sql = sql.replace("${ru_analysis_last_date}", str(inputMap['ru_analysis_last_date']))
                    sql = sql.replace("${ru_days}", str(inputMap['ru_days']))
                    sql = sql.replace("${distribution_startDate}", str(inputMap['distribution_startDate']))
                    sql = sql.replace("${distribution_endDate}", str(inputMap['distribution_endDate']))
                    sql = sql.replace("${ist_start_date}", str(inputMap['ist_start_date']))
                    sql = sql.replace("${ist_end_date}", str(inputMap['ist_end_date']))
                    # sql = sql.replace("${dm_start_date}", str(inputMap['dm_start_date']))
                    # sql = sql.replace("${dm_end_date}", str(inputMap['dm_end_date']))
                    sql = sql.replace("${repl_sales_days}", str(inputMap['repl_sales_days']))
                    download_using_query(cursor, sql, None, table)
                    upload_file_to_directory(VM_DIR, table, project_dir)

                    # cursor.close()
                    cursor.commit()
                conn.commit()

    return func.HttpResponse(
            "This HTTP triggered function executed successfully.",
            status_code=200
    )