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
SYNC_DURATION = 13

VM_DIR = "vm"


sync_table_list = [
# "a_end_date_stock",
"a_aop",
"a_attribute",
"a_store",
"a_store_style_inclusions",
"a_store_style_exclusions",
"input_dist_store_style_depth",
"a_planogram",
"a_pricebucket",
"a_style",
"a_sku_attribs",
"input_dist_pullback_config",
"input_noos_cat_qty",
"input_ag",
"input_noos_override",
"a_ag_id",
"input_asp_disc_benchmark",
"input_size_set_properties",
"input_ow_decile_action",
# "a_whstock",
"a_returns",
"a_new_store",
"input_dist_store",
"input_dist_channel_style_override",
"a_warehouse",
"input_size_mapping",
"input_cat_size_seq",
"input_od_override",
"a_sku_parent_map",
"a_style_parent_map",
# "input_dist_git",
# "input_dist_iit",
# "input_dist_open_orders",
"input_dist_wh_store_map",
"input_dist_style_reserve",
"input_dist_jit_style_depth_override",
"input_plano_basestock_override",
"input_otb_str_override",
"input_otb_returns",
"input_otb_min_display",
"input_otb_cat_exit",
"input_otb_inseason_plano",
"input_otb_depth_range",
"input_otb_microbuy_benchmark",
"input_dist_max_sku_depth",
"input_story_cat_combinations",
"input_story_cat_min_options",
"input_story_style_list",
"a_size_set",
"input_bi_retail_week",
"input_otb_moq",
"input_disc_style_override",
"input_disc_sell_through_benchmarks",
"input_disc_store",
"input_disc_store_group_style_discount",
"input_disc_style_grouping",
"input_disc_discount_rules",
"input_disc_guardrails",
"output_discounting",
"input_disc_grn",
"input_wh_inwards",
"input_store_inwards",
# "inv_creation_inwards",
"input_reordering_cover_days_target_spread",
"input_depletion_lead_time",
"a_jit_category",
"input_style_wise_to_size_wise_buy_style_buy",
"input_style_wise_to_size_wise_buy_category_moq",
"a_channel_style_inclusions",
"a_channel_style_exclusions",
# "input_dist_git_whstock",
"input_gap_brand_grading",
"input_reordering_store_group",
"input_reordering_override",
"input_dist_style_pack_size",
"input_style_mrp",
"a_style_season",
"input_dist_wh_planogram",
"input_otb_style_wise_asp_override",
"input_od_asp_override",
"input_od_category_discount_benchmark",
"input_otb_style_wise_str_override",
"input_otb_swb_min_display",
"input_otb_swb_style_segmentation",
"input_otb_style_wise_depth_grading"
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

    client = params.get('client', None)
    project_dir = params.get('project_dir', None)

    python_config = configparser.ConfigParser()
    python_config.read('config.ini')

    config_account_name = python_config["sync_query_props"]["account_name"]
    config_account_key = python_config["sync_query_props"]["account_key"]
    config_container = python_config["sync_query_props"]["container"]
    config_sql_path = python_config["sync_query_props"]["path"]

    # storage_account_name = 'celio'
    # storage_account_key = 'qzmpTLo+4wv5BM3ByXC7RuDwCEsL/RybW5n4IwUogmBvNHzg+53lQ48IeYO7V9iL8gg/cQduHUM7+AStJkjNhg=='

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

    file_prefix = client + '_' + project_dir + '_'
    input_file_prefix = file_prefix + table_name + '_'
    sync_file_prefix = 'sync_' + file_prefix

    initialize_storage_account(storage_account_name, storage_account_key)
    download_file_from_directory(service_client, project_dir, VM_DIR, input_file_prefix, "a_input.tsv")
    download_file_from_directory(service_client, project_dir, VM_DIR, input_file_prefix, "input_periods.tsv")

    config_storage_account = get_storage_account(config_account_name, config_account_key)

    with open(LOCAL_FILE_PATH + input_file_prefix + "a_input.tsv", "r") as input_tsv_file:
        input_df = pd.read_csv(input_tsv_file, delimiter='\t', index_col=False)
        for index, input in input_df.iterrows():
            if input['name'] == "end_date":
                end_date_in_a_input = datetime.datetime.strptime(str(input['value']), DATE_FORMAT).date()
            if input['name'] == "week_start":
                week_start = input['value']
        input_tsv_file.close()


    with open(LOCAL_FILE_PATH + input_file_prefix + "input_periods.tsv", "r") as input_tsv_file:
        input_period_df = pd.read_csv(input_tsv_file, delimiter='\t', index_col=False)
        for index, input in input_period_df.iterrows():
            end_date_string = input['sales_end']

            # convert from string format to datetime format
            end_date = datetime.datetime.strptime(str(end_date_string), DATE_FORMAT).date()
            end_date_list.append(end_date)

        end_date_max = max(end_date_list)

        final_end_date = end_date_in_a_input if input_period_df.shape[0] == 0 else end_date_max
        final_end_date = final_end_date if final_end_date > end_date_in_a_input else end_date_in_a_input

        start_date = final_end_date - dateutil.relativedelta.relativedelta(
            months=SYNC_DURATION) - dateutil.relativedelta.relativedelta(days=1)
        stock_start_date = end_date_in_a_input - dateutil.relativedelta.relativedelta(days=30)

        print(start_date)
        print(final_end_date)
        print(stock_start_date)

        # sales_query = "select 'day', 'store', 'sku', 'disc_value', 'revenue', 'qty';\n" + \
        #               "select day, store, sku, sum(disc_value), sum(revenue), sum(qty) from a_sales where store" + \
        #               " in (select store from a_store_config where enabled = 1) and ( (day between '" + \
        #               str(start_date - dateutil.relativedelta.relativedelta(days=start_date.isoweekday() - int(week_start))) + \
        #               "' and '" + \
        #               str(final_end_date + dateutil.relativedelta.relativedelta(days=8 - final_end_date.isoweekday() - int(week_start))) + \
        #               "')"

        # keyframe_query = "select 'store', 'day', 'sku', 'qty';\n" + \
        #                  "select store, day, sku, qty from a_keyframe where store" + \
        #                  " in (select store from a_store_config where enabled = 1) and " + \
        #                  "( (day between '" + \
        #                  str(start_date - dateutil.relativedelta.relativedelta(days=start_date.isoweekday() - int(week_start))) + \
        #                  "' and '" + \
        #                  str(final_end_date + dateutil.relativedelta.relativedelta(days=8 - final_end_date.isoweekday() - int(week_start))) + \
        #                  "')"
# 
        # for index, input in input_period_df.iterrows():
        #     sales_start = datetime.datetime.strptime(str(input['sales_start']), DATE_FORMAT).date()
        #     sales_end = datetime.datetime.strptime(str(input['sales_end']), DATE_FORMAT).date()
        #     if sales_start > start_date:
        #         continue
        #     if sales_start < start_date and sales_end > start_date:
        #         keyframe_query += " or (day between '" + str(sales_start - dateutil.relativedelta.relativedelta(days=sales_start.isoweekday() - int(week_start))) + \
        #                     "' and '" + str(start_date + dateutil.relativedelta.relativedelta(days=8 - start_date.isoweekday() - int(week_start))) + "')"
        #         sales_query += " or (day between '" + str(sales_start - dateutil.relativedelta.relativedelta(days=sales_start.isoweekday() - int(week_start))) + \
        #                     "' and '" + str(start_date + dateutil.relativedelta.relativedelta(days=8 - start_date.isoweekday() - int(week_start))) + "')"

        #     else:
        #         keyframe_query += " or (day between '" + str(sales_start - dateutil.relativedelta.relativedelta(days=sales_start.isoweekday() - int(week_start))) + \
        #                     "' and '" + str(sales_end + dateutil.relativedelta.relativedelta(days=8 - sales_end.isoweekday() - int(week_start))) + "')"

        #         sales_query += " or (day between '" + str(sales_start - dateutil.relativedelta.relativedelta(days=sales_start.isoweekday() - int(week_start))) + \
        #                     "' and '" + str(sales_end + dateutil.relativedelta.relativedelta(days=8 - sales_end.isoweekday() - int(week_start))) + "')"

        # keyframe_query += ")"
        # sales_query += " ) group by day, store, sku"

        # print("Sales Query")
        # print(sales_query)
        
        print(str(stock_start_date))
        print(str(end_date_in_a_input))

        failed_table_names = []

        if table_name == "a_sales":
            with pyodbc.connect('DRIVER=' + ODBC_DRIVER + ';SERVER=tcp:' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
                with conn.cursor() as cursor:
                    download_using_query(cursor, sales_query, None, "a_sales")
                    upload_file_to_directory(VM_DIR, "a_sales", project_dir)

        elif table_name == "a_keyframe":
            with pyodbc.connect('DRIVER=' + ODBC_DRIVER + ';SERVER=tcp:' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
                with conn.cursor() as cursor:
                    download_using_query(cursor, keyframe_query, None, "a_keyframe")
                    upload_file_to_directory(VM_DIR, "a_keyframe", project_dir)

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
                        sql = sql.replace("${startDate}", str(start_date))
                        sql = sql.replace("${endDate}", str(end_date_in_a_input))
                        sql = sql.replace("${stockStartDate}", str(stock_start_date))

                        download_using_query(cursor, sql, None, table)
                        upload_file_to_directory(VM_DIR, table, project_dir)

                        # cursor.close()
                        cursor.commit()
                    conn.commit()

        input_tsv_file.close()
        # print(failed_table_names)

    return func.HttpResponse(
            "This HTTP triggered function executed successfully.",
            status_code=200
    )