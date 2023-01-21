import pandas as pd
import boto3, json, logging, re, os
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta

resources_bucket = os.environ['ResourcesBucket'] 
trigger_bucket = os.environ['SecondTriggerBucket'] 

host= os.environ['DbHost'] 
port = os.environ['DbPort'] 
user = os.environ['DbUser'] 
password = os.environ['DbPassword'] 
database = os.environ['DbDatabase'] 
tb_prefix = os.environ['DbTablePrefix']

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(f'Event structure: {event}')
    
    # START functions sections required to run file ###########################
    # START functions sections required to run file ###########################
    def redshift_create_table(create_query, conn):
        cursor = conn.cursor()
        cursor.execute(create_query)
        conn.commit()
        cursor.close()

    def redshift_execute_commit(conn, sql: str):
        conn.cursor().execute(sql)
        conn.commit()
    
    def redshift_insert_format(conn, sql: str, tuples: tuple):
        conn.cursor().execute(sql, tuples)
        conn.commit()
        
    def redshift_return_rows(cursor, sql: str):
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows
    
    
    def dateTimeToUnix(dateTime = None, format = '%Y-%m-%d %H:%M:%S') -> int:
        """ 2022-11-19 04:19:13 -> 1668831553 """
        if type(dateTime) is datetime and dateTime >= datetime(1970, 1, 1):
            unixtime = int(datetime.timestamp(dateTime))
        elif type(dateTime) is str and re.match('^((\d{4})-(\d{2})-(\d{2}))$ || ^((\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}))$', dateTime) and datetime.strptime(dateTime, format) >= datetime(1970, 1, 1):
            dateTime = datetime.strptime(dateTime, format) # string to datetime object
            unixtime = int(datetime.timestamp(dateTime))
        elif dateTime is None:
            dateTimeNow = datetime.now().strftime(format)
            dateTimeNow = datetime.strptime(dateTimeNow, format) # string to datetime object
            unixtime = int(datetime.timestamp(dateTimeNow))
        else:
            unixtime = 0
        return unixtime # return integer

    # END functions sections required to run file ###########################
    # END functions sections required to run file ###########################
    
    s3 = boto3.client('s3')
    
    conn = psycopg2.connect(
         host = host,
         port = port,
         database = database,
         user = user,
         password = password
    )
    
    cursor = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    
    
    tb_name = {
        "orders": tb_prefix + "orders",
        "stores": tb_prefix + "stores",
        "products": tb_prefix + "products",
        "products_orders": tb_prefix + "products_orders",
        "data_archive": tb_prefix + "data_archive",
    }
    
    table_dict = {
        tb_name["orders"]:f"""
            CREATE TABLE IF NOT EXISTS {tb_name["orders"]} (
            order_id INT IDENTITY(1,1) not null,
            order_time timestamp,
            store_name varchar(45),
            total_price decimal(8,2),
            payment_type text
        );
        """,
        tb_name["stores"]:f"""
            CREATE TABLE IF NOT EXISTS {tb_name["stores"]} (
                store_id INT IDENTITY(1,1) not null,
                store_name varchar(45)
            );
        """,
        tb_name["products"]:f"""
            CREATE TABLE IF NOT EXISTS {tb_name["products"]} (
                product_id INT IDENTITY(1,1) not null,
                product_name varchar(45),
                product_size varchar(45),
                product_price decimal(8,2)
            );
        """,
        tb_name["products_orders"]:f"""
            CREATE TABLE IF NOT EXISTS {tb_name["products_orders"]} (
                poid INT IDENTITY(1,1) not null,
                store_id integer not null,
                order_id integer not null,
                product_id integer not null,
                product_price decimal(8,2),
                quantity_sold integer not null
            );
        """,
        tb_name["data_archive"]:f"""
            CREATE TABLE IF NOT EXISTS {tb_name["data_archive"]} (
                data_id INT IDENTITY(1,1) not null,
                data_file_name varchar(255),
                input_time timestamp
            );
        """    
    }
    
    # Create tables if not existed ##################################
    for table_name, create_sql in table_dict.items():
        redshift_create_table(create_sql, conn)

        
    # START only insert data only once, only if those tables are empty #############
    # START only insert data only once, only if those tables are empty #############
    sql = f"""SELECT COUNT(*) as count FROM {tb_name["stores"]}"""
    cursor.execute(sql)
    cnt = cursor.fetchone()
    print(f'xxxx {tb_name["stores"]} count xxxx ', cnt["count"])
    if cnt["count"] < 1:
        df_store = pd.DataFrame({'store_id': [1, 2, 3], 'store_name': ['Chesterfield', 'Longridge', 'Uppingham']})
        for index, row in df_store.iterrows():
            print(index, row['store_id'], row['store_name'])
            insert = f"""
            INSERT INTO {tb_name["stores"]} (store_name) values ('{row['store_name']}')
            """
            cursor.execute(insert)
            conn.commit()

    sql = f"""SELECT COUNT(*) as count FROM {tb_name["products"]}"""
    cursor.execute(sql)
    cnt = cursor.fetchone()
    print(f'xxxx {tb_name["products"]} count xxxx ', cnt["count"])
    if cnt["count"] < 1:
        s3.download_file(resources_bucket, 'products_menu.csv', '/tmp/products_menu.csv')
        df_product=pd.DataFrame(pd.read_csv('/tmp/products_menu.csv', names=["product_name", "product_size", "product_price"]))
        print(f'xxxxxxxx start print -> df_product.head() xxxxxxxx')
        print(df_product.head())    
        print(f'xxxxxxxx End print -> df_product.head() xxxxxxxx')
        for index, row in df_product.iterrows():
            insert = f"""
            INSERT INTO {tb_name["products"]} (product_name, product_size, product_price) values ('{row['product_name']}', '{row['product_size']}', {float(row['product_price'])})
            """
            cursor.execute(insert)
            conn.commit() 
    # END only insert data only once, only if those tables are empty #############
    # END only insert data only once, only if those tables are empty #############
     
    prefix = ''  # Set this to an empty string to list the entire bucket

    # Set up the list to store the results
    results = []
    
    # Set up the pagination variables
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=trigger_bucket, Prefix=prefix)
    
    # Iterate through the pages and add the contents to the list
    for page in page_iterator:
        for obj in page['Contents']:
            if obj['Key'].endswith('.csv'):
                results.append(obj['Key'])
                
    read_file_count = 0
    read_file_limit = 3 # The AWS trigger invocation seems to set to takes 3 time. That is this script will run 3 times. 
    
    results.sort(key=lambda x: datetime.strptime(x.split('_')[1], '%d-%m-%Y'))    
    
    # [print(item) for item in results]
    
    ### START checking out time gap to prevent concurrent invocation ###############
    sql = f"""SELECT MAX(data_id) FROM {tb_name['data_archive']} """
    max_data_id = redshift_return_rows(cursor, sql)
    print(max_data_id)
    print(max_data_id[0]['max'])
    
    codepass = True
    if max_data_id[0]['max'] is not None:
        sql = f"""SELECT input_time FROM {tb_name['data_archive']} WHERE data_id = {max_data_id[0]['max']}"""
        rows_2 = redshift_return_rows(cursor, sql)

        time_gap = dateTimeToUnix() - dateTimeToUnix(rows_2[0]['input_time'])
        
        if time_gap < 900:
            print(f'Time gap in seconds [ {time_gap} ] is too small, not allow to re-access trigger bucke. It is to prevent recursive invocation of lambda function to make itself time out; which leads to import data to redshift will be incorrect')
            codepass = False       
    ### END checking out time gap to prevent concurrent invocation ###############
    
    for file in results:
        
        if read_file_count >= read_file_limit: # set for loop limit to prevent lambda function time out
            break
        
        res = re.findall(('[^/]+\.csv'), file)
        csvfname = res[0]
        

        if file.endswith('.csv'):
        
            sql = f"""SELECT data_file_name FROM {tb_name['data_archive']} WHERE data_file_name = '{csvfname}'"""
            rows = redshift_return_rows(cursor, sql)
            # print(rows)
                        
            if len(rows) == 0 and codepass:
                read_file_count += 1
                
                print(read_file_count, ' [New csv file name, no pervious record] ', csvfname)
                
                insert = f"""
                INSERT INTO {tb_name['data_archive']} (data_file_name, input_time) values ('{csvfname}', CURRENT_TIMESTAMP);
                """
                redshift_execute_commit(conn, insert)

                
                s3.download_file(trigger_bucket, file, "/tmp/" + csvfname)
                df_order=pd.DataFrame(pd.read_csv("/tmp/" + csvfname, names=["order_time", "store_name", "items", "total_price", "payment_type"]))
                print(f'xxxxxxxx start print -> df_order.head() xxxxxxxx')
                print(df_order.head())
                print(f'xxxxxxxx start print -> df_order.head() xxxxxxxx')
                for index, row in df_order.iterrows():
                    
                    insert = f"""
                    INSERT INTO {tb_name['orders']} (order_time, store_name, total_price, payment_type) values ('{row['order_time']}', '{row['store_name']}', {float(row['total_price'])}, '{row['payment_type']}')
                    """
                    conn.cursor().execute(insert)
                    sql = f"""SELECT max(order_id) from {tb_name['orders']};"""
                    order_id = redshift_return_rows(cursor, sql)
                    conn.commit()
            
                    print(order_id[0]['max'])
                    
                    pd_count = 0
                    lst = row['items'].split(', ')
                    dic_lst = [{'item':x, 'count':lst.count(x)} for x in set(lst)]
                    for dic in dic_lst:
                        pd_count += 1
                        if match := re.findall('^(Regular|Large) (.*) - ([0-9\.]+)$', dic['item']):
                            pid_sql = f"""SELECT product_id FROM {tb_name['products']} WHERE product_name = '{match[0][1]}' and product_size = '{match[0][0]}'"""
                            pid_rows = redshift_return_rows(cursor, pid_sql)
    
                            sid_sql = f"""SELECT store_id FROM {tb_name['stores']} WHERE store_name = '{row['store_name']}'"""
                            sid_rows = redshift_return_rows(cursor, sid_sql)
                            
                            insert = f"""
                            INSERT INTO {tb_name['products_orders']} (store_id, order_id, product_id, product_price, quantity_sold) values (%s, %s, %s, %s, %s)
                            """
                            tuples = (sid_rows[0]['store_id'], order_id[0]['max'], pid_rows[0]['product_id'], float(match[0][2]), dic['count'])
                            # print(tuples)
                            redshift_insert_format(conn, insert, tuples)
                            
                            
    print("")
 
    sql = f"SELECT MAX(order_id) as max_oid from {tb_name['orders']}"
    maxs = redshift_return_rows(cursor, sql)
    print(f"redshift max order_id in {tb_name['orders']}: ", maxs[0]['max_oid'])
    
    sql = f"SELECT MAX(poid) as max_poid from {tb_name['products_orders']}"
    maxs = redshift_return_rows(cursor, sql)
    print(f"redshift max poid in {tb_name['products_orders']}: ", maxs[0]['max_poid'])
    
    print(f"Congratulation! Data are successfully loaded")
    print("")