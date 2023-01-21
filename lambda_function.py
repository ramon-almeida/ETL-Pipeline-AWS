import pandas as pd
import boto3, json, logging, re, os
import psycopg2
import psycopg2.extras

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(f'Event structure: {event}')


    source_bucket = 'deman4-group1'
    products_bucket = 'deman4-group1-deployment'

    host='redshiftcluster-gyryx7hwpsmz.cv7hcrmjdnhd.eu-west-1.redshift.amazonaws.com'
    port = '5439'
    user = "group01"
    password = "Redshift-deman4-group01"
    database = "group01_cafe"
    
    DB_DATA = 'postgresql://' + user + ':' + password + '@' + host + ':5439/' \
           + database
    
    conn = psycopg2.connect(
         host = host,
         port = port,
         database = database,
         user = user,
         password = password
    )
    
    cursor = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

    s3 = boto3.client('s3')
    

    ###############################################################
    def create_redshift_db(conn_create, sql: str):
        conn_create.cursor().execute(sql)
        conn_create.commit()
        conn_create.cursor().close()
        conn_create.close()
        
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
    
    
    sql = "SELECT * FROM stores"
    res = redshift_return_rows(cursor, sql)
    print(res)

    # Start ONE TIME ONLY products and stores table ############################ 
    # Start ONE TIME ONLY products and stores table ############################ 
    
    # df_store = pd.DataFrame({'store_id': [1, 2, 3], 'store_name': ['Chesterfield', 'Longridge', 'Uppingham']})
    # for index, row in df_store.iterrows():
    #     print(index, row['store_id'], row['store_name'])
    #     insert = f"""
    #     INSERT INTO stores (store_name) values ('{row['store_name']}')
    #     """
    #     redshift_execute_commit(conn, insert)
    
    # s3.download_file(products_bucket, 'products_menu.csv', '/tmp/products_menu.csv')
    # df_product=pd.DataFrame(pd.read_csv('/tmp/products_menu.csv', names=["product_name", "product_size", "product_price"]))

    # # print(df_product.head())

    # df_product = df_product.assign(product_id = range(1, len(df_product)+1))
    # for index, row in df_product.iterrows():
    #     print(index, type(row['product_id']), type(row['product_name']), type(row['product_size']), type(row['product_price']))
    #     insert = f"""
    #     INSERT INTO products (product_name, product_size, product_price) values ('{row['product_name']}', '{row['product_size']}', {float(row['product_price'])})
    #     """
    #     redshift_execute_commit(conn, insert)
    
    # sql = 'SELECT * FROM products'
    # rows = redshift_return_rows(cursor, sql)
    
    # for row in rows:
    #     print('xx ', row['product_id'], row['product_name'])
    
    # End ONE TIME ONLY products and stores table ############################ 
    # End ONE TIME ONLY products and stores table ############################ 
    
    

    
    read_file_count = 0
    read_file_limit = 3
    
    prefix = ''  # Set this to an empty string to list the entire bucket

    # Set up the list to store the results
    results = []
    
    # Set up the pagination variables
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=source_bucket, Prefix=prefix)
    
    # Iterate through the pages and add the contents to the list
    for page in page_iterator:
        for obj in page['Contents']:
            if obj['Key'].endswith('.csv'):
                results.append(obj)
        
    for i, item in enumerate(results):
        
        if read_file_count >= read_file_limit:
            break
        
        file=item['Key']
        
        if file.endswith('.csv'):
        
            sql = f"""SELECT data_file_name FROM data_archive WHERE data_file_name = '{file}'"""
            rows = redshift_return_rows(cursor, sql)
            
            if len(rows) == 0:
                read_file_count += 1
                
                print('New file, no pervious record ', file)
                
                insert = f"""
                INSERT INTO data_archive (data_file_name) values ('{file}');
                """
                conn.cursor().execute(insert)
                sql = f"""SELECT max(data_id) from data_archive;"""
                data_id = redshift_return_rows(cursor, sql)
                conn.commit()
                print('Inserted data_id in data_archive ', data_id[0]['max'])
                
                res = re.findall(('[^/]+\.csv'), file)
                s3.download_file(source_bucket, file, "/tmp/" + res[0])
                df=pd.DataFrame(pd.read_csv("/tmp/" + res[0], names=["order_time", "store_name", "customer_name", "items", "total_price", "payment_type", "card_number"]))
                
                df = df.drop(columns = ['customer_name', 'card_number'])
                print(df.head())
        
                for index, row in df.iterrows():
                    
                    insert = f"""
                    INSERT INTO orders (order_time, store_name, total_price, payment_type) values (TO_TIMESTAMP('{row['order_time']}', 'DD/MM/YYYY HH24:MI'), '{row['store_name']}', {float(row['total_price'])}, '{row['payment_type']}')
                    """
                    conn.cursor().execute(insert)
                    sql = f"""SELECT max(order_id) from orders;"""
                    order_id = redshift_return_rows(cursor, sql)
                    conn.commit()
                    print(order_id[0]['max'])
                    
                    pd_count = 0
                    lst = row['items'].split(', ')
                    dic_lst = [{'item':x, 'count':lst.count(x)} for x in set(lst)]
                    for dic in dic_lst:
                        pd_count += 1
                        if match := re.findall('^(Regular|Large) (.*) - ([0-9\.]+)$', dic['item']):
                            pid_sql = f"""SELECT product_id FROM products WHERE product_name = '{match[0][1]}' and product_size = '{match[0][0]}'"""
                            pid_rows = redshift_return_rows(cursor, pid_sql)
    
                            sid_sql = f"""SELECT store_id FROM stores WHERE store_name = '{row['store_name']}'"""
                            sid_rows = redshift_return_rows(cursor, sid_sql)
                            
                            insert = """
                            INSERT INTO products_orders (store_id, order_id, product_id, product_price, quantity_sold) values (%s, %s, %s, %s, %s)
                            """
                            tuples = (sid_rows[0]['store_id'], order_id[0]['max'], pid_rows[0]['product_id'], float(match[0][2]), dic['count'])
                            print(tuples)
                            redshift_insert_format(conn, insert, tuples)
                            
                            
    print("")
    print('Number of pandas data input into orders: ', len(df))
    
    sql = f"SELECT MAX(order_id) as max_oid from orders"
    maxs = redshift_return_rows(cursor, sql)
    print('redshift max order_id in orders: ', maxs[0]['max_oid'])
    
    sql = f"SELECT MAX(poid) as max_poid from products_orders"
    maxs = redshift_return_rows(cursor, sql)
    print('redshift max poid in products_orders: ', maxs[0]['max_poid'])
    
    if len(df) > 0:
        print('Congratulation! Data are successfully loaded')
    print("")