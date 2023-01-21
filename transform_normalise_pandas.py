from sqlalchemy import create_engine, Table, Column, DateTime, Float, Integer, String, Text, MetaData
# from sqlalchemy import create_engine
from sqlalchemy.types import *
import pandas as pd
import pymysql, re, os
from sqlalchemy_utils import database_exists, create_database, drop_database

orders_data = 'data.csv'

products_menu = 'products_menu.csv'

host = "localhost"
user = "root"
password = "password"
database = "cup_of_joy"

DB_DATA = 'mysql+pymysql://' + user + ':' + password + '@' + host + ':3306/' \
       + database + '?charset=utf8mb4'

###############################################################

def create_mysql_db(conn_create, sql: str):
    conn_create.cursor().execute(sql)
    conn_create.commit()
    conn_create.cursor().close()
    conn_create.close()
    
def mysql_execute_commit(conn, sql: str):
    conn.cursor().execute(sql)
    conn.commit()

def mysql_insert_format(conn, sql: str, tuples: tuple):
    conn.cursor().execute(sql, tuples)
    conn.commit()
    
def mysql_return_rows(cursor, sql: str):
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows

# sqlalchemy
engine = create_engine(DB_DATA)

if database_exists(engine.url):
    pass
else:
    create_database(engine.url)


# pymysql
conn = pymysql.connect(
host = host,
user = user,
password = password,
database = database,
cursorclass = pymysql.cursors.DictCursor
)
cursor = conn.cursor()

meta = MetaData()

orders = Table(
   'orders', meta, 
   Column('order_id', Integer, primary_key = True), 
   Column('order_time', DateTime), 
   Column('store_name', String(45)),
#    Column('items', Text),
   Column('total_price', Text),
   Column('payment_type', Text),
)

stores = Table(
   'stores', meta, 
   Column('store_id', Integer, primary_key = True), 
   Column('store_name', String(45)), 
)

products = Table(
   'products', meta, 
   Column('product_id', Integer, primary_key = True), 
   Column('product_name', String(45)), 
   Column('product_size', String(45)),
   Column('product_price', Float),
)

products_orders = Table(
   'products_orders', meta, 
   Column('poid', Integer, primary_key = True), 
   Column('store_id', Integer),
   Column('order_id', Integer),
   Column('product_id', Integer),
   Column('product_price', Float),
   Column('quantity_sold', Integer),
#    Column('sub_total', Float),
#    Column('order_time', DateTime), 
)

meta.drop_all(engine) # drop table if exist
meta.create_all(engine)

df_store = pd.DataFrame({'store_id': [1, 2, 3], 'store_name': ['Chesterfield', 'Longridge', 'Uppingham']})
df_store.set_index('store_id', inplace=True)
df_sotre_schema = {
    'store_id': Integer, 
    'store_name':  String(45),
}
df_store.to_sql('stores', engine, index=True, if_exists='replace', dtype=df_sotre_schema)


df_product=pd.DataFrame(pd.read_csv(products_menu, names=["product_name", "product_size", "product_price"]))
df_product = df_product.assign(product_id = range(1, len(df_product)+1))
df_product.set_index('product_id', inplace=True)
df_product_schema = {
    # 'product_id': 'INTEGER PRIMARY KEY AUTOINCREMENT', 
    'product_id': Integer,
    'product_name':  String(45),
    'product_size':  String(45),
    'product_price': Float,
}
df_product.to_sql('products', engine, index=True, if_exists='replace', dtype=df_product_schema)

#####################################################################################################

df=pd.DataFrame(pd.read_csv(orders_data, names=["order_time", "store_name", "customer_name", "items", "total_price", "payment_type", "card_number"]))
df['order_time'] = pd.to_datetime(df['order_time'], format = '%d/%m/%Y %H:%M')

df = df.drop(columns = ['customer_name', 'card_number'])

sql = f"SELECT MAX(order_id) as max_oid from orders"
maxs = mysql_return_rows(cursor, sql)
# print(maxs)

if maxs[0]['max_oid'] == None:
    start_index = 1
else:
    start_index = int(maxs[0]['max_oid']) + 1

end_index = start_index + len(df)

df = df.assign(order_id = range(start_index, end_index))

for index, row in df.iterrows():
    insert = """
    INSERT INTO orders (order_id, order_time, store_name, total_price, payment_type) values (%s, %s, %s, %s, %s)
    """
    tuples = (int(row['order_id']), row['order_time'], row['store_name'], float(row['total_price']), row['payment_type'])
    mysql_insert_format(conn, insert, tuples)

# sql =f"SELECT COUNT(*) as totals FROM `orders` WHERE 1"
# rows = mysql_return_rows(cursor, sql)
# print('rows ', rows)

# sql =f"SELECT COUNT(*) as totals FROM `products` WHERE 1"
# rows = mysql_return_rows(cursor, sql)
# print('rows ', rows)

# sql =f"SELECT COUNT(*) as totals FROM `stores` WHERE 1"
# rows = mysql_return_rows(cursor, sql)
# print('rows ', rows)

sql = f"SELECT MAX(poid) as max_poid from products_orders"
maxs = mysql_return_rows(cursor, sql)
# print(maxs)

if maxs[0]['max_poid'] == None:
    start_index = 1
else:
    start_index = int(maxs[0]['max_poid']) + 1

miss_insert = 0
pd_count = 0
for index, row in df.iterrows():
    lst = row['items'].split(', ')
    dic_lst = [{'item':x, 'count':lst.count(x)} for x in set(lst)]
    for dic in dic_lst:
        pd_count += 1
        if match := re.findall('^(Regular|Large) (.*) - ([0-9\.]+)$', dic['item']):
            pid_sql = f"""SELECT product_id FROM products WHERE product_name = '{match[0][1]}' and product_size = '{match[0][0]}'"""
            pid_rows = mysql_return_rows(cursor, pid_sql)
           
            sid_sql = f"""SELECT store_id FROM stores WHERE store_name = '{row['store_name']}'"""
            sid_rows = mysql_return_rows(cursor, sid_sql)
            insert = """
            INSERT INTO products_orders (poid, store_id, order_id, product_id, product_price, quantity_sold) values (%s, %s, %s, %s, %s, %s)
            """
            tuples = (start_index, sid_rows[0]['store_id'], row['order_id'], pid_rows[0]['product_id'], float(match[0][2]), dic['count'])
            mysql_insert_format(conn, insert, tuples)
            start_index += 1
        else:
            miss_insert += 1

os.system("cls")
print("")
print('Number of pandas data input into orders: ', len(df))
print('Number of pandas data input into products_orders: ', pd_count)

sql = f"SELECT MAX(order_id) as max_oid from orders"
maxs = mysql_return_rows(cursor, sql)
print('mySQl max order_id in orders: ', maxs[0]['max_oid'])

sql = f"SELECT MAX(poid) as max_poid from products_orders"
maxs = mysql_return_rows(cursor, sql)
print('mySQl max poid in products_orders: ', maxs[0]['max_poid'])

if len(df) > 0:
    print('miss_insert ', miss_insert)
    print('If miss_insert is 0 that means you are totally 100% successful.')
    print('Congratulation! You now have orders table and products_orders table with normalise data.')
print("")