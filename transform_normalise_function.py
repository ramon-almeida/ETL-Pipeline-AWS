import pymysql
import re
# Once we have a solution in-place to clean the transaction data, we need to normalise it. We need to separate the data into a single record for the transaction itself, and one record each per basket item. Each basket record should have a foreign key pointing to the transactions primary key.

file_name = 'data.csv' # your CSV file path and file name.

database = 'cup_of_joy' # Database name you want to created
host = 'localhost'
user = 'root'
password = 'password'

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


conn = pymysql.connect(
host = host,
user = user,
password = password,
database = database,
cursorclass = pymysql.cursors.DictCursor
)
cursor = conn.cursor()

sql = "SHOW COLUMNS FROM `transactions` LIKE 'tid'"
rows = mysql_return_rows(cursor, sql) 

if len(rows) < 1:
    sql = f"""
    ALTER TABLE `transactions` 
    ADD COLUMN `tid` INT NOT NULL AUTO_INCREMENT FIRST,
    ADD PRIMARY KEY (`tid`),
    ADD UNIQUE INDEX `tid_UNIQUE` (`tid` ASC) VISIBLE;
    """
    mysql_execute_commit(conn, sql)


sql = f"""DROP TABLE IF EXISTS `trans_items`;"""
mysql_execute_commit(conn, sql)


sql = f"""
CREATE TABLE IF NOT EXISTS `trans_items` (
  `tmid` INT NOT NULL AUTO_INCREMENT,
  `tid` INT NOT NULL,
  `item_name` VARCHAR(45) NULL,
  `item_price` FLOAT NULL,
  `item_size` VARCHAR(45) NULL,
  `trans_time` datetime DEFAULT NULL,
  PRIMARY KEY (`tmid`),
  UNIQUE INDEX `tpid_UNIQUE` (`tmid` ASC) VISIBLE
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;
"""
mysql_execute_commit(conn, sql)



sql = f"""
    SELECT tid, trans_time, store_name, items, total_price, payment_type FROM transactions WHERE 1
"""
rows = mysql_return_rows(cursor, sql) 


miss_insert = 0
for row in rows:
    lst = row['items'].split(', ')
    for item in lst:
        if match := re.findall('^(Regular |Large )(.*) - ([0-9\.]+)$', item):
            insert = """
            INSERT INTO trans_items (tid, item_name, item_price, item_size, trans_time) values (%s, %s, %s, %s, %s)
            """
            tuples = (row['tid'], match[0][1], float(match[0][2]), match[0][0], row['trans_time'])
            mysql_insert_format(conn, insert, tuples)
        else:
            miss_insert += 1
            


print('miss_insert ', miss_insert)
        



