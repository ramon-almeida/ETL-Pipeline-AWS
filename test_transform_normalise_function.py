# Unit tests for create_my_sql_db
from unittest.mock import MagicMock
from unittest import TestCase
from unittest.mock import patch
import mysql.connector

    
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


class Test_Class(TestCase):
                
    # Basic Mocking test to ensure correct arguments passed in    
    def test_mysql_execute_commit(self):
        conn = MagicMock()
        # Make call using mocked cursor
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
        # Assert correct commands were called
        try:
            conn.cursor().execute.assert_any_call(sql)
        except:
            print("Test Failed function mysql_execute_commit(), conn_create.cursor().execute() not called with expected command")
        else:
            print("Test passed function mysql_execute_commit()")  

    def test_mysql_insert_format(self):
        conn = MagicMock()
        # Make call using mocked cursor
        insert = """
            INSERT INTO trans_items (tid, item_name, item_price, item_size, trans_time) values (%s, %s, %s, %s, %s)
            """
        tuples = (1, 'Flavoured iced latte - Hazelnut', 2.75, 'Regular', '2021-08-25 09:00:00')
        mysql_insert_format(conn, insert, tuples)
        # Assert correct commands were called
        try:
            conn.cursor().execute.assert_any_call(insert, tuples)
        except:
            print("Test Failed function mysql_insert_format(), conn_create.cursor().execute() not called with expected command")
        else:
            print("Test passed function mysql_insert_format()")  
            
    def test_mysql_return_rows(self):
        cursor = MagicMock()
        # Make call using mocked cursor
        sql = f"""SELECT tid, trans_time, store_name, items, total_price, payment_type FROM transactions WHERE 1"""
        mysql_return_rows(cursor, sql)
        # Assert correct commands were called
        try:
            cursor.execute.assert_any_call(sql)
        except:
            print("Test Failed function mysql_return_rows(), conn_create.cursor().execute() not called with expected command")
        else:
            print("Test passed function mysql_return_rows()")                                               
    
testing = Test_Class()
testing.test_mysql_execute_commit()
testing.test_mysql_insert_format()
testing.test_mysql_return_rows()
