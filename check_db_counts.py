import pymysql
import pandas as pd

def check_data():
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd', autocommit=True)
    cursor = connection.cursor()
    
    print("Checking daily_index_test count...")
    cursor.execute("SELECT count(*) FROM daily_index_test")
    print(f"daily_index_test rows: {cursor.fetchone()[0]}")
    
    print("Checking five_minute_index_test count...")
    cursor.execute("SELECT count(*) FROM five_minute_index_test")
    print(f"five_minute_index_test rows: {cursor.fetchone()[0]}")

    print("Checking daily_price count...")
    cursor.execute("SELECT count(*) FROM daily_price")
    print(f"daily_price rows: {cursor.fetchone()[0]}")
    
    print("Checking if test_index_list has data for kind 3...")
    cursor.execute("SELECT * FROM test_index_list WHERE test_index_kind=3")
    rows = cursor.fetchall()
    print(f"test_index_list kind 3: {len(rows)} rows")
    
    cursor.close()
    connection.close()

if __name__ == "__main__":
    check_data()
