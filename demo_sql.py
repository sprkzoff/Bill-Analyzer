import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
load_dotenv(verbose=True)



try:
    connection = mysql.connector.connect(host=os.getenv('DB_HOST'),
                                                         database=os.getenv('DB_NAME'),
                                                         user=os.getenv('DB_USER'),
                                                         password=os.getenv('DB_PASSWORD'))
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

        cursor = connection.cursor()
        cursor.execute("select * from expense;")
        record = cursor.fetchall()
        all_expense = sum([e[2] for e in record if e[1] == 'park' ])
        print(all_expense)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")