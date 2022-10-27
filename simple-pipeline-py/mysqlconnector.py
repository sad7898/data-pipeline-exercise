# This program requires the python module mysql-connector-python to be installed.
# Install it using the below command
# pip3 install mysql-connector-python
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()
def createMySQLConnection():
	connection = mysql.connector.connect(user=os.getenv("MYSQL_USER"), password=os.getenv("MYSQL_PWD"),host=os.getenv("MYSQL_HOST"),database='sales')
	return connection


