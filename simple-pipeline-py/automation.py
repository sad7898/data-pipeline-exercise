# Import libraries required for connecting to mysql
from mysqlconnector import createMySQLConnection
# Import libraries required for connecting to DB2
import db2connector
# Connect to MySQL
sqlConn = createMySQLConnection()
# Connect to DB2
db2Conn = db2connector.createConnection()
# Find out the last rowid from DB2 data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database.

def get_last_rowid():
	SQL = "select ROWID from sales_data order by ROWID desc limit 1"
	# code for db2connector.query()
	# def query(sql,conn):
    # stmt = ibm_db.exec_immediate(conn, sql)
    # results = []
    # tuple = True
    # while tuple != False:
    #     tuple = ibm_db.fetch_tuple(stmt)
    #     if tuple is not False:
    #         results.append(tuple)
    # return results
	return db2connector.query(SQL,db2Conn)[0][0]



last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.
 
def get_latest_records(rowid: int):
	if not isinstance(rowid,int) or rowid < 1:
		raise Exception("rowid should be integer")
	cursor = sqlConn.cursor()
	SQL = "select * from sales_data where ROWID > %s"
	cursor.execute(SQL,(rowid,))
	return cursor.fetchall()


new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))


# Insert the additional records from MySQL into DB2 data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database.

def insert_records(records: list):
	SQL = "INSERT INTO sales_data(rowid,product_id,customer_id,quantity)  VALUES(?,?,?,?);"
	prepSql = db2connector.prepare(db2Conn,SQL)
	results = []
	for record in records:
		results.append(db2connector.execute(prepSql,record))
	return results



insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# # disconnect from mysql warehouse
sqlConn.close()
# # disconnect from DB2 data warehouse
db2connector.closeConnection(db2Conn)
# # End of program
