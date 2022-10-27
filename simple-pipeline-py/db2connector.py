
import ibm_db
import os
from dotenv import load_dotenv
load_dotenv()
# connectction details

dsn_hostname = os.getenv("DB2_HOST") # e.g.: "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
dsn_uid =  os.getenv("DB2_USER")        # e.g. "abc12345"
dsn_pwd = os.getenv("DB2_PWD")      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port = os.getenv("DB2_PORT")                # e.g. "50000" 
dsn_database = "bludb"            # i.e. "BLUDB"
dsn_driver = "{IBM DB2 ODBC DRIVER}" # i.e. "{IBM DB2 ODBC DRIVER}"           
dsn_protocol = "TCPIP"            # i.e. "TCPIP"
dsn_security = "SSL"              # i.e. "SSL"

#Create the dsn connection string
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

# create connection
def createConnection():
    conn = ibm_db.connect(dsn, "", "")
    print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)
    return conn

def closeConnection(conn):
    print("try closing db2 connection")
    try:
        ibm_db.close(conn)
        print("db2 connection closed")
    except e:
        print(e)
        print("Cannot close db2 connection")
    return

def query(sql,conn):
    stmt = ibm_db.exec_immediate(conn, sql)
    results = []
    tuple = True
    while tuple != False:
        tuple = ibm_db.fetch_tuple(stmt)
        if tuple is not False:
            results.append(tuple)
    return results

def prepare(conn,sql):
    return ibm_db.prepare(conn, sql)

def execute(sql,value):
    return ibm_db.execute(sql,value)