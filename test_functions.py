from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase
import kerberos
from subprocess import call
import os

#CDP6.3.4 kerberos enabled
hbase_thrift_server_host = 'c1677-node4.coelab.cloudera.com'
hbase_thrift_server_port = 9090
def display_title_bar():
    # Clears the terminal screen, and displays a title bar.
    os.system('clear')
              
    print("\t***************************************************")
    print("\t*** Python App- Interact with HBase Thrift Proxy***")
    print("\t***************************************************")
  
def kinit():
  krb5_server='c1677-node1.coelab.cloudera.com'
  krb_service='hbase'
  principal='hbase/c1677-node4.coelab.cloudera.com'
  keytab="/root/HBaseThrift/c1677.keytab"
  kinitCommand="kinit"+" "+"-kt"+" "+keytab+" "+principal
  call(kinitCommand,shell="True")

def connect(thrift_host,thrift_port):
  socket = TSocket.TSocket(thrift_host, thrift_port)
  transport = TTransport.TSaslClientTransport(
    socket,
    host=thrift_host,
    service='hbase',
    mechanism='GSSAPI'
  )
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = Hbase.Client(protocol)
  return client,transport

def getAllTableNames():
  alltables = client.getTableNames()
  tables=[]
  for t in alltables:
    tables.append(t.decode())
  return tables

def showAllTables():
  tables=getAllTableNames()
  print("Current All Tables:\n",tables)

def searchTable(tablename):
  tables = getAllTableNames()
  found = False
  for table in tables:
    if table == tablename:
      found = True
    return found

def createTable(tablename,cfname):
  found=searchTable(tablename)
  try:
    client.createTable(tablename.encode(), [Hbase.ColumnDescriptor(name=cfname.encode())])
    print(tablename + " is created!")
  except:
    print(tablename + " is already exists!")

def istableenabled(tablename):
  istblenabled=client.isTableEnabled(tablename.encode())
  return istblenabled

def enableTable(tablename):
  try:
    client.enableTable(tablename.encode())
  except:
    print("cannot enable "+tablename)

def disableTable(tablename):
  try:
    client.disableTable(tablename.encode())
  except:
    print("cannot disable "+tablename)

def deleteTable(tablename):
  try:
    client.deleteTable(tablename.encode())
  except:
    print(tablename + "cannot be deleted")

def mutateRows(tablename,cfname,rowkey,columnvalue):
  mutations = [Hbase.Mutation(column=cfname.encode(), value=columnvalue.encode())]
  client.mutateRow(tablename.encode(), rowkey.encode(), mutations,None)

def getRows(tablename,rowkey,cfname):
  rows=client.getRow(tablename.encode(),rowkey.encode(),None)
  for row in rows:  
    cv=row.columns.get(cfname.encode()).value
    print("RowKey="+row.row.decode(),"ColumnValue="+cv.decode())

if  __name__ == "__main__":
  kinit()
  client,transport=connect(hbase_thrift_server_host,hbase_thrift_server_port)
  transport.open()
  display_title_bar()
  choice=''
  while choice!='q':
    print("\n[1] Show All Tables.")
    print("[2] Create New Table.")
    print("[3] Enable Table")
    print("[4] Disable Table")
    print("[5] Delete Table")
    print("[6] Mutate Rows")
    print("[7] Get Rows")
    print("[c] Clear Screen")
    print("[q] Exit")
    choice = input("What is your choice? ")
    if choice=='1':
      showAllTables()
    elif choice=='2':
      input_create_tbl=str(input("Input Table Name to create a table:\n"))
      input_create_col=str(input("Input Column Family Name to create a table:\n"))
      createTable(input_create_tbl,input_create_col)
      if searchTable('input_create_tbl'):
        print(input_create_tbl+" is created!")
    elif choice=='3':
      input_enable_tbl=str(input("Input Table Name to enable:\n"))
      enableTable(input_enable_tbl)
      if not istableenabled(input_enable_tbl):
        print(input_disable_tbl+" is disabled!")
    elif choice=='4':
      input_disable_tbl=str(input("Input Table Name to disable:\n"))
      disableTable(input_disable_tbl)
      if istableenabled(input_disable_tbl):
        print(input_disable_tbl+" is disabled!")
    elif choice=='5':
      input_delete_tbl=str(input("Input Table Name to delete:\n"))
      confirm=str(input("The Table "+input_delete_tbl+" will be deleted from HBase, press [Y/y] to confirm:"))
      if confirm=='Y' or 'y':
        deleteTable(input_delete_tbl)
      if not searchTable(input_delete_tbl):
        print(input_delete_tbl+" is deleted!")
    elif choice=='6':
      input_mutate_tbl=str(input('Input Table Name to mutate:'))
      input_mutate_cf=str(input('Input CF Name:'))
      input_mutate_rk=str(input('Input RowKey:'))
      input_mutate_cv=str(input('Input ColumnValue:'))
      mutateRows(input_mutate_tbl,input_mutate_cf,input_mutate_rk,input_mutate_cv)
    elif choice=='7':
      input_getrow_tbl=str(input('Input Table Name:'))
      input_getrow_rk=str(input('Input Rowkey:'))
      input_getrow_cf=str(input('Input CF Name:'))
      getRows(input_getrow_tbl,input_getrow_rk,input_getrow_cf)
    elif choice=='c':
      display_title_bar()
    elif choice=='q':
      print("\nThanks for playing. Bye.")
      transport.close()