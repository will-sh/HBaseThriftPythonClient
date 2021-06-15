from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase
from subprocess import call
import time
import progressbar

def pbar():
  print("\nInitializing app\n")
  for i in progressbar.progressbar(range(100)):
    time.sleep(0.03)

def test():
  print("I'm test in functions.py")

def kinit():
  krb5_server='c1677-node1.coelab.cloudera.com'
  krb_service='hbase'
  principal='hbase/c1677-node4.coelab.cloudera.com'
  keytab="/root/keytabs/c1677.keytab"
  kinitCommand="kinit"+" "+"-kt"+" "+keytab+" "+principal
  call(kinitCommand,shell="True")
  call("klist",shell="True")

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


def getAllTables(client):
  alltables = client.getTableNames()
  tables=[]
  for t in alltables:
    tables.append(t.decode())
  return tables

def showAllTables(client):
  tables=getAllTables(client)
  print(tables,"\n")

def searchTable(client,tablename):
  tables = getAllTables(client)
  found = False
  for table in tables:
    if table == tablename:
      found = True
    return found

def createTable(client):
  tablename=input("Enter table name:\n")
  cfname=input("Enter cf name (e.g. cf:a):\n")
  found=searchTable(client,tablename)
  try:
    client.createTable(tablename.encode(), [Hbase.ColumnDescriptor(name=cfname.encode())])
    print(tablename + " is created!")
  except:
    print(tablename + " is already exists!")

def istableenabled(client,tablename):
  istblenabled=client.isTableEnabled(tablename.encode())
  return istblenabled

def enableTable(client):
  tablename=input("Enter table name:\n")
  if istableenabled(client,tablename):
    print(tablename+" is already enabled\n")
  else:
    try:
      client.enableTable(tablename.encode())
      print(tablename+" is enabled!\n")
    except:
      print("cannot enable "+tablename+"\n")

def disableTable(client):
  tablename=input("Enter table name:\n")
  if istableenabled(client,tablename):
    try:
      client.disableTable(tablename.encode())
      print(tablename+" is disabled\n")
    except:
      print("cannot disable "+tablename)

def deleteTable(client):
  tablename=input("Enter table name:\n")
  if istableenabled(client,tablename):
    print("cannot delete "+tablename+" because it is enabled! Please disabled it firstly!\n")
  else:
    try:
      client.deleteTable(tablename.encode())
      print(tablename+" is deleted\n")
    except:
      print(tablename + "cannot be deleted\n")

def mutateRows(client):
  tablename=str(input('Input Table Name:\n'))
  rowkey=str(input('Input Rowkey:\n'))
  cfname=str(input('Input CF Name:\n'))
  columnvalue=str(input('Input Column Value:\n'))
  mutations = [Hbase.Mutation(column=cfname.encode(), value=columnvalue.encode())]
  client.mutateRow(tablename.encode(), rowkey.encode(), mutations,None)

def getRows(client):
  tablename=str(input('Input Table Name:\n'))
  rowkey=str(input('Input Rowkey:\n'))
  cfname=str(input('Input CF Name:\n'))
  rows=client.getRow(tablename.encode(),rowkey.encode(),None)
  print(rows)
  for row in rows:
    cv=row.columns.get(cfname.encode()).value
    print("RowKey="+row.row.decode(),"ColumnValue="+cv.decode())

def deleteRows(client):
  tablename=str(input('Input Table Name:\n'))
  rowkey=str(input('Input Rowkey:\n'))
  client.deleteAllRow(tablename.encode(),rowkey.encode(), attributes =None)
  print("The row of "+rowkey+" is deleted\n")