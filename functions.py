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
    time.sleep(0.02)

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
  return tables

def searchTable(client,tablename):
  tables = getAllTables(client)
  found = False
  for table in tables:
    if table == tablename:
      found = True
    return found

def createTable(client):
  tables=showAllTables(client)
  tablename=input("Enter table name:\n")
  if tablename not in tables:
    cfname=input("Enter cf name (e.g. cf:a):\n")
    found=searchTable(client,tablename)
    try:
      client.createTable(tablename.encode(), [Hbase.ColumnDescriptor(name=cfname.encode())])
      tables=showAllTables(client)
      if tablename in tables:
        print("table ["+tablename+"] is created!")
    except:
      print("table ["+tablename + "] is already exists!")
  else:
    print("table ["+tablename + "] is already exists!")
def istableenabled(client,tablename):
  istblenabled=client.isTableEnabled(tablename.encode())
  return istblenabled

def enableTable(client):
  tables=showAllTables(client)
  tablename=input("Enter table name:\n")
  if tablename in tables:
    if istableenabled(client,tablename):
      print("table ["+tablename+"] is already enabled\n")
    else:
      try:
        client.enableTable(tablename.encode())
        print(tablename+" is enabled!\n")
      except:
        print("cannot enable table ["+tablename+"]\n")
  else:
    print("table ["+tablename+"] does not exist!\n")

def disableTable(client):
  tables=showAllTables(client)
  tablename=input("Enter table name:\n")
  if tablename in tables:
    if istableenabled(client,tablename):
      try:
        client.disableTable(tablename.encode())
        print("table ["+tablename+"] is disabled\n")
      except:
        print("cannot disable ["+tablename+"]\n")
  else:
    print("table ["+tablename+"] does not exist!\n")

def deleteTable(client):
  tables=showAllTables(client)
  tablename=input("Enter table name:\n")
  if tablename in tables:
    if istableenabled(client,tablename):
      print("cannot delete table ["+tablename+"] because it is enabled! Please disabled it firstly!\n")
    else:
      try:
        client.deleteTable(tablename.encode())
        print("table ["+tablename+"] is deleted\n")
      except:
        print("table ["+tablename +"] cannot be deleted\n")
  else:
    print("table ["+tablename+"] does not exist!\n")

def getRows(client):
  tables=showAllTables(client)
  tablename=str(input('Input Table Name:\n'))
  if tablename in tables:
    rowkey=str(input('Input Rowkey:\n'))
    cfname=str(input('Input CF Name:\n'))
    rows=client.getRow(tablename.encode(),rowkey.encode(),None)
    print(rows)
    for row in rows:
      cv=row.columns.get(cfname.encode()).value
      print("RowKey="+row.row.decode(),"ColumnValue="+cv.decode())
  else:
    print("table ["+tablename+"] does not exist!\n")

def showRows(client,tablename,rowkey):
  rows=client.getRow(tablename.encode(),rowkey.encode(),None)
  return rows

def mutateRows(client):
  tables=showAllTables(client)
  tablename=str(input('Input Table Name:\n'))
  if tablename in tables:
    rowkey=str(input('Input Rowkey:\n'))
    cfname=str(input('Input CF Name:\n'))
    columnvalue=str(input('Input Column Value:\n'))
    if rowkey and cfname and columnvalue:
      mutations = [Hbase.Mutation(column=cfname.encode(), value=columnvalue.encode())]
      client.mutateRow(tablename.encode(), rowkey.encode(), mutations,None)
      print(showRows(client,tablename,rowkey))
  else:
    print("table ["+tablename+"] does not exist!\n")

def deleteRows(client):
  tables=showAllTables(client)
  tablename=str(input('Input Table Name:\n'))
  if tablename in tables:
    rowkey=str(input('Input Rowkey:\n'))
    if rowkey:
      rows=showRows(client,tablename,rowkey)
      for row in rows:
        if rowkey in row.row.decode():
          client.deleteAllRow(tablename.encode(),rowkey.encode(), attributes =None)
          if not showRows(client,tablename,rowkey):
            print("The row of "+rowkey+" is deleted\n")
        else:
          print("The rowkey ["+rowkey+"] is not exists in table ["+tablename+"]\n")
  else:
    print("table ["+tablename+"] does not exist!\n")