from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport
from hbase import Hbase
import kerberos
from subprocess import call

#CDP6.3.4 kerberos enabled
hbase_thrift_server_host = 'c1677-node4.coelab.cloudera.com'
hbase_thrift_server_port = 9090

krb5_server='c1677-node1.coelab.cloudera.com'
krb_service='hbase'
principal='hbase/c1677-node4.coelab.cloudera.com'
keytab="/root/HBaseThrift/c1677.keytab"
kinitCommand="kinit"+" "+"-kt"+" "+keytab+" "+principal
call(kinitCommand,shell="True")

socket = TSocket.TSocket(hbase_thrift_server_host, hbase_thrift_server_port)

transport = TTransport.TSaslClientTransport(
 socket,
 host=hbase_thrift_server_host,
 service='hbase',
 mechanism='GSSAPI'
 )
protocol = TBinaryProtocol.TBinaryProtocol(transport)
transport.open()
client = Hbase.Client(protocol)
tables = client.getTableNames()
print(tables)
transport.close()
