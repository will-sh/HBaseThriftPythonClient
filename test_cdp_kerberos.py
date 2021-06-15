from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport
from hbase import Hbase
import kerberos
import sasl
from puresasl.client import SASLClient
from subprocess import call,check_output

#CDP7.1.4 kerberos enabled
hbase_thrift_server_host = 'c4677-node2.coelab.cloudera.com'
hbase_thrift_server_port = '9090'
krb5_server='c4677-node1.coelab.cloudera.com'
krb_service='hbase'
principal='hbase/c4677-node2.coelab.cloudera.com'
keytab="/root/HBaseThrift/c4677.keytab"
kinitCommand="kinit"+" "+"-kt"+" "+keytab+" "+principal
call(kinitCommand,shell="True")

socket = TSocket.TSocket(hbase_thrift_server_host, hbase_thrift_server_port)
transport = TTransport.TSaslClientTransport(socket,hbase_thrift_server_host,'hbase',mechanism='GSSAPI')
protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
transport.open()
client = Hbase.Client(protocol)
tables = client.getTableNames()
print(tables)
transport.close()
