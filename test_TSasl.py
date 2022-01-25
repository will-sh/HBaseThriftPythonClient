from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase
import kerberos
import sasl
from subprocess import call

# Replace the <hostname> with your thrift server hostname
# Replace <REALM>,<PID> with yours

thrift_host='<hostname>'
thrift_port=9090

# call kinit commands to get the kerberos ticket. 

krb_service='hbase'
principal='hbase/<hostname>@<REALM>'
keytab="/run/cloudera-scm-agent/process/<PID>-hbase-HBASETHRIFTSERVER/hbase.keytab"
kinitCommand="kinit"+" "+"-kt"+" "+keytab+" "+principal
call(kinitCommand,shell="True")
socket = TSocket.TSocket(thrift_host, thrift_port)
transport = TTransport.TSaslClientTransport(socket,host=thrift_host,service='hbase',mechanism='GSSAPI')
protocol = TBinaryProtocol.TBinaryProtocol(transport)
transport.open()
client = Hbase.Client(protocol)
print(client.getTableNames())
transport.close()

