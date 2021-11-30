from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase
import kerberos
import sasl
from subprocess import call
thrift_host='c2677-node4.coelab.cloudera.com'
thrift_port=9090
# call kinit commands to get the kerberos ticket. 
krb_service='hbase'
principal='hbase/c2677-node4.coelab.cloudera.com@COELAB.CLOUDERA.COM'
keytab="/run/cloudera-scm-agent/process/1546335371-hbase-HBASETHRIFTSERVER/hbase.keytab"
kinitCommand="kinit"+" "+"-kt"+" "+keytab+" "+principal
call(kinitCommand,shell="True")
socket = TSocket.TSocket(thrift_host, thrift_port)
transport = TTransport.TSaslClientTransport(socket,host=thrift_host,service='hbase',mechanism='GSSAPI')
protocol = TBinaryProtocol.TBinaryProtocol(transport)
transport.open()
client = Hbase.Client(protocol)
print(client.getTableNames())
transport.close()

