from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol
from hbase import Hbase
import socket

# Initialize TSocket and TTransport
hostname = socket.gethostname()
t_socket = TSocket.TSocket(hostname, 9090)
transport=TTransport.TSaslClientTransport(t_socket,host=hostname,service='hbase',mechanism='GSSAPI')

# Initialize TCompactProtocol with TTransport
protocol = TCompactProtocol.TCompactProtocol(transport)

# Create HBase client
client = Hbase.Client(protocol)

# Open connection and retrieve list of HBase tables
transport.open()
tables = client.getTableNames()
print(tables)

# Close connection
transport.close()

