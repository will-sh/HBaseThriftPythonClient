from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol
from hbase.Hbase import Client
from subprocess import call
import ssl
import kerberos

def kerberos_auth():
  call("kdestroy",shell="True")

# Replace <hostname> with thrift server hostname
# Replace <hostname>,<REALM>,<PID> with yours.
# Replace <cert_file_path>,<key_file_path> with yours. 
# Example: cert_file='/etc/pki/tls/certs/localhost.crt',key_file='/etc/pki/tls/private/localhost.key'

  clientPrincipal='hbase/<hostname>@<REALM>'
  keytab="/run/cloudera-scm-agent/process/<PID>-hbase-HBASETHRIFTSERVER/hbase.keytab"
  kinitCommand="kinit"+" "+"-kt"+" "+keytab+" "+clientPrincipal
  call(kinitCommand,shell="True")
  hbaseService="HTTP"
  __, krb_context = kerberos.authGSSClientInit(hbaseService)
  kerberos.authGSSClientStep(krb_context, "")
  negotiate_details = kerberos.authGSSClientResponse(krb_context)
  headers = {'Authorization': 'Negotiate ' + negotiate_details,'Content-Type':'application/binary'}
  return headers

httpClient = THttpClient.THttpClient('https://<hostname>:9090/', cert_file='/etc/pki/tls/certs/localhost.crt',key_file='/etc/pki/tls/private/localhost.key', ssl_context=ssl._create_unverified_context())
httpClient.setCustomHeaders(headers=kerberos_auth())
protocol = TBinaryProtocol.TBinaryProtocol(httpClient)
httpClient.open()
client = Client(protocol)
tables=client.getTableNames()
print(tables)
httpClient.close()
