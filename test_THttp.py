from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol
from hbase.Hbase import Client
from subprocess import call
import ssl
import kerberos

def kerberos_auth():
  call("kdestroy",shell="True")
  clientPrincipal='hbase/c2677-node4.coelab.cloudera.com@COELAB.CLOUDERA.COM'


  keytab="/run/cloudera-scm-agent/process/1546335371-hbase-HBASETHRIFTSERVER/hbase.keytab"
  kinitCommand="kinit"+" "+"-kt"+" "+keytab+" "+clientPrincipal
  call(kinitCommand,shell="True")

  hbaseService="HTTP"

  __, krb_context = kerberos.authGSSClientInit(hbaseService)
  kerberos.authGSSClientStep(krb_context, "")
  negotiate_details = kerberos.authGSSClientResponse(krb_context)
  headers = {'Authorization': 'Negotiate ' + negotiate_details,'Content-Type':'application/binary'}
  return headers

httpClient = THttpClient.THttpClient('https://c2677-node4.coelab.cloudera.com:9090/', cert_file='/etc/pki/tls/certs/localhost.crt',key_file='/etc/pki/tls/private/localhost.key', ssl_context=ssl._create_unverified_context())
# if no ssl verification is required
httpClient.setCustomHeaders(headers=kerberos_auth())
protocol = TBinaryProtocol.TBinaryProtocol(httpClient)
httpClient.open()
client = Client(protocol)
tables=client.getTableNames()
print(tables)
httpClient.close()
