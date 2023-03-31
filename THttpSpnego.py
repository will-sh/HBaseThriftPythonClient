# This example code assumes to run at HBase Thrift server host
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol
from hbase.Hbase import Client
from ssl import create_default_context
import kerberos
import os
import socket
from subprocess import call

# Get the hostname, agent_cert_dir, ca_file,
def get_env_params():
    hostname = socket.gethostname()
    agent_cert_dir = '/var/lib/cloudera-scm-agent/agent-cert/'
    ca_file = os.path.join(agent_cert_dir, 'cm-auto-global_cacerts.pem')
    keytab_file = '/cdep/keytabs/hbase.keytab'
    return hostname, agent_cert_dir, ca_file, keytab_file

#Check if a valid Kerberos ticket is already present in the cache
def check_kerberos_ticket():
    ccache_file = os.getenv('KRB5CCNAME')
    if ccache_file:
        ccache = CCache.load_ccache(ccache_file)
        if ccache.get_principal() and not ccache.get_principal().is_anonymous():
            return True
    return False

# Obtain a Kerberos ticket by running kinit from keytab
def kinit(keytab_file):
    call(['kinit', '-kt', keytab_file, 'hbase'])

# Function to authenticate with Kerberos and get a SPNEGO token
def get_spnego_token():
    service_name = 'HTTP@{}'.format(hostname)
    result, context = kerberos.authGSSClientInit(service_name, gssflags=kerberos.GSS_C_MUTUAL_FLAG)
    kerberos.authGSSClientStep(context, "")
    spnego_token = kerberos.authGSSClientResponse(context)
    headers = {'Authorization': 'Negotiate {}'.format(spnego_token)}
    return headers

# Initialize an SSL context with certificate verification enabled
def get_ssl_context():
    context = create_default_context()
    context.load_verify_locations(ca_file)
    return context

# Main function to create the HBase client and retrieve tables
if __name__ == '__main__':
    hostname, agent_cert_dir, ca_file, keytab_file = get_env_params()

# Check if a valid Kerberos ticket is already present in the cache
    if not check_kerberos_ticket():
    # If a valid ticket is not present, obtain one by running kinit
        kinit(keytab_file)

    # Create a THttpClient instance with the SSL context and custom headers
    httpClient = THttpClient.THttpClient('https://' + hostname + ':9090/', ssl_context=get_ssl_context())
    httpClient.setCustomHeaders(headers=get_spnego_token())

    # Initialize TBinaryProtocol with THttpClient
    protocol = TBinaryProtocol.TBinaryProtocol(httpClient)

    # Create HBase client
    client = Client(protocol)

    # Retrieve list of HBase tables
    tables = client.getTableNames()
    print(tables)
