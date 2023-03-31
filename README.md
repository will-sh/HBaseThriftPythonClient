# HBaseThriftPythonClient

TSaslClientTransport and THttpClient example for hbase thrift python client.

The thrift bindings are compiled using Apache Thrift compiler.

Compile steps:

https://thrift.apache.org/docs/install/

https://docs.cloudera.com/runtime/7.2.10/accessing-hbase/topics/hbase-use-apache-thrift-proxy-api.html

Requirements:

Python version: 3.6.8
pip version: 21.3.1

pip3 install --upgrade pip  
pip3 install virtualenv  
virtualenv py3env  
source py3env/bin/activate  
pip3 install -r requirements.txt  

Package    Version:  
kerberos   1.3.1  
pip        21.3.1  
six        1.16.0  
pure-sasl  0.6.2
setuptools 58.3.0
wheel      0.37.0  

Note:
1. TSaslClientTransport works only after CDP 7.1.7, the earlier CDP version has known bug https://issues.apache.org/jira/browse/HBASE-21652
2. THttpClient can work in all CDP version with kerberos and SSL enabled at same time.
