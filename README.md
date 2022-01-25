# HBaseThriftPythonClient

TSaslClientTransport and THttpClient example for hbase thrift python client.

The thrift bindings are compiled using Apache Thrift compiler.

Compile steps:

https://thrift.apache.org/docs/install/

https://docs.cloudera.com/runtime/7.2.10/accessing-hbase/topics/hbase-use-apache-thrift-proxy-api.html

Requirements:

yum install cyrus-sasl-devel,python3,python3-devel  
pip3 install virtualenv  
virtualenv py3env  
source py3env/bin/activate  

Python version: 3.6.8  
pip version: 21.3.1  
pip list:  
Package    Version:  
kerberos   1.3.1  
pip        21.3.1  
pure-sasl  0.6.2  
sasl       0.3.1  
setuptools 58.3.0  
six        1.16.0  
wheel      0.37.0  

1. TSaslTransport
HBase configs:  
hbase.regionserver.thrift.http = false  
hbase.thrift.support.proxyuser = true  
hbase.regionserver.thrift.framed = false  
hbase.regionserver.thrift.compact = false  
hbase.thrift.security.qop = auth-conf  

2. THttpClient  
HBase configs:  
hbase.thrift.ssl.enabled = true  
ihbase.regionserver.thrift.http = true  
hbase.thrift.support.proxyuser = true  
hbase.regionserver.thrift.framed = false  
hbase.regionserver.thrift.compact = false  
hbase.thrift.security.qop = auth-conf  

Note:
1. TSaslClientTransport works only after CDP 7.1.7, the earlier CDP version has known bug https://issues.apache.org/jira/browse/HBASE-21652
2. THttpClient can work in all CDP version with kerberos and SSL enabled at same time.
