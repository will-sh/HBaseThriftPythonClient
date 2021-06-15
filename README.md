# HBaseThriftPythonClient

An interactive client app play with HBase Thrift server with kerberos enabled.

The thrift bindings are compiled using Apache Thrift compiler.

https://thrift.apache.org/docs/install/

Features:

- Codes only work in Python 3
- Support Kerberos in both thrift 0.9 and thrift 0.13
- Implemented progressbar  
  Install per  
  `sudo pip3 install progressbar2`   
  https://github.com/WoLpH/python-progressbar
- Implemented simple-term-menu  
  Install per  
  `sudo pip3 install simple-term-menu`  
  https://github.com/IngoMeyer441/simple-term-menu

Preparation steps:
- Prepare CDH6/CDP HBase environment by yourself, enable kerberos in your cluster.
  https://docs.cloudera.com/cdp-private-cloud-base/7.1.6/installation/topics/cdpdc-installation.html
- Install kerberos client and modify krb5.conf in your app server.
- make sure the app server can authenticate to CDH/CDP using latest hbase.keytab.


