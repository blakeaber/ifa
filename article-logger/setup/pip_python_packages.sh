#!/bin/bash -xe

# Non-standard and non-Amazon Machine Image Python modules
# ---------------------------------------------------------
sudo pip install -U feedparser bs4 selenium
sudo yum install -y gcc libxml2-devel libxslt-devel
sudo pip install lxml==3.6.0

# install phantomjs on ec2
# ---------------------------------------------------------
wget https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-linux-x86_64.tar.bz2
sudo mkdir -p /opt/phantomjs
bzip2 -d phantomjs-2.1.1-linux-x86_64.tar.bz2
sudo tar -xvf phantomjs-2.1.1-linux-x86_64.tar --directory /opt/phantomjs/ --strip-components 1
sudo ln -s /opt/phantomjs/bin/phantomjs /usr/bin/phantomjs

phantomjs /opt/phantomjs/examples/hello.js