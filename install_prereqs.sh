#!/usr/bin/env bash

# Some prereqs for easier dev and running kudu binaries.
sudo yum -y install tmux python-devel autoconf automake cyrus-sasl-devel \
  cyrus-sasl-gssapi cyrus-sasl-plain flex gcc gcc-c++ gdb git \
  java-1.8.0-openjdk-devel krb5-server krb5-workstation libtool make \
  openssl-devel patch pkgconfig redhat-lsb-core rsync unzip vim-common which

# pip to get a bunch of data science stuff.
curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
sudo python get-pip.py
sudo pip install numpy matplotlib jupyter
