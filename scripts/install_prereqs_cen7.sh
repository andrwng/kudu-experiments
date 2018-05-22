#!/usr/bin/env bash

# Install all Kudu dependencies.
sudo yum -y update
sudo yum -y install autoconf automake cyrus-sasl-devel cyrus-sasl-gssapi \
    cyrus-sasl-plain flex gcc gcc-c++ gdb git java-1.8.0-openjdk-devel \
    krb5-server krb5-workstation libtool make openssl-devel patch \
    pkgconfig redhat-lsb-core rsync unzip vim-common which

# Mount any devices that might exist.
if [[ ! -d /mntd ]]; then
  sudo mkdir /mntd
  sudo mkfs.ext4 /dev/xvdb
  sudo mount -t ext4 /dev/xvdb /mntd
fi
