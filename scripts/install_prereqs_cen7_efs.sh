#!/usr/bin/env bash

sudo yum -y update
sudo yum -y install autoconf automake cyrus-sasl-devel cyrus-sasl-gssapi \
    cyrus-sasl-plain flex gcc gcc-c++ gdb git java-1.8.0-openjdk-devel \
    krb5-server krb5-workstation libtool make openssl-devel patch \
    pkgconfig redhat-lsb-core rsync unzip vim-common which

sudo yum -y install nfs-utils

if [[ ! -d /data/efs ]]; then
  sudo mkdir /data/efs
  sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport 172.31.42.25:/   /data/efs
fi

if [[ ! -d /mnte ]]; then
  sudo mkdir /mnte
  sudo mkfs.ext4 /dev/xvdc
  sudo mount -t ext4 /dev/xvdc /mnte
fi
