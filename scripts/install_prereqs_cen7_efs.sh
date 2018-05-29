#!/usr/bin/env bash
set -ex 

sudo yum -y update
sudo yum -y install autoconf automake cyrus-sasl-devel cyrus-sasl-gssapi \
    cyrus-sasl-plain flex gcc gcc-c++ gdb git java-1.8.0-openjdk-devel \
    krb5-server krb5-workstation libtool make openssl-devel patch \
    pkgconfig redhat-lsb-core rsync unzip vim-common which

sudo yum -y install nfs-utils

if [[ ! -d $MOUNTPOINT ]]; then
  sudo mkdir $MOUNTPOINT
  sudo mkfs.ext4 $DEVICE
  sudo mount -t ext4 $DEVICE $MOUNTPOINT
fi

if [[ ! -d $EFS_MOUNTPOINT ]]; then
  sudo mkdir $EFS_MOUNTPOINT
  sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport $EFS_IP:/   $EFS_MOUNTPOINT
fi

