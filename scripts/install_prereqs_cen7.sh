#!/usr/bin/env bash

# Install all Kudu dependencies.
sudo yum -y update
sudo yum -y install autoconf automake cyrus-sasl-devel cyrus-sasl-gssapi \
    cyrus-sasl-plain flex gcc gcc-c++ gdb git java-1.8.0-openjdk-devel \
    krb5-server krb5-workstation libtool make openssl-devel patch \
    pkgconfig redhat-lsb-core rsync unzip vim-common which

# Mount any devices that might exist.
if [[ -z $(sudo df -h | grep $DEVICE) ]]; then
  sudo rm -rf $MOUNTPOINT || true
  sudo mkdir $MOUNTPOINT
  sudo mkfs.ext4 $DEVICE
  sudo mount -t ext4 $DEVICE $MOUNTPOINT
fi
