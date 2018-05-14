#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import copy
import logging
import os
import paramiko
import re
import shutil
import subprocess
import sys
import time
import urllib2
import yaml

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
BASE_OPTS = None

# Runs a command remotely, logging errors and exiting on failure.
def exec_or_log_error(client, cmd):
    try:
        stdin, stdout, stderr = client.exec_command(cmd)
    except IOError as e:
        logging.error("Error executing: {}".format(cmd))
        for line in stdout:
            logging.error(line)
        sys.exit(1)

# Manages a set of identical Kudu daemons.
class Servers(object):
    def __init__(self, bin_type, server_config, secrets):
        self.bin_type = bin_type
        self.config = server_config
        self.secrets = secrets
        self.addresses = self.config['addresses']
        self.setup_script = None
        self.remote_binary = None
        if len(self.config['setup_script']) > 0:
            self.setup_script = os.path.abspath(self.config['setup_script'])
        self.remote_working_dir = self.config['working_dir']

        # Set the log and fs directory flags.
        self.log_dir = os.path.join(self.remote_working_dir, "logs")
        self.dir_flags = ["--log_dir={}".format(self.log_dir)]
        self.dirs = [self.log_dir]
        for name, path in self.config['dir_flags'].iteritems():
            self.dir_flags.append("--{}={}".format(name, path))
            self.dirs.extend(path.split(','))
        logging.info("dir_flags: {}, dirs: {}".format(self.dir_flags, self.dirs))
        logging.info("addrs: {}".format(self.addresses))

    # Create clients to connect with each server.
    #
    # Create necessary directories on each server and send over any scripts or
    # binaries that may need running.
    def setup(self):
        # Connect to each address.
        self.clients = {}
        for addr in self.addresses:
            logging.info("Connecting to server {}".format(addr))

            # Create a client for this address.
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if addr in self.secrets.keys():
                secret = self.secrets[addr]
                logging.info("Reading secrets for {}".format(addr))
                key = paramiko.RSAKey.from_private_key_file( \
                    secret['private_key_file'], password=secret['pkey_password'])
                logging.info("Logging in with user {}".format(secret['user']))
                client.connect(
                    hostname=addr, username=secret['user'], password=secret['password'], pkey=key)
            else:
                logging.info("No secrets listed for server")
                client.connect(hostname=addr)
            logging.info("Connected to server {}".format(addr))
            self.clients[addr] = client

            # Create any directories we might need.
            exec_or_log_error(client, "mkdir -p {}".format(" ".join(self.dirs)))

        # Send over the setup script and run setup.
        if self.setup_script:
            remote_script = os.path.join(BASE_OPTS['kudu_sbin_dir'], self.setup_script)
            self.distribute_file(remote_script)
            exec_or_log_error(remote_script)

        # Distribute the binary file.
        self.distribute_file(os.path.join(BASE_OPTS['kudu_sbin_dir'], self.bin_type))


    # Runs the binary files across all servers.
    def start(self, flags, port):
        remote_binary = os.path.join(self.remote_working_dir, self.bin_type)
        flags_str = " ".join(self.dir_flags + flags)
        for addr in self.addresses:
            logging.info("Starting daemon on server {}".format(addr))
            cmd = "{} {}".format(remote_binary, flags_str)
            logging.info(cmd)
            client = self.clients[addr]
            stdin, stdout, stderr = client.exec_command(cmd)

            # Wait for the server to come online.
            for x in xrange(60):
                try:
                    logging.info("Waiting for server {} to come up".format(addr))
                    urllib2.urlopen("http://{}:{}/".format(addr, port))
                    break
                except:
                    if x == 59:
                        logging.error("Couldn't ping server...")
                        for line in stdout.readlines():
                            logging.error(line)
                    time.sleep(1)
                    pass

    # Send a local file at 'src' to the servers, putting it in the remote
    # working directory. If the file already exists on a server, that server
    # will be skipped.
    def distribute_file(self, src):
        logging.info("Distributing file {}".format(src))
        remote_file = self.remote_file(os.path.basename(src))
        for addr in self.addresses:
            sftp_client = self.clients[addr].open_sftp()
            try:
                sftp_client.stat(remote_file)
                logging.info("File already exists on {}".format(addr))
            except IOError:
                sftp_client.put(src, remote_file)
            sftp_client.close()

    # Return the name of a file in the remote working directory.
    def remote_file(self, filename):
        return os.path.join(self.remote_working_dir, filename)

    # Retrieve the contents of the metrics directory specific to this run.
    def collect_metrics(self, local_dir):
        return

    # Cleanup each cluster and close the clients for each one.
    def cleanup(self):
        for addr in self.addresses:
            client = self.clients[addr]
            stdin, stdout, stderr = client.exec_command("pkill {}".format(self.bin_type))
            stdin, stdout, stderr = client.exec_command("rm -rf {}".format(" ".join(self.dirs)))
            client.close()


# Encapsulates the masters and tablet servers.
# Expected usage is to:
# 1. setup the servers
# 2. start the servers
# 3. run workloads against the servers
# 4. collect the metrics for the servers
# 5. destroy the servers
class Cluster:
    def __init__(self, config, secrets):
        self.config = config
        self.masters = Servers("kudu-master", self.config['masters'], secrets)
        self.tservers = Servers("kudu-tserver", self.config['tservers'], secrets)

    # Set up the masters and tablet servers, distributing the binaries
    # necessary to start the cluster.
    def setup_servers(self):
        self.masters.setup()
        self.tservers.setup()

    # Start the servers and wait for them to come online.
    def start_servers(self):
        logging.info("Starting masters")
        master_flags = []
        if len(self.masters.addresses) > 1:
            master_flags.append("--master_addresses={}".format(
                ",".join(["{}:7051".format(a) for a in self.masters.addresses])))
        self.masters.start(master_flags, "8051")

        logging.info("Starting tservers")
        tserver_flags = []
        tserver_flags.append("--tserver_master_addrs={}".format(
            ",".join(["{}:7051".format(a) for a in self.masters.addresses])))
        self.tservers.start(tserver_flags, "8050")

    # Run a workload against the cluster.
    def run_workload(self):
        return

    # Kill the Kudu processes on the remote hosts.
    def kill_servers(self):
        logging.info("cleaning up")
        self.masters.cleanup()
        self.tservers.cleanup()


# Create and connect to the various servers of the cluster, running any
# necessary pre-requisite scripts to running workloads.
def load_clusters(setup_yaml, secrets_yaml, cluster_filter):
    clusters = {}
    cluster_re = re.compile(cluster_filter)
    for name, config in setup_yaml['clusters'].iteritems():
        if len(cluster_re.findall(name)) == 0:
            logging.info("Skipping cluster: {}".format(name))
            continue
        logging.info("Connecting to cluster: {}".format(name))
        clusters[name] = Cluster(config, secrets_yaml)
        clusters[name].setup_servers()
    return clusters


# Load the clusters and start them.
def main():
    p = argparse.ArgumentParser("Run a set of experiments")
    p.add_argument("--setup-yaml",
        dest="setup_yaml_path",
        type=str,
        help="YAML file describing experiments to run",
        default=os.path.join(BASE_DIR, "setup.yaml"))
    p.add_argument("--secrets-yaml",
        dest="secrets_yaml_path",
        type=str,
        help="YAML file containing user info",
        default=os.path.join(BASE_DIR, "secrets.yaml"))
    p.add_argument("--cluster-filter",
        dest="cluster_filter",
        type=str, default=".*",
        help="Regex pattern used to filter which clusters to run against")
    args = p.parse_args()
    setup_yaml = yaml.load(file(args.setup_yaml_path))
    secrets_yaml = yaml.load(file(args.secrets_yaml_path))
    global BASE_OPTS
    BASE_OPTS = setup_yaml['base_opts']

    clusters = load_clusters(setup_yaml, secrets_yaml, args.cluster_filter)
    for cname, cluster in clusters.iteritems():
        logging.info("Starting servers for cluster {}".format(cname))
        cluster.start_servers()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
