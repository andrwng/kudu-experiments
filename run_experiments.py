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
        self.flags = []
        self.dirs = []
        self.remote_working_dir = self.config['working_dir']
        self.dirs.append(self.remote_working_dir)
        for name, path in self.config['dir_flags'].iteritems():
            self.flags.append("--{}={}".format(name, path))
            self.dirs.extend(path.split(','))
        logging.info("flags: {}, dirs: {}".format(self.flags, self.dirs))
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

            # Create client.
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if addr in self.secrets.keys():
                secret = self.secrets[addr]
                logging.info("Reading secrets for {}".format(addr))
                key = paramiko.RSAKey.from_private_key_file( \
                    secret['private_key_file'], password=secret['pkey_password'])
                logging.info("Logging in with user {}".format(secret['user']))
                client.connect(hostname=addr, username=secret['user'], password=secret['password'], pkey=key)
            else:
                logging.info("No secrets listed for server")
                client.connect(hostname=addr)
            logging.info("Connected to server {}".format(addr))
            self.clients[addr] = client

            # Create any directories we might need.
            stdin, stdout, stderr = client.exec_command("mkdir -p {}".format(" ".join(self.dirs)))

        # Send over the setup script and run setup.
        if self.setup_script:
            self.distribute_file(self.setup_script)

        # Distribute the binary file.
        self.remote_binary = self.distribute_file(
            os.path.join(BASE_OPTS['kudu_sbin_dir'], self.bin_type))


    # Runs the binary files across all servers.
    def start(self, flags, port):
        for addr in self.addresses:
            logging.info("Starting daemon on server {}".format(addr))
            client = self.clients[addr]
            stdin, stdout, stderr = client.exec_command(
                "{} {}".format(self.remote_binary), flags)

            # Wait for the server to come online.
            for x in xrange(60):
                try:
                    logging.info("Waiting for server {} to come up".format(addr))
                    urllib2.urlopen("http://{}:{}/".format(addr, port))
                    break
                except:
                    if x == 59:
                        raise
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
        return remote_file

    # Returns the name of a file in the remote working directory.
    def remote_file(self, filename):
        return os.path.join(self.remote_working_dir, filename)

    # Retrieves the contents of the metrics directory specific to this run.
    def collect_metrics(self, local_dir):
        return

    # Cleanup each cluster and close the clients for each one.
    def cleanup(self):
        for addr in self.addresses:
            client = self.clients[addr]
            stdin, stdout, stderr = client.exec_command("pkill {}".format(self.bin_type))
            stdin, stdout, stderr = client.exec_command("rm -rf {}".format(" ".join(self.dirs)))
            client.close()

    # Returns the requested metrics for the servers.
    def collect_metrics(self, metrics):
        return


# Encapsulates the masters and tablet servers.
class Cluster:
    def __init__(self, config, secrets):
        self.config = config
        self.masters = Servers("kudu-master", self.config['masters'], secrets)
        self.tservers = Servers("kudu-tserver", self.config['tservers'], secrets)

    # Sets up the masters and tablet servers, distributing the binaries
    # necessary to start the cluster.
    def setup_servers(self):
        self.masters.setup()
        self.tservers.setup()

    # Starts the servers and waits for them to come online.
    def start_servers(self):
        logging.info("starting masters")
        self.masters.start(master_flags, "8051")

        logging.info("starting tservers")
        self.tservers.start(tserver_flags, "8050")

    # Kills the Kudu processes on the remote hosts.
    def kill_servers(self):
        logging.info("cleaning up")
        self.masters.cleanup()
        self.tservers.cleanup()

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

    # Add the local binary directory to the local path.
    kudu_sbin_dir = BASE_OPTS['kudu_sbin_dir']
    if len(kudu_sbin_dir) == 0:
        sys.path.append(kudu_sbin_dir)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
