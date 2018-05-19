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
import datetime
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
WORKLOADS_DIR = os.path.join(BASE_DIR, "workloads")
LOCAL_LOGS_DIR = os.path.join(BASE_DIR, "logs")
LOCAL_SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
BASE_OPTS = None
SECRETS = None
HOST_CONFIGS = None

# Runs a command remotely, logging errors and exiting on failure.
def exec_and_log(client, cmd):
    try:
        logging.info("Running command: {}".format(cmd))
        stdin, stdout, stderr = client.exec_command(cmd)
    except IOError as e:
        logging.error("Error executing")
        sys.exit(1)
    finally:
        logging.info("STDOUT:")
        for line in stdout:
            logging.info(line)
        logging.info("STDERR:")
        for line in stderr:
            logging.error(line)


# Manages a set of identical Kudu daemons.
class Servers(object):
    def __init__(self, bin_type, server_config):
        logging.info("Initializing {} servers".format(bin_type))
        self.dir_flags = []
        self.dirs = []
        self.bin_type = bin_type
        self.config = server_config
        self.addresses_and_bin = \
          [(HOST_CONFIGS[host]["address"], os.path.join(HOST_CONFIGS[host]["bin_dir"], bin_type)) \
            for host in self.config["hosts"]]

        # Set the log and fs directory flags.
        for flag_name, path in self.config['dir_flags'].iteritems():
            self.dir_flags.append("--{}={}".format(flag_name, path))
            self.dirs.extend(path.split(','))
        self.remote_log_dir = self.config["dir_flags"]["log_dir"]

        logging.info("dir_flags: {}, dirs: {}".format(self.dir_flags, self.dirs))
        logging.info("addrs: {}".format(self.addresses_and_bin))

    # Create clients to connect with each server.
    #
    # Create necessary directories on each server and send over any scripts or
    # binaries that may need running.
    def setup(self):
        # Connect to each address.
        self.clients = {}
        for addr, _ in self.addresses_and_bin:
            logging.info("Connecting to server {}".format(addr))

            # Create an SSH client for this address.
            client = connect_to_host(addr)
            self.clients[addr] = client

            # Destroy and recreate any directories we might need.
            exec_and_log(client, "sudo rm -rf {}".format(" ".join(self.dirs)))
            exec_and_log(client, "sudo mkdir -p {}".format(" ".join(self.dirs)))
            exec_and_log(client, "sudo chown -R {} {}".format( \
                SECRETS[addr]['user'], " ".join(self.dirs)))

    # Runs the binary files across all servers.
    def start(self, flags, port):
        flags_str = " ".join(self.dir_flags + flags)
        flags_str += " --block_manager=file --trusted_subnets=0.0.0.0/0 "
        for addr, bin_file in self.addresses_and_bin:
            logging.info("Starting daemon on server {}".format(addr))
            cmd = "{} {}".format(bin_file, flags_str + \
                " --rpc_advertised_addresses={}".format(addr))
            logging.info(cmd)
            client = self.clients[addr]
            stdin, stdout, stderr = client.exec_command(cmd)
            logging.info("Waiting a bit for startup")
            time.sleep(3)

    # Retrieve the contents of each server's metrics directory, limited to the
    # most recent workload, and place them in 'local_dir'.
    def collect_metrics(self, local_dir):
        metrics_dir = os.path.abspath(local_dir)
        metrics_regex = re.compile("{}.*diagnostics.*".format(self.bin_type))
        for addr, _ in self.addresses_and_bin:
            # Make the local directory for this server.
            local_metrics_dir = os.path.join(metrics_dir, addr)
            subprocess.check_output(["mkdir", "-p", local_metrics_dir])

            # Now fetch the remote diagnostics files, only picking out the ones
            # that belong to this server.
            sftp_client = self.clients[addr].open_sftp()
            for filename in sftp_client.listdir(self.remote_log_dir):
                if len(metrics_regex.findall(filename)) > 0:
                    logging.info("Getting remote file: {}".format(filename))
                    remote_metrics_log = os.path.join(self.remote_log_dir, filename)
                    local_metrics_log = os.path.join(local_metrics_dir, filename)
                    sftp_client.get(remote_metrics_log, local_metrics_log)

    # Stop each node.
    def stop(self):
        for addr, _ in self.addresses_and_bin:
            client = self.clients[addr]
            exec_and_log(client, "pkill {}".format(self.bin_type))

    # Delete the server's contents.
    def cleanup(self):
        for addr, _ in self.addresses_and_bin:
            client = self.clients[addr]
            exec_and_log(client, "rm -rf {}".format(" ".join(self.dirs)))

    # Close any existing connections to servers.
    def close(self):
        for client in self.clients:
            client.close()

# Encapsulates the masters and tablet servers.
# Expected usage is to:
# 1. setup the servers
# 2. start the servers
# 3. run workloads against the servers
# 4. collect the metrics for the servers
# 5. destroy the servers
class Cluster(object):
    def __init__(self, config):
        self.config = config
        self.tservers = Servers("kudu-tserver", self.config['tservers'])
        self.masters = Servers("kudu-master", self.config['masters'])
        self.master_addrs = ",".join(["{}:7051".format(a) for a, _ in self.masters.addresses_and_bin])
        self.cluster_env = os.environ.copy()
        self.cluster_env["KUDU_MASTERS"] = self.master_addrs
        self.cluster_env["PATH"] += os.pathsep + BASE_OPTS["kudu_sbin_dir"]

    # Set up the masters and tablet servers, creating the appropriate
    # necessary to start the cluster.
    def setup_servers(self):
        self.masters.setup()
        self.tservers.setup()

    # Start the servers and wait for them to come online.
    def start_servers(self):
        logging.info("Starting masters")
        master_flags = []
        if len(self.masters.addresses_and_bin) > 1:
            master_flags.append("--master_addresses={}".format(self.master_addrs))
        self.masters.start(master_flags, "8051")

        logging.info("Starting tservers")
        tserver_flags = ["--metrics_log_interval_ms=1000"]
        tserver_flags.append("--tserver_master_addrs={}".format(self.master_addrs))
        self.tservers.start(tserver_flags, "8050")

    # Collect the metrics cluster-wide and place them in the local directory
    # 'dir_name'. 'dir_name' must already exist.
    def collect_metrics(self, dir_name):
        metrics_dir = os.path.abspath(dir_name)
        masters_log_dir = os.path.join(metrics_dir, "masters")
        subprocess.check_output(["mkdir", "-p", masters_log_dir])
        self.masters.collect_metrics(masters_log_dir)

        tservers_log_dir = os.path.join(metrics_dir, "tservers")
        subprocess.check_output(["mkdir", "-p", tservers_log_dir])
        self.tservers.collect_metrics(tservers_log_dir)

    # Run a command, with cluster-specific environment variables set.
    def run_workload(self, cmd, timeout):
        try:
            # cmd = "timeout -k {} bash {}".format(timeout, cmd)
            logging.info("Running command: {}".format(cmd))
            out = subprocess.check_output(cmd, env=self.cluster_env)
            logging.info(out)
        except:
            logging.info("Done!")


    # Stop the Kudu processes on the remote hosts.
    def stop_servers(self):
        logging.info("Stopping servers")
        self.masters.stop()
        self.tservers.stop()


# Returns a paramiko client connected to `address`. Searches through `SECRETS`
# for the right credentials to use.
def connect_to_host(address):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if address in SECRETS.keys():
        logging.info("Reading secrets for {}".format(address))
        secret = SECRETS[address]

        # Get the private key password, if one exists.
        pkey_password = secret['pkey_password']
        if len(pkey_password) == 0:
            pkey_password = None

        # Generate a key object based on the private key file.
        key = paramiko.RSAKey.from_private_key_file( \
            secret['private_key_file'], password=pkey_password)

        logging.info("Logging in with user {}".format(secret['user']))
        password = secret['password']
        if len(password) == 0:
            password = None
        client.connect(hostname=address, username=secret['user'], password=password, pkey=key)
    else:
        logging.info("No secrets listed for address {}".format(address))
        client.connect(hostname=address)
    return client


# Returns whether or not the absoluate path 'dst' refers to a file according to
# the SFTP client.
def remote_file_exists(sftp_client, dst):
    logging.info("Checking for remote file {}".format(dst))
    try:
        sftp_client.stat(dst)
        logging.info("File already exists")
        return True
    except IOError:
        return False


# Sends a file if it doesn't already exist remotely.
# 'src' is the absolute path of the local file.
# 'dst' is the absolute path of the remote file.
def send_file(sftp_client, src, dst):
    if not remote_file_exists(sftp_client, dst):
        logging.info("Sending file {}".format(src))
        sftp_client.put(src, dst)


# Setting up a host entails sending over any necessary binary files,
# scripts, and running host setup scripts to install dependencies and such.
def setup_host(host_config):
    setup_script = host_config["setup_script"]
    address = host_config["address"]

    # Make a connection with the host.
    client = connect_to_host(address)
    sftp_client = client.open_sftp()

    # Create the remote bin dir.
    remote_bin_dir = host_config["bin_dir"]
    if not remote_file_exists(sftp_client, remote_bin_dir):
        exec_and_log(client, "sudo mkdir -p {}".format(remote_bin_dir))

    exec_and_log(client, "sudo chown -R {} {}".format(SECRETS[address]['user'], remote_bin_dir))

    # Send over the setup script and binaries, and assign the correct
    # permissions.
    remote_setup_script = os.path.join(remote_bin_dir, host_config["setup_script"])
    send_file(sftp_client, os.path.join(LOCAL_SCRIPTS_DIR, setup_script), remote_setup_script)
    exec_and_log(client, "sudo chmod +x {}".format(remote_setup_script))
    for bin_file in host_config["bin_files"]:
        remote_bin_file = os.path.join(remote_bin_dir, bin_file)
        send_file(sftp_client, os.path.join(BASE_OPTS["kudu_sbin_dir"], bin_file), \
            remote_bin_file)
        exec_and_log(client, "sudo chmod +x {}".format(remote_bin_file))

    # Run the setup script.
    exec_and_log(client, "sudo {}".format(remote_setup_script))

    # Close shop.
    sftp_client.close()
    client.close()


# Create and connect to the various servers of the cluster, running any
# necessary pre-requisite scripts to running workloads.
#
# Returns the configurations for relevant hosts and clusters, based on the
# cluster filter.
def load_configs(setup_yaml, cluster_filter):
    relevant_hosts = {}
    relevant_clusters = {}
    cluster_re = re.compile(cluster_filter)
    host_configs = setup_yaml["hosts"]

    # Pick out the clusters we actually want to set up.
    for cname, config in setup_yaml['clusters'].iteritems():
        if len(cluster_re.findall(cname)) == 0 or \
          cname in relevant_clusters.keys():
            logging.info("Skipping cluster: {}".format(cname))
            continue
        logging.info("Loading config for cluster: {}".format(cname))
        relevant_clusters[cname] = config

        # Pick out the hosts relevant to this cluster.
        for mname in config["masters"]["hosts"]:
            if mname not in relevant_hosts.keys():
                relevant_hosts[mname] = host_configs[mname]
        for tname in config["tservers"]["hosts"]:
            if tname not in relevant_hosts.keys():
                relevant_hosts[tname] = host_configs[tname]

    return relevant_hosts, relevant_clusters


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

    global SECRETS
    SECRETS = yaml.load(file(args.secrets_yaml_path))

    global BASE_OPTS
    BASE_OPTS = setup_yaml['base_opts']

    # Parse out which hosts and clusters we actually need.
    # TODO: make this less ugly.
    global HOST_CONFIGS
    HOST_CONFIGS, cluster_configs = load_configs(setup_yaml, args.cluster_filter)

    # Setup the relevant hosts.
    for hname, config in HOST_CONFIGS.iteritems():
        logging.info("Setting up host {}".format(hname))
        setup_host(config)

    # Setup the relevant clusters.
    clusters = []
    for cname, config in cluster_configs.iteritems():
        logging.info("Setting up cluster {}".format(cname))
        cluster = Cluster(config)
        cluster.setup_servers()
        clusters.append(( cname, cluster ))

    # Run each workload on each cluster.
    for wname, config in BASE_OPTS["workloads"].iteritems():
        logging.info("Running workload {}".format(wname))
        workload_script = os.path.join(WORKLOADS_DIR, config["script"])
        for cname, cluster in clusters:
            cluster.start_servers()
            cluster.run_workload(workload_script, config["max_execution_time"])
            cluster.stop_servers()

            # Create the appropriate directories for metrics.
            metrics_dir = os.path.join(LOCAL_LOGS_DIR, wname + time_suffix, cname)
            subprocess.check_output(["mkdir", "-p", metrics_dir])
            cluster.collect_metrics(metrics_dir)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
