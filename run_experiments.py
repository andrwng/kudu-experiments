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
        # TODO: Make this less ugly.
        self.addresses_and_bin = [(HOST_CONFIGS[host]["address"], HOST_CONFIGS[host]["bin_dir"]) \
            for host in self.config["hosts"]]

        # Set the log and fs directory flags.
        for flag_name, path in self.config['dir_flags'].iteritems():
            self.dir_flags.append("--{}={}".format(flag_name, path))
            self.dirs.extend(path.split(','))
        self.remote_log_dir = self.config["dir_flags"]["log_dir"]

        logging.info("dir_flags: {}, dirs: {}".format(self.dir_flags, self.dirs))
        logging.info("addrs: {}".format(self.addresses_and_bin))

    # Create necessary directories on each server and send over any scripts or
    # binaries that may need running.
    def setup(self):
        for addr, _ in self.addresses_and_bin:
            logging.info("Connecting to server {}".format(addr))

            # Create an SSH client for this address.
            client = connect_to_host(addr)

            # Destroy and recreate any directories we might need.
            exec_and_log(client, "sudo rm -rf {}".format(" ".join(self.dirs)))
            exec_and_log(client, "sudo mkdir -p {}".format(" ".join(self.dirs)))
            exec_and_log(client, "sudo chown -R {} {}".format( \
                SECRETS[addr]['user'], " ".join(self.dirs)))
            client.close()

    # Runs the binary files across all servers.
    def start(self, server_flags, port, polling_interval=None):
        extra_flags = ["--block_manager=log", "--trusted_subnets=0.0.0.0/0"]
        flags_str = " ".join(self.dir_flags + server_flags + extra_flags)
        if "flags" in self.config:
            flags_str += " " + " ".join(self.config["flags"])
        if polling_interval:
          flags_str += " --unlock_experimental_flags=true " + \
              " --diagnostics_log_stack_traces_interval_ms={}".format(polling_interval * 1000)
        # Some extra flags need to be added per server; namely:
        # - the advertised address for RPCs
        # - the KUDU_HOME directory for each server
        for addr, bin_dir in self.addresses_and_bin:
            logging.info("Starting daemon on server {}".format(addr))
            cmd = "env KUDU_HOME={} {} {} {}".format(bin_dir, os.path.join(bin_dir, self.bin_type),
                flags_str, "--rpc_advertised_addresses={}".format(addr))
            logging.info(cmd)
            client = connect_to_host(addr)
            stdin, stdout, stderr = client.exec_command(cmd)
            logging.info("Waiting a bit for startup")
            time.sleep(10)
            client.close()

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
            client = connect_to_host(addr)
            sftp_client = client.open_sftp()
            for filename in sftp_client.listdir(self.remote_log_dir):
                if len(metrics_regex.findall(filename)) > 0:
                    logging.info("Getting remote file: {}".format(filename))
                    remote_metrics_log = os.path.join(self.remote_log_dir, filename)
                    local_metrics_log = os.path.join(local_metrics_dir, filename)
                    sftp_client.get(remote_metrics_log, local_metrics_log)
            sftp_client.close()
            client.close()

    # Stop each node.
    def stop(self):
        for addr, _ in self.addresses_and_bin:
            client = connect_to_host(addr)
            exec_and_log(client, "sudo pkill {}".format(self.bin_type))
            client.close()

    # Delete the server's contents.
    def cleanup(self):
        for addr, _ in self.addresses_and_bin:
            client = connect_to_host(addr)
            exec_and_log(client, "rm -rf {}".format(" ".join(self.dirs)))
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
        if type(self.config['tservers']) is dict:
            self.tservers = [Servers("kudu-tserver", self.config['tservers'])]
        else:
            self.tservers = [Servers("kudu-tserver", ts_config) \
                for ts_config in self.config['tservers']]
        self.masters = Servers("kudu-master", self.config['masters'])
        self.master_addrs = ",".join(["{}:7051".format(a) for a, _ in self.masters.addresses_and_bin])
        self.cluster_env = os.environ.copy()
        self.cluster_env["KUDU_MASTERS"] = self.master_addrs
        self.cluster_env["PATH"] = BASE_OPTS["kudu_sbin_dir"] + os.pathsep + self.cluster_env["PATH"]

    # Set up the masters and tablet servers, creating the appropriate
    # directories necessary to start the cluster.
    def setup_servers(self):
        self.masters.setup()
        for ts in self.tservers:
            ts.setup()

    # Start the servers and wait for them to come online.
    def start_servers(self, tserver_polling_interval=None):
        logging.info("Starting masters")
        master_flags = []
        if len(self.masters.addresses_and_bin) > 1:
            master_flags.append("--master_addresses={}".format(self.master_addrs))
        self.masters.start(master_flags, "8051")

        logging.info("Starting tservers")
        tserver_flags = ["--tserver_master_addrs={}".format(self.master_addrs)]
        for ts in self.tservers:
            ts.start(tserver_flags, "8050", tserver_polling_interval)

    # Collect the metrics cluster-wide and place them in the local directory
    # 'dir_name'. 'dir_name' must already exist.
    def collect_metrics(self, dir_name):
        metrics_dir = os.path.abspath(dir_name)
        masters_log_dir = os.path.join(metrics_dir, "masters")
        subprocess.check_output(["mkdir", "-p", masters_log_dir])
        self.masters.collect_metrics(masters_log_dir)

        tservers_log_dir = os.path.join(metrics_dir, "tservers")
        subprocess.check_output(["mkdir", "-p", tservers_log_dir])
        for ts in self.tservers:
            ts.collect_metrics(tservers_log_dir)

    # Run a command, with cluster-specific environment variables set.
    def run_workload(self, cmd, timeout=None):
        try:
            env = self.cluster_env
            if timeout:
                env["WORKLOAD_TIMEOUT"] = str(timeout)
            logging.info("Running command: {}".format(cmd))
            out = subprocess.check_output(cmd, env=env)
            logging.info(out)
        except Exception, e:
            logging.info("Exitted with {}".format(e))


    # Stop the Kudu processes on the remote hosts.
    def stop_servers(self):
        logging.info("Stopping servers")
        self.masters.stop()
        for ts in self.tservers:
            ts.stop()


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


# Sends over a directory recursively.
# 'src' is the absolute path of the local directory.
# 'dst' is the absolute path of the remote directory.
def send_recursively(sftp_client, src, dst):
    to_copy = [(src, dst)]
    while len(to_copy) > 0:
        local, remote = to_copy.pop()
        if os.path.isdir(local):
            if remote_file_exists(sftp_client, remote):
                continue
            # Create the directory remotely.
            # Iterate through the children of the local directory and send the contents.
            sftp_client.mkdir(remote)
            logging.info("creating remote dir: {}".format(remote))
            for e in os.listdir(local):
                to_copy.append((os.path.join(local, e), os.path.join(remote, e)))
        else:
            if remote_file_exists(sftp_client, remote):
                continue
            logging.info("sending file {} to {}".format(local, remote))
            send_file(sftp_client, local, remote)

# Setting up a host entails sending over any necessary binary files,
# scripts, and running host setup scripts to install dependencies and such.
def setup_host(host_config, resend_bins):
    setup_script = host_config["setup_script"]
    address = host_config["address"]
    setup_env_str = ""
    if "setup_env" in host_config.keys():
        setup_env = host_config["setup_env"]
        setup_env_str = "env " + " ".join(["{}={}".format(k, v) for k, v in setup_env.iteritems()])

    # Make a connection with the host.
    client = connect_to_host(address)
    sftp_client = client.open_sftp()

    # Create the remote bin dir, wiping it if requested.
    remote_bin_dir = host_config["bin_dir"]
    if resend_bins:
        exec_and_log(client, "sudo rm -rf {}".format(remote_bin_dir))

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
    exec_and_log(client, "{} sudo -E {}".format(setup_env_str, remote_setup_script))

    # Send over the Kudu /www files.
    send_recursively(sftp_client, os.path.join(BASE_OPTS["kudu_home"], "www"), \
                     os.path.join(remote_bin_dir, "www"))

    # Close shop.
    sftp_client.close()
    client.close()


# Filters out the configurations for relevant hosts and clusters, based on the
# cluster filter.
#
# Returns ({ host_name:String : host_yaml:Dict[String,String] },
#          { cluster_name:String : cluster_yaml:Dict[String,String] })
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
        if type(config["tservers"]) is dict:
            for tname in config["tservers"]["hosts"]:
                if tname not in relevant_hosts.keys():
                    relevant_hosts[tname] = host_configs[tname]
        else:
            for ts_config in config["tservers"]:
                for tname in ts_config["hosts"]:
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
    p.add_argument("--resend-bins",
        dest="resend_bins",
        action="store_true",
        help="Whether to wipe the current binaries and resend them. Helpful if updating setup scripts")
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

    # Setup the relevant hosts, sending over Kudu binaries and running any
    # necessary bootstrapping scripts.
    for hname, config in HOST_CONFIGS.iteritems():
        logging.info("Setting up host {}".format(hname))
        setup_host(config, args.resend_bins)

    # Initialize the specified clusters. Note that this doesn't make any
    # connections; it just sets up in-memory state.
    clusters = []
    for cname, config in cluster_configs.iteritems():
        logging.info("Setting up cluster {}".format(cname))
        cluster = Cluster(config)
        clusters.append(( cname, cluster ))

    # Run each workload on each cluster.
    time_suffix = datetime.datetime.now().strftime("-%Y-%m-%d_%H%M")
    for wname, config in BASE_OPTS["workloads"].iteritems():
        logging.info("Running workload {}".format(wname))
        workload_script = os.path.join(WORKLOADS_DIR, config["script"])
        for cname, cluster in clusters:
            # First, stop any Kudu processes that might currently be running.
            cluster.stop_servers()

            # Set up the servers, wiping any existing data, and creating the
            # remote Kudu directories.
            cluster.setup_servers()

            # Run the Kudu binaries.
            cluster.start_servers(30)

            # Runs the workload locally, adding the KUDU_HOME and
            # WORKLOAD_TIMEOUT environment variables.
            cluster.run_workload(workload_script, config["max_execution_time"])

            # Once the workload finishes, stop the Kudu processes.
            cluster.stop_servers()

            # Create the appropriate directories for metrics and fetch them.
            metrics_dir = os.path.join(LOCAL_LOGS_DIR, wname + time_suffix, cname)
            subprocess.check_output(["mkdir", "-p", metrics_dir])
            cluster.collect_metrics(metrics_dir)



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
