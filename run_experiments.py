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
import shutil
import subprocess
import sys
import time
import urllib2
import yaml

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


class Servers(object):
    def __init__(self, server_config, secrets):
        self.config = server_config
        self.secrets = secrets
        self.addresses = self.config['addresses']
        self.setup_script = None
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
    # Create necessary directories on each server and send over any scripts
    # that may need running.
    def setup(self):
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
                    secret['private_key_file'], password=secret['password'])
                client.connect(hostname=addr, username=secret['user'], pkey=key)
            else:
                logging.info("No secrets listed for server")
                client.connect(hostname=addr)
            logging.info("Connected to server {}".format(addr))

            # Create any directories we might need.
            stdin, stdout, stderr = client.exec_command("mkdir -p {}".format(" ".join(self.dirs)))
            self.clients[addr] = client

        # Send over the setup script and run setup.
        if self.setup_script:
            self.distribute_file(self.setup_script, self.remote_working_dir)

    # Runs the given binary file, located in the remote working directories,
    # across all servers.
    def start(self, bin_file, flags, port):
        for addr in self.addresses:
            logging.info("Starting daemon on server {}".format(addr))
            client = self.clients[addr]
            stdin, stdout, stderr = client.exec_command(
                "{} {}".format(self.remote_file(bin_file)), flags)

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

    # Send a local file at 'src' to the servers, placing it in the remote
    # working directory as the file 'dst'.
    # If the file already exists on a server, that server will be skipped.
    def distribute_file(self, src, dst):
        logging.info("Distributing file {}".format(src));
        remote_file = self.remote_file(dst)
        for addr in self.addresses:
            sftp_client = self.clients[addr].open_sftp()
            try:
                sftp_client.stat(remote_file)
                logging.info("File already exists on {}".format(addr))
            except IOError:
                sftp_client.put(src, remote_file)
            sftp_client.close()

    # Returns the name of a remote filename.
    def remote_file(self, filename):
        return os.path.join(self.remote_working_dir, filename)

    # Cleanup each cluster and close the clients for each one.
    def cleanup(self):
        for addr in self.addresses:
            client = self.clients[addr]
            stdin, stdout, stderr = client.exec_command("pkill kudu-master")
            stdin, stdout, stderr = client.exec_command("pkill kudu-tserver")
            stdin, stdout, stderr = client.exec_command("rm -rf {}".format(" ".join(self.dirs)))
            client.close()

    # Returns the requested metrics for the servers.
    def collect_metrics(self, metrics):
        return


class Cluster:
    def __init__(self, base_opts, config, secrets):
        self.config = config
        self.masters = Servers(self.config['masters'], secrets)
        self.tservers = Servers(self.config['tservers'], secrets)
        self.master_flags = ",".join(self.masters.addresses)

    # Sets up the masters and tablet servers, and distributes the binaries
    # necessary to start the cluster.
    def setup_servers():
        self.masters.setup()
        self.tservers.setup()
        self.masters.distribute_file(
            os.path.join(base_opts['kudu_sbin_dir'], "kudu-master"), "kudu-master")
        self.tservers.distribute_file(
            os.path.join(base_opts['kudu_sbin_dir'], "kudu-tserver"), "kudu-tserver")

    # Starts the servers and waits for them to come online.
    def start_servers():
        self.masters.start("kudu-master", "", "8051")
        self.tservers.start("kudu-tserver", "", "8050")

    # Kills the Kudu processes on the remote hosts.
    def kill_servers():
        logging.info("cleaning up")
        self.masters.cleanup()
        self.tservers.cleanup()


def start_servers(config):
    logging.info("Starting servers...")

    ts_bin = os.path.join(exp.config['kudu_sbin_dir'], "kudu-tserver")
    master_bin = os.path.join(exp.config['kudu_sbin_dir'], "kudu-master")
    if not os.path.exists(exp.log_dir):
        os.makedirs(exp.log_dir)
    try:
        ts_proc = subprocess.Popen(
            [ts_bin] + exp.flags("ts_flags") + ["--log_dir", exp.log_dir],
            stderr=subprocess.STDOUT,
            stdout=file(os.path.join(exp.log_dir, "ts.stderr"), "w"))
        master_proc = subprocess.Popen(
            [master_bin] + exp.flags("master_flags") + [ "--log_dir", exp.log_dir],
            stderr=subprocess.STDOUT,
            stdout=file(os.path.join(exp.log_dir, "master.stderr"), "w"))
    except OSError, e:
        logging.fatal("Failed to start kudu servers: %s", e)
        if '/' not in ts_bin:
            logging.fatal("Make sure they are on your $PATH, or configure kudu_sbin_dir")
        else:
            logging.fatal("Tried path: %s", ts_bin)
            sys.exit(1)
    # Wait for servers to start.
    for x in xrange(60):
        try:
            logging.info("Waiting for servers to come up...")
            urllib2.urlopen("http://localhost:8050/").read()
            urllib2.urlopen("http://localhost:8051/").read()
            break
        except:
            if x == 59:
                raise
            time.sleep(1)
            pass
    return (master_proc, ts_proc)


def dump_ts_info(exp, suffix):
    for page, fname in [("rpcz", "rpcz"),
                ("metrics?include_raw_histograms=1", "metrics"),
                ("mem-trackers?raw", "mem-trackers")]:
        fname = "%s-%s.txt" % (fname, suffix)
        dst = file("")
        try:
            shutil.copyfileobj(urllib2.urlopen("http://localhost:8050/" + page), dst)
        except:
            logging.fatal("Failed to fetch tablet server info. TS may have crashed.")
            logging.fatal("Check for FATAL log files in %s", exp.log_dir)
            sys.exit(1)

def run_experiment(exp):
    logging.info("Running experiment %s" % exp.dimensions)
    stop_servers()
    remove_data()
    # Sync file system so that there isn't any dirty data left over from prior runs
    # sitting in buffer caches.
    subprocess.check_call(["sync"])
    start_servers(exp)
    for yaml_entry in exp.config['ycsb_workloads']:
        phase, workload = yaml_entry['phase'], yaml_entry['workload']
        run_ycsb(exp, phase, workload)
        dump_ts_info(exp, "after-%s-%s" % (phase, workload))
    stop_servers()
    remove_data()

def load_clusters(setup_yaml, secrets_yaml):
    base_opts = setup_yaml['base_opts']
    clusters = {}
    for name, config in setup_yaml['clusters'].iteritems():
        logging.info("Connecting to cluster: {}".format(name))
        clusters[name] = Cluster(base_opts, config, secrets_yaml)
    return clusters, base_opts

def run_all(clusters, opts):
    opts['workloads']
    for c in clusters:
        run_experiment(c)

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
    args = p.parse_args()
    setup_yaml = yaml.load(file(args.setup_yaml_path))
    secrets_yaml = yaml.load(file(args.secrets_yaml_path))
    clusters, opts = load_clusters(setup_yaml, secrets_yaml)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
