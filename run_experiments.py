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
PASSWORD = "awong"

class Cluster:
    class Server(object):
        def __init__(self, config):
            self.config = config
            self.addresses = self.config['addresses']
            self.setup = None
            if len(self.config['setup_script']) > 0:
                self.setup = os.path.abspath(self.config['setup_script'])
            self.flags = []
            self.dirs = []
            for name, path in self.config['dir_flags'].iteritems():
                self.flags.append("--{}={}".format(name, path))
                self.dirs.extend(path.split(','))

        # Create the directories and run any setup.
        def setup(self):
            # Create client.
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key = paramiko.RSAKey.from_private_key_file("")
            client.connect(hostname="", username="", pkey=key)

            # Check if there exists the binary already.
            # TODO: send over the setup script and run setup.
            return

    def __init__(self, config):
        self.config = config
        self.cred_file = None
        if len(self.config['cred_file']) > 0:
            self.cred_file = os.path.abspath(self.config['cred_file'])
        self.masters = Server(self.config['masters'])
        self.tservers = Server(self.config['tservers'])

    def start_servers():
        return

    @property
    def results_dir(self):
        path = os.path.join(BASE_DIR, "results")
        for dim_name, dim_val in sorted(self.dimensions.iteritems()):
            path = os.path.join(path, "%s=%s" % (dim_name, dim_val))
        return path
    
    @property
    def log_dir(self):
        return os.path.join(self.results_dir, "logs")

    def flags(self, config_key):
        ret = []
        for flag, value in self.config[config_key].iteritems():
        if type(value) == list:
            value = ",".join(value)
        ret.append("--%s=%s" % (flag, value))
        return ret

    def workload_path(self, workload):
        return os.path.join(self.workload_dir, workload)

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

def stop_servers():
    subprocess.call(["pkill", "kudu-tserver"])
    subprocess.call(["pkill", "kudu-master"])


def remove_data():
    for d in DATA_DIRS:
        rmr(d)
        os.makedirs(d)

def rmr(dir):
    if os.path.exists(dir):
        logging.info("Removing data from %s" % dir)
        shutil.rmtree(dir)


def dump_ts_info(exp, suffix):
    for page, fname in [("rpcz", "rpcz"),
                ("metrics?include_raw_histograms=1", "metrics"),
                ("mem-trackers?raw", "mem-trackers")]:
        fname = "%s-%s.txt" % (fname, suffix)
        dst = file(os.path.join(exp.results_dir, fname), "w")
        try:
            shutil.copyfileobj(urllib2.urlopen("http://localhost:8050/" + page), dst)
        except:
            logging.fatal("Failed to fetch tablet server info. TS may have crashed.")
            logging.fatal("Check for FATAL log files in %s", exp.log_dir)
            sys.exit(1)

def run_experiment(exp):
    if os.path.exists(exp.results_dir):
        logging.info("Skipping experiment %s (results dir already exists)" % exp.dimensions)
        return
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

def generate_dimension_combinations(setup_yaml):
    combos = [{}]
    for dim_name, dim_values in setup_yaml['dimensions'].iteritems():
        new_combos = []
        for c in combos:
            for dim_val in dim_values:
                new_combo = c.copy()
                new_combo[dim_name] = dim_val
                new_combos.append(new_combo)
        combos = new_combos
    return combos

def load_clusters(setup_yaml):
    base_opts = setup_yaml['base_opts']
    clusters = {}
    for name, config in setup_yaml['clusters'].iteritems():
        clusters[name] = Cluster(base_opts, config)
    return clusters, base_opts


def run_all(clusters, opts):
    for exp in exps:
        run_experiment(exp)


def main():
    p = argparse.ArgumentParser("Run a set of experiments")
    p.add_argument("--setup-yaml",
        dest="setup_yaml_path",
        type=str,
        help="YAML file describing experiments to run",
        default=os.path.join(BASE_DIR, "setup.yaml"))
    args = p.parse_args()
    setup_yaml = yaml.load(file(args.setup_yaml_path))
    clusters, opts = load_clusters(setup_yaml)
    run_all(clusters, opts)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
