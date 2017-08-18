# coding=utf-8
"""
Corfu docker cluster orchestrator.
"""
import sys
from pprint import pprint

import docker

from corfu_cluster import Cluster

assert len(sys.argv) > 1

client = docker.from_env()
cluster = Cluster(sys.argv[1], client)
cluster.setup_cluster()
pprint(cluster.get_layout(["192.168.0.5:9000"]))
cluster.destroy_cluster()
