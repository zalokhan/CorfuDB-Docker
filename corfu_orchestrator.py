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

# Creates the cluster and returns a list of nodes.
node_list = cluster.setup_cluster()

# Fetches the layout from 192.168.0.5
pprint(cluster.get_layout(["192.168.0.5:9000"]))
# Executes a command on the given node.
print(node_list[1].execute_command("ls -la /var/corfu/"))

# Destroys the cluster by removing all the nodes.
cluster.destroy_cluster()
