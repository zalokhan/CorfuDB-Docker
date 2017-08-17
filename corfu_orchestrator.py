# coding=utf-8
"""
Corfu docker cluster orchestrator.
"""
import sys
import docker
from corfu_cluster import Cluster

assert len(sys.argv) > 1

client = docker.from_env()
cluster = Cluster(sys.argv[1], client)
cluster.setup_cluster()
