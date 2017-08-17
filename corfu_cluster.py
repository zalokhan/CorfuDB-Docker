# coding=utf-8
"""
Corfu Cluster Object
"""
import json
import os

from docker import types
from docker.errors import NotFound

from constants import CORFU_DOCKER_NETWORK, SUBNET, GATEWAY
from constants import CORFU_IMAGE
from corfu_node import Node


class Cluster(object):
    """
    Creates a Corfu Cluster
    """
    layout = None
    client = None

    def __init__(self, layout, client) -> None:
        self.layout = layout
        self.client = client
        self.initialize_network()

    def initialize_network(self):
        """
        Creates the new docker network if not already present with the pre-configured subnet pool size.
        """
        try:
            self.client.networks.get(CORFU_DOCKER_NETWORK)
        except NotFound:
            ipam_pool = types.IPAMPool(
                subnet=SUBNET,
                gateway=GATEWAY
            )
            ipam_config = types.IPAMConfig(
                pool_configs=[ipam_pool]
            )
            self.client.networks.create(
                CORFU_DOCKER_NETWORK,
                driver="bridge",
                ipam=ipam_config
            )
            print("New docker network " + CORFU_DOCKER_NETWORK + " created.")

    @staticmethod
    def get_endpoints(layout):
        """
        Gives us the unique endpoints in the layout.
        :param layout: layout file
        :return: Set of endpoints.
        """
        layout_file = open(layout)
        data = json.load(layout_file)

        endpoints = set()
        endpoints.update(data['layoutServers'])
        endpoints.update(data['sequencers'])
        endpoints.update(data['segments'][0]['stripes'][0]['logServers'])
        endpoints.update(data['unresponsiveServers'])
        return endpoints

    def bootstrap_cluster(self, layout):
        """
        Bootstraps the cluster with the given layout using the cmdlet.
        :param layout: layout to bootstrap the cluster.
        """
        self.client.containers.run(CORFU_IMAGE, ["sh", "-c", "corfu_bootstrap_cluster -l /tmp/layout"],
                                   name="orchestrator", remove=True, network=CORFU_DOCKER_NETWORK, tty=True,
                                   volumes={os.path.abspath(layout): {'bind': '/tmp/layout', 'mode': 'ro'}})

    def get_layout(self, layout):
        """
        Prints the layout by querying one of the layout servers.
        :param layout: Layout to parse the layout server endpoints.
        """
        layout_file = open(layout)
        data = json.load(layout_file)
        endpoints = set()
        endpoints.update(data['layoutServers'])
        print(self.client.containers.run(CORFU_IMAGE,
                                         ["sh", "-c", "corfu_layouts -c "
                                          + ",".join(endpoints) + " query"],
                                         name="layout_getter", remove=True, network=CORFU_DOCKER_NETWORK, tty=True)
              .decode("utf-8"))

    def setup_cluster(self) -> object:
        """
        Deploys the cluster, bootstraps and fetches the committed layout.
        :return: Cluster instance.
        """
        endpoints = Cluster.get_endpoints(self.layout)

        for endpoint in endpoints:
            ip = endpoint.split(':')[0]
            port = endpoint.split(':')[1]
            node = Node(client=self.client,
                        port=port,
                        address=ip)
            run_result = node.run_container()
            if run_result:
                print("Spawned container :" + endpoint.replace(":", "_"))
            else:
                print("Container " + endpoint.replace(":", "_") + " already running.")

        self.bootstrap_cluster(self.layout)
        print("Cluster bootstrapped successfully.")
        print("Fetching committed layout...")
        self.get_layout(self.layout)
        return self
