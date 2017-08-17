# coding=utf-8
"""
Corfu Cluster Object
"""
import json
import os
from concurrent.futures import ThreadPoolExecutor

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
    executor = ThreadPoolExecutor(max_workers=5)

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
        endpoints = set()
        endpoints.update(layout['layoutServers'])
        endpoints.update(layout['sequencers'])
        endpoints.update(layout['segments'][0]['stripes'][0]['logServers'])
        endpoints.update(layout['unresponsiveServers'])
        return endpoints

    def bootstrap_cluster(self, layout):
        """
        Bootstraps the cluster with the given layout using the cmdlet.
        :param layout: layout to bootstrap the cluster.
        """
        return self.client.containers.run(CORFU_IMAGE, ["sh", "-c", "corfu_bootstrap_cluster -l /tmp/layout"],
                                          name="orchestrator", remove=True, network=CORFU_DOCKER_NETWORK, tty=True,
                                          volumes={os.path.abspath(layout): {'bind': '/tmp/layout', 'mode': 'ro'}}) \
            .decode("utf-8")

    def get_layout(self, layout):
        """
        Prints the layout by querying one of the layout servers.
        :param layout: Layout to parse the layout server endpoints.
        """
        endpoints = set()
        endpoints.update(layout['layoutServers'])
        print(self.client.containers.run(CORFU_IMAGE,
                                         ["sh", "-c", "corfu_layouts -c "
                                          + ",".join(endpoints) + " query"],
                                         name="layout_getter", remove=True, network=CORFU_DOCKER_NETWORK, tty=True)
              .decode("utf-8"))

    def spawn_node(self, endpoint):
        """
        Spawn single node
        :param endpoint: Endpoint of the node.
        """
        ip = endpoint.split(':')[0]
        port = endpoint.split(':')[1]
        node = Node(client=self.client,
                    port=port,
                    address=ip)
        run_result = node.run_container()
        if run_result:
            print("Spawned container :" + Node.get_name_from_endpoint(endpoint))
        else:
            print("Container " + Node.get_name_from_endpoint(endpoint) + " already running.")

    def setup_cluster(self) -> object:
        """
        Deploys the cluster, bootstraps and fetches the committed layout.
        :return: Cluster instance.
        """
        endpoints = Cluster.get_endpoints(to_json(self.layout))

        future_list = []
        for endpoint in endpoints:
            future_list.append(self.executor.submit(self.spawn_node, endpoint))

        for future in future_list:
            future.result()

        self.bootstrap_cluster(self.layout)
        print("Cluster bootstrapped successfully.")
        return self

    def destroy_cluster(self):
        """
        Destroys all the nodes in the given layout.
        :return:
        """
        endpoints = Cluster.get_endpoints(to_json(self.layout))
        future_list = []
        for endpoint in endpoints:
            if not Node.check_if_container_exists(self.client, Node.get_name_from_endpoint(endpoint)):
                continue
            name = Node.get_name_from_endpoint(endpoint)
            future_list.append(self.executor.submit(Node.remove_container, self.client, name))

        for future in future_list:
            future.result()


def to_json(layout_file):
    """
    Converts the layout file to a json object
    :param layout_file:
    :return: Json object
    """
    return json.load(open(layout_file))
