# coding=utf-8
"""
Corfu Cluster Object
"""
import json
import os
from concurrent.futures import ThreadPoolExecutor

from docker import types
from docker.errors import NotFound

from constants import CMDLET_LAYOUT_QUERY, CMDLET_CLUSTER_BOOTSTRAP
from constants import CORFU_DOCKER_NETWORK, SUBNET, GATEWAY
from constants import CORFU_IMAGE
from corfu_node import Node
from timeout import timeout


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

    @timeout(seconds=60, container_to_kill=CMDLET_CLUSTER_BOOTSTRAP)
    def bootstrap_cluster(self, layout):
        """
        Bootstraps the cluster with the given layout using the cmdlet.
        :param layout: layout to bootstrap the cluster.
        """
        return self.client.containers.run(CORFU_IMAGE, ["sh", "-c", "corfu_bootstrap_cluster -l /tmp/layout"],
                                          name=CMDLET_CLUSTER_BOOTSTRAP, remove=True, network=CORFU_DOCKER_NETWORK,
                                          tty=True,
                                          volumes={os.path.abspath(layout): {'bind': '/tmp/layout', 'mode': 'ro'}}) \
            .decode("utf-8")

    @timeout(container_to_kill=CMDLET_LAYOUT_QUERY)
    def get_layout(self, endpoints):
        """
        Prints the layout by querying one of the layout servers.
        This will get stuck in a deadlock if the endpoint does not exist.
        :param endpoints: List of endpoints to query layout from.
        """
        # Assert if endpoints is a list
        assert isinstance(endpoints, list)
        output = self.client.containers.run(CORFU_IMAGE,
                                            ["sh", "-c", "corfu_layouts -c " + ",".join(endpoints) + " query"],
                                            name=CMDLET_LAYOUT_QUERY, remove=True, network=CORFU_DOCKER_NETWORK,
                                            tty=True) \
            .decode("utf-8")
        # Clean this hack. This is to remove the Warning... and errors (if any) of the cmdlet.
        output = output[output.find('{'):]
        return json.loads(output)

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
            print("Spawned container :{}".format(Node.get_name_from_endpoint(endpoint)))
        else:
            print("Container {} already running.".format(Node.get_name_from_endpoint(endpoint)))
        return node

    def setup_cluster(self) -> list:
        """
        Deploys the cluster, bootstraps and fetches the committed layout.
        :return: The list of nodes spawned.
        """
        endpoints = Cluster.get_endpoints(to_json(self.layout))

        future_list = []
        node_list = []
        for endpoint in endpoints:
            future_list.append(self.executor.submit(self.spawn_node, endpoint))

        for future in future_list:
            node_list.append(future.result())

        self.bootstrap_cluster(self.layout)
        print("Cluster setup successful.")
        return node_list

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
        print("Cluster destroyed.")


def to_json(layout_file):
    """
    Converts the layout file to a json object
    :param layout_file:
    :return: Json object
    """
    return json.load(open(layout_file))
