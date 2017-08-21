# coding=utf-8
"""
Corfu Instance
"""
from docker.errors import NotFound
from requests.packages.urllib3.response import HTTPResponse

from constants import CORFU_SERVER_SCRIPT, CORFU_IMAGE, CORFU_DOCKER_NETWORK
from constants import DEFAULT_PORT, DEFAULT_CONSOLE_LOG_PATH, DEFAULT_LOG_PATH, DEFAULT_IP_ADDRESS


class Node(object):
    """
    Creates a Corfu Node
    """
    client = None

    address = DEFAULT_IP_ADDRESS
    port = DEFAULT_PORT
    single = False
    memory = False
    log_path = DEFAULT_LOG_PATH
    endpoint = "_".join(["corfu", address, port])
    logging_level = "INFO"

    def __init__(self, client, port=DEFAULT_PORT, memory=False, log_path=DEFAULT_LOG_PATH, single=False,
                 address=DEFAULT_IP_ADDRESS, logging_level="INFO") -> None:
        self.client = client
        self.address = address
        self.port = str(port)
        self.single = single
        self.memory = memory
        self.log_path = log_path
        self.logging_level = logging_level
        self.endpoint = "_".join([address, port])

    def generate_run_command(self):
        """
        Generates the command to run the corfu process with the specified options.
        :return:
        """
        result = "{} ".format(CORFU_SERVER_SCRIPT)
        result += "-a {} ".format(self.address)
        result += "-m " if self.memory else "-l {} ".format(self.log_path)
        result += "-s " if self.single else ""
        result += str(self.port)
        result += " >> {}".format(DEFAULT_CONSOLE_LOG_PATH)
        return result

    def run_container(self):
        """
        Runs the container with the server process as the starting process.
        Note: If the server crashes, the container stops. This helps in monitoring servers.
        :return:
        """
        if Node.check_if_container_exists(self.client, self.endpoint):
            return None
        container = self.client.containers.run(image=CORFU_IMAGE,
                                               command=["sh", "-c", self.generate_run_command()],
                                               name=self.endpoint,
                                               detach=True,
                                               tty=True)
        self.client.networks.get(CORFU_DOCKER_NETWORK).connect(container, ipv4_address=self.address)
        return container

    def execute_command(self, command):
        """
        Executes the command on this node
        :param command: Command to be executed.
        :return: Returns the output of the command. None if node absent.
        """
        container = Node.check_if_container_exists(self.client, self.endpoint)
        if container is None:
            return None
        return container.exec_run(["sh", "-c", command]).decode("utf-8")

    def save_data_log(self, path=None):
        """
        Saves the data log of the container endpoint to the specified path.
        :param path: Path to save the data logs.
        :return:
        """
        if not path:
            path = "/tmp/{}_data.tar".format(self.endpoint)
        # HTTPResponse archive from container.
        data_log = HTTPResponse(self.client.containers.get(self.endpoint).get_archive("/var/corfu")[0])

        data_log_file = open(path, mode="wb")
        data_log_file.write(data_log.data)
        data_log_file.close()
        return

    def save_console_log(self, path=None):
        """
        Saves the console log of the container to the specified path.
        :param path: Path to save the console log.
        :return:
        """
        if not path:
            path = "/tmp/{}_console_log.tar".format(self.endpoint)
        # HTTPResponse archive from container.
        console_log = HTTPResponse(self.client.containers.get(self.endpoint).get_archive("/var/log/corfu.9000.log*")[0])

        console_log_file = open(path, mode="wb")
        console_log_file.write(console_log.data)
        console_log_file.close()
        return

    @staticmethod
    def check_if_container_exists(client, name) -> object:
        """
        Checks if container with this name exists.
        :param client: Client to connect to.
        :param name: Name of container.
        :return: Container if found else None.
        """
        try:
            return client.containers.get(name)
        except NotFound as error:
            return None

    @staticmethod
    def remove_container(client, name):
        """
        Removes the container forcefully.
        """
        container = Node.unpause_container(client, name)
        if container is not None:
            container.remove(force=True)

    @staticmethod
    def unpause_container(client, name):
        """
        Unpauses container if already paused
        """
        if Node.check_if_container_exists(client, name):
            container = client.containers.get(name)
            if container.status != "paused":
                return container
            container.unpause()
            return client.containers.get(name)
        return None

    @staticmethod
    def start_corfu(client, name):
        """
        Starts a stopped container.
        """
        container = Node.unpause_container(client, name)
        container.start()

    @staticmethod
    def stop_corfu(client, name):
        """
        Stops the corfu server container.
        """
        container = Node.unpause_container(client, name)
        container.kill()

    @staticmethod
    def get_name_from_endpoint(endpoint):
        """
        Returns the container name from the node endpoint
        :param endpoint: Endpoint of the node.
        :return: Container name
        """
        return endpoint.replace(":", "_")
