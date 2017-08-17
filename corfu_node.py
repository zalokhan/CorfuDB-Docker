# coding=utf-8
"""
Corfu Instance
"""
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

    container = None

    def __init__(self, client, port=DEFAULT_PORT, memory=False, log_path=DEFAULT_LOG_PATH, single=False,
                 address=DEFAULT_IP_ADDRESS) -> None:
        self.client = client
        self.address = address
        self.port = str(port)
        self.single = single
        self.memory = memory
        self.log_path = log_path
        self.endpoint = "_".join([address, port])

    def generate_run_command(self):
        """
        Generates the command to run the corfu process with the specified options.
        :return:
        """
        result = CORFU_SERVER_SCRIPT + " "
        result += ("-a " + self.address + " ")
        result += "-m " if self.memory else ("-l " + self.log_path + " ")
        result += "-s " if self.single else ""
        result += str(self.port)
        result += " >> " + DEFAULT_CONSOLE_LOG_PATH
        return result

    def run_container(self):
        """
        Runs the container with the server process as the starting process.
        Note: If the server crashes, the container stops. This helps in monitoring servers.
        :return:
        """
        if not self.check_name_unique(self.endpoint):
            return None
        container = self.client.containers.run(image=CORFU_IMAGE,
                                               command=["sh", "-c", self.generate_run_command()],
                                               name=self.endpoint,
                                               detach=True,
                                               tty=True)
        self.client.networks.get(CORFU_DOCKER_NETWORK).connect(container, ipv4_address=self.address)
        return container

    def check_name_unique(self, name) -> bool:
        """
        Checks if this container name is already taken.
        :param name: Name of container.
        :return: True if name is unique else False.
        """
        for container in self.client.containers.list():
            if name == container.name:
                return False
        return True

    def remove_container(self):
        """
        Removes the container forcefully.
        """
        self.container.remove(force=True)

    def start_corfu(self):
        """
        Starts a stopped container.
        """
        self.container.start()

    def stop_corfu(self):
        """
        Stops the corfu server container.
        """
        self.container.kill()
