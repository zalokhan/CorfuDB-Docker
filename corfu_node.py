# coding=utf-8
"""
Corfu Instance
"""
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
    endpoint = None
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
        :return: Returns the output of the command.
        """
        container = self.client.containers.get(self.endpoint)
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

    def remove(self):
        """
        Removes the docker container.
        """
        return self.unpause().remove(force=True)

    def pause(self):
        """
        Pauses the container.
        :return: Container instance.
        """
        container = self.client.containers.get(self.endpoint)
        if container.status == "running":
            container.pause()
        return self.client.containers.get(self.endpoint)

    def unpause(self):
        """
        Un-pauses this node.
        :return: Returns node if container present else throws NotFound Exception.
        """
        container = self.client.containers.get(self.endpoint)
        if container.status != "paused":
            return container
        container.unpause()
        # Re-fetch as status is updated.
        return self.client.containers.get(self.endpoint)

    def start_corfu(self):
        """
        Starts a stopped container with the corfu process.
        """
        self.unpause().start()

    def stop_corfu(self):
        """
        Stops the corfu container.
        """
        self.unpause().kill()
