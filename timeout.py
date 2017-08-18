# coding=utf-8
"""
Timeout Exception wrapper
"""

from concurrent.futures import ThreadPoolExecutor
from functools import wraps

import docker

from corfu_node import Node


def timeout(seconds=30, container_to_kill=None):
    """
    Decorator to timeout the annotated function.
    :param seconds: Time to timeout after.
    :param container_to_kill: If timeout kills the container with this name passed as parameter.
    :return:
    """

    def decorator(func):
        """
        Wraps the function.
        :param func: Function to be wrapped.
        :return:
        """

        def wrapper(*args, **kwargs):
            """
            Wrapper function.
            :param args: arguments passed to the annotated function.
            :param kwargs: kwargs passed to the annotated function.
            :return: Returns the result of the annotated function or TimeoutError if timed out.
            """
            executor = ThreadPoolExecutor(max_workers=1)
            future = executor.submit(func, *args)
            try:
                result = future.result(seconds)
            except Exception as error:
                # Remove the hung container if required.
                if container_to_kill is not None:
                    print("Killing container {} due to received error :{}.".format(container_to_kill, error))
                    Node.remove_container(docker.from_env(), container_to_kill)
                raise error
            return result

        return wraps(func)(wrapper)

    return decorator
