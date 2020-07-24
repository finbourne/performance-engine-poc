from typing import Any

import jsonpickle


def serialise(deserialised_object: Any):
    """
    Takes an deserialised Python object (i.e. a Python class) and serialises it

    :param Any deserialised_object: The deserialised object to serialise into a string

    :return: str: The serialised object
    """
    return jsonpickle.encode(deserialised_object)


def deserialise(serialised_object: str, version: str):
    """
    Takes a serialised Python object

    :param str serialised_object: The string representing a serialised Python object
    :param str version: The version of the serialised object, this tells the deserialiser what
    logic to use to deserialise the object

    :return: Any: The deserialised object (i.e. a Python class)
    """
    return jsonpickle.decode(serialised_object)
