import pickle
import os
from typing import Callable

from lusidtools.lpt import lse
from lusidtools.lpt.either import Either


class CachingApi:
    """
    Private class.
    Replaces the 'call' element in the
    Original lse - it redirects any
    LUSID api call through to the interceptor
    """

    class Interceptor:
        """

        """
        def __init__(self, original, wrapper):
            self.original = original
            self.wrapper = wrapper

        def __getattr__(self, name):
            fn = getattr(self.original,name)

            def envelope(*args,**kwargs):
                return self.wrapper(name,fn,*args,**kwargs)

            return envelope
    
    def __init__(self, filename: str, *args, **kwargs):
        """
        :param str filename: The name of the file containing the cached API calls
        """
        self.filename = os.path.join(kwargs.get("folder","cached"), filename)
        try:
            with open(self.filename, 'rb') as f:
                # Load the cached API calls
                self.calls = pickle.load(f)
        except:
            # If the file can not be read, there are no cached API calls
            self.calls = []

        # If there are no cached API calls then set the record_mode to True to record the next API calls
        self.record_mode = len(self.calls) == 0

    def __enter__(self):
        """
        The __enter__ method is called when using Python's built in "with" statement e.g. with CachingApi() as api.

        :return:
        """

        # If record_mode is true connect to LUSID
        if self.record_mode:
            self.api = lse.connect()
        else:
            # Otherwise
            import lusid
            self.api = lse.ExtendedAPI({}, lusid.ApiClient(lusid.Configuration()), lusid)

        # Replace the original 'call' with our interceptor
        self.api.call = self.Interceptor(
            original=self.api.call,
            wrapper=self.recorder if self.record_mode else self.reader
        )

        return self.api

    def __exit__(self, type, value, traceback):
        """
        This is called upon exiting a "with" statement

        :param type:
        :param value:
        :param traceback:
        :return:
        """
        if self.record_mode:
            with open(self.filename, 'wb') as f:
                 pickle.dump(self.calls, f)

    def recorder(self, name: str, func: Callable, *args, **kwargs):
        """
        The responsibility of this function is to act as a wrapper around an API call and record the result of the
        call

        :param str name: The name of the API call e.g. BuildTransactions
        :param Callable func: The API call

        :return: Callable func: The API call
        """
        def success(result):
            nonlocal self
            self.calls.append((name, result))
            return result

        return func(*args, **kwargs).bind(success)

    def reader(self, name: str, func: Callable, *args, **kwargs):
        """
        The responsibility of this function is to act as a wrapper around an API call and read the result of the
        call

        :param str name: The name of the API call e.g. BuildTransactions
        :param Callable func: The API call

        :return:
        """
        try:
            fn, result = self.calls.pop(0)
            assert fn == name
            return Either.Right(result)

        except Exception as e:
            print(f"ERROR READING FROM CACHE:{self.filename}\n"
                   "DELETE THE CACHE FILE AND RETRY")
            raise e
