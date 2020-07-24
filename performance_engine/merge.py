from typing import Callable, Hashable, Iterable, Iterator, List, Any, Tuple
import heapq


class Merger:
    """
    The responsibility of this class is to appropriately merge a set of Iterable objects, each of which has an
    index that can be identified by a common function
    """
    def __init__(self, key_fn: Callable):
        """
        :param Callable key_fn: The function to find the key of the provided values to merge
        """
        self.iterators = {}
        self.key_fn = key_fn

    def include(self, key: Hashable, iterator: Iterable) -> None:
        """
        Adds an iterator to the Merger's dictionary of iterators

        :param Hashable key: The key for the iterator
        :param Iterable iterator: The iterable object to add to the dictionary of iterators

        :return: None
        """
        self.iterators[key] = iter(iterator)

    def merge(self) -> Iterator[Tuple[Hashable, List[Any]]]:
        """
        Merge the list of iterables together then group them by their index.

        :return: Iterator[Hashable, List[Any]]: The iterator yielding the key of each group and its members
        """
        f = self.key_fn

        # Merge the Iterables together
        i = iter(heapq.merge(*list(self.iterators.values()), key=f))

        # Iterate over the results and group them by index
        v = next(i, None)
        while v is not None:
            grp = []
            key = f(v)
            while v is not None and key == f(v):
                grp.append(v)
                v = next(i, None)
            yield key, grp
