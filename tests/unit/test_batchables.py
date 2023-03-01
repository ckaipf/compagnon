import unittest

from compagnon.domain import model
from compagnon.service_layer import batchables

# from typing


class Batachable(unittest.TestCase):
    def setUp(self):
        self.so_many_bubbles = 10000
        self.bubble_multishooter_batch_size = 100
        self.bubbles = set([x for x in range(10000)])
        self.batches = []

    def bubble_popper(self, batch_size: int) -> bool:
        for _ in range(batch_size):
            if self.bubbles:
                self.bubbles.pop()
                finished = False
            else:
                finished = True
        self.batches.append("I'm a batch")
        return finished

    def test_bubble_popper(self):
        assert not self.batches
        self.bubble_popper(10)
        assert len(self.bubbles) == self.so_many_bubbles - 10
        assert len(self.batches) == 1

    def test_batched_bubble_popper(self):
        @batchables.batched(batch_size=self.bubble_multishooter_batch_size)
        def batch_bubble_popper(**kwargs):
            return self.bubble_popper(**kwargs)

        assert not self.batches
        batch_bubble_popper()

        assert len(self.bubbles) == 0
        assert len(self.batches) == int(
            (self.so_many_bubbles / self.bubble_multishooter_batch_size) + 1
        )

    def test_not_batchable_bubble_popper(self):
        pass
