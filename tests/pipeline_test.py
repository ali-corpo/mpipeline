import unittest
import time
import asyncio
from typing import List, Any
from mpipeline.pipeline import Pipeline, Stage
from mpipeline.worker import Worker
from mpipeline.worker_exception import WorkerException


class SimpleWorker(Worker[int, int]):
    def doTask(self, inp: int) -> int:
        return inp * 2


class SlowWorker(Worker[int, int]):
    def doTask(self, inp: int) -> int:
        time.sleep(0.1)
        return inp * 3


class ErrorWorker(Worker[int, int]):
    def doTask(self, inp: int) -> int:
        if inp > 5:
            raise ValueError(f"Input {inp} is too large")
        return inp


class ResourceWorker(Worker[int, int]):
    def doInit(self) -> None:
        self.resource = [1, 2, 3]  # Simulate some resource

    def doTask(self, inp: int) -> int:
        return inp + sum(self.resource)

    def doDispose(self) -> None:
        self.resource = []  # Clean up resource


class AsyncWorker(Worker[int, int]):
    async def doTask(self, inp: int) -> int:
        await asyncio.sleep(0.1)
        return inp * 2


class InitWorker(Worker[int, int]):
    def doInit(self):
        self.multiplier = 2

    def doTask(self, inp: int) -> int:
        return inp * self.multiplier

    def doDispose(self):
        self.multiplier = 0


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.inputs = list(range(10))

    def test_simple_pipeline(self):
        """Test basic pipeline functionality."""
        pipeline = Pipeline(Stage(SimpleWorker))
        results = list(pipeline.run(self.inputs))
        self.assertEqual(results, [x * 2 for x in self.inputs])

    def test_multi_stage_pipeline(self):
        """Test pipeline with multiple stages."""
        pipeline = Pipeline(Stage(SimpleWorker)).then(Stage(SlowWorker))
        results = list(pipeline.run(self.inputs))
        self.assertEqual(results, [x * 2 * 3 for x in self.inputs])

    def test_error_handling(self):
        """Test error handling in pipeline."""
        pipeline = Pipeline(Stage(ErrorWorker))
        with self.assertRaises(WorkerException) as ctx:
            list(pipeline.run(self.inputs))
        self.assertIn("too large", str(ctx.exception))

    def test_resource_cleanup(self):
        """Test proper resource cleanup."""
        pipeline = Pipeline(Stage(ResourceWorker))
        results = list(pipeline.run(range(3)))
        self.assertEqual(results, [x + 6 for x in range(3)])  # 6 is sum([1,2,3])

    def test_async_worker(self):
        """Test async worker support."""
        pipeline = Pipeline(Stage(AsyncWorker))
        results = list(pipeline.run(self.inputs))
        self.assertEqual(results, [x * 2 for x in self.inputs])

    def test_parallel_processing(self):
        """Test parallel processing with multiple workers."""
        pipeline = Pipeline(Stage(SlowWorker, worker_count=4))

        start_time = time.time()
        results = list(pipeline.run(self.inputs))
        end_time = time.time()

        # Should be faster than sequential processing
        sequential_time = 0.1 * len(self.inputs)  # Each item takes 0.1s
        self.assertLess(end_time - start_time, sequential_time)
        self.assertEqual(results, [x * 3 for x in self.inputs])

    def test_ordered_results(self):
        """Test that results maintain input order."""
        pipeline = Pipeline(Stage(SlowWorker, worker_count=4))

        results = list(pipeline.run(self.inputs, ordered_result=True))
        self.assertEqual(results, [x * 3 for x in self.inputs])

    def test_unordered_results(self):
        """Test unordered result processing."""
        pipeline = Pipeline(Stage(SlowWorker, worker_count=4))

        results = list(pipeline.run(self.inputs, ordered_result=False))
        self.assertEqual(sorted(results), sorted([x * 3 for x in self.inputs]))

    def test_worker_initialization(self):
        """Test worker initialization and disposal."""
        pipeline = Pipeline(Stage(InitWorker))
        results = list(pipeline.run(self.inputs))
        self.assertEqual(results, [x * 2 for x in self.inputs])


if __name__ == '__main__':
    unittest.main()
