#!/usr/bin/env python3
"""Example usage of the mpipeline library."""

import time
from typing import List, Dict
from mpipeline import Pipeline, Worker, Stage
from .worker_exception import WorkerException
from .examples import NumberGenerator, SlowProcessor, ErrorProneWorker


def main():
    """Run example pipeline."""
    print("Pipeline Example")
    print("-" * 50)

    print("\nDemonstrating unordered results...")
    # Show unordered results for better performance
    pipeline = Pipeline(
        Stage(NumberGenerator, worker_count=4)
    ).then(
        Stage(SlowProcessor, worker_count=2)
    )

    results = list(pipeline.run(range(50), ordered_result=False, show_progress=True, show_stage_progress=True))
    print("Unordered results:", results)

    print("\nDemonstrating Ordered results...")
    # Show unordered results for better performance
    pipeline = Pipeline(
        Stage(NumberGenerator, worker_count=4)
    ).then(
        Stage(SlowProcessor, worker_count=2)
    )

    results = list(pipeline.run(range(50), ordered_result=True, show_progress=True, show_stage_progress=True))
    print("Ordered results:", results)

    # Create pipeline with multiple stages
    pipeline = Pipeline(
        Stage(NumberGenerator, worker_count=4, worker_kwargs={'name': 'Generator'})
    ).then(
        Stage(SlowProcessor, worker_count=2, worker_kwargs={'name': 'Processor'})
    ).then(
        Stage(ErrorProneWorker, worker_count=2, worker_kwargs={'name': 'Validator'})
    )

    # Process some numbers
    inputs = range(10)
    print(f"\nProcessing {len(list(inputs))} numbers...")

    try:
        # Run pipeline with progress tracking
        results = list(pipeline.run(
            inputs,
            ordered_result=True,
            show_progress=True,
            show_stage_progress=True
        ))
        print("\nResults:")
        for result in results:
            print(result)

    except Exception as e:
        print(f"\nError occurred: {e}")

    print("\nDemonstrating error handling...")
    # Try processing numbers that will cause errors
    try:
        pipeline = Pipeline(
            Stage(NumberGenerator, worker_count=2)
        ).then(
            Stage(ErrorProneWorker, worker_count=2)
        )

        results = list(pipeline.run(range(15)))
        print("Results (should not see this):", results)

    except Exception as e:
        print(f"Caught expected error: {e}")
        if isinstance(e, WorkerException):
            e.re_raise()
        raise e


if __name__ == '__main__':
    main()
