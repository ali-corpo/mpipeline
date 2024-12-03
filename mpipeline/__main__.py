#!/usr/bin/env python3
"""Example usage of the mpipeline library."""
from .examples import ErrorProneWorker
from .examples import NumberGenerator
from .examples import SharedDataProcessor
from .examples import SlowProcessor
from mpipeline import Pipeline
from mpipeline import Stage


def main():
    """Run example pipeline."""
    print("Pipeline Example")
    print("-" * 50)

    print("\nDemonstrating unordered results with thread mode...")
    # Show unordered results for better performance using threads
    pipeline = Pipeline(
        Stage(NumberGenerator, worker_count=4, mode='thread')
    ).then(
        Stage(SlowProcessor, worker_count=2, mode='process')
    ).then(
        Stage(SlowProcessor, worker_count=2, mode='process')
    ).then(
        Stage(SlowProcessor, worker_count=200, mode='process')
    )

    results = list(pipeline.run(range(10), ordered_result=False, progress='stage'))
    import sys
    print("Unordered results:", results, file=sys.stderr)

    # import time
    # time.sleep(100)
    # return
    print("\nDemonstrating Ordered results with process mode...")
    # Show ordered results using processes with spawn
    pipeline = Pipeline(
        Stage(NumberGenerator, worker_count=4, mode='process', multiprocess_mode='spawn')
    ).then(
        Stage(SlowProcessor, worker_count=2, mode='process', multiprocess_mode='spawn')
    ).then(
        Stage(SharedDataProcessor, worker_count=2, mode='thread')
    )

    results = list(pipeline.run(range(50), ordered_result=True, progress='stage'))
    print("Ordered results:", results)

    # Create pipeline with multiple stages using mixed modes
    pipeline = Pipeline(
        Stage(NumberGenerator, worker_count=4, mode='thread',
              )
    ).then(
        Stage(SlowProcessor, worker_count=2, mode='process', multiprocess_mode='spawn',
              worker_kwargs={'name': 'Processor'})
    ).then(
        Stage(ErrorProneWorker, worker_count=2, mode='process',
              worker_kwargs={'name': 'Validator'})
    )

    # Process some numbers
    inputs = range(10)
    print(f"\nProcessing {len(list(inputs))} numbers...")

    try:
        # Run pipeline with progress tracking
        results = list(pipeline.run(
            inputs,
            ordered_result=True,
            progress='stage',
        ))
        print("\nResults:")
        for result in results:
            print(result)

    except Exception as e:
        print(f"\nError occurred: {e}")
        import traceback
        traceback.print_exc()

    print("\nDemonstrating error handling...")
    # Try processing numbers that will cause errors
    try:
        pipeline = Pipeline(
            Stage(NumberGenerator, worker_count=4, mode='process')
        ).then(
            Stage(ErrorProneWorker, worker_count=2, mode='process')
        )

        results = list(pipeline.run(range(15)))
        print("Results (should not see this):", results)

    except Exception as e:
        print(f"Caught expected error: {e}")
        raise e


if __name__ == '__main__':
    main()
