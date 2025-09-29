#!/usr/bin/env python3
"""
Performance benchmark for RethinkDB Python driver concurrency improvements.

This script tests various concurrency patterns and measures throughput
and latency improvements from the modernization changes.
"""

import asyncio
import time
import statistics
from concurrent.futures import ThreadPoolExecutor
import rethinkdb as r


async def benchmark_concurrent_queries(connection, num_queries=100, concurrency=10):
    """Benchmark concurrent query execution"""
    print(f"Running {num_queries} queries with concurrency={concurrency}")

    async def single_query():
        start_time = time.time()
        try:
            # Simple query that should be fast
            result = await r.expr(1).add(1).run(connection)
            return time.time() - start_time, True
        except Exception as e:
            return time.time() - start_time, False

    # Run queries in batches to control concurrency
    all_times = []
    successful = 0

    start_time = time.time()

    for i in range(0, num_queries, concurrency):
        batch_size = min(concurrency, num_queries - i)
        tasks = [single_query() for _ in range(batch_size)]
        results = await asyncio.gather(*tasks)

        for duration, success in results:
            all_times.append(duration)
            if success:
                successful += 1

    total_time = time.time() - start_time

    return {
        'total_time': total_time,
        'successful_queries': successful,
        'failed_queries': num_queries - successful,
        'queries_per_second': num_queries / total_time,
        'avg_latency': statistics.mean(all_times),
        'median_latency': statistics.median(all_times),
        'p95_latency': statistics.quantiles(all_times, n=20)[18] if len(all_times) >= 20 else max(all_times),
        'p99_latency': statistics.quantiles(all_times, n=100)[98] if len(all_times) >= 100 else max(all_times),
    }


async def benchmark_cursor_operations(connection, num_operations=50):
    """Benchmark cursor-heavy operations"""
    print(f"Running {num_operations} cursor operations")

    # Create test data
    test_table = 'benchmark_test'

    try:
        await r.table_drop(test_table).run(connection)
    except:
        pass

    try:
        await r.table_create(test_table).run(connection)
        # Insert test data
        test_data = [{'id': i, 'value': f'test_{i}'} for i in range(1000)]
        await r.table(test_table).insert(test_data).run(connection)
    except Exception as e:
        print(f"Setup failed: {e}")
        return None

    async def cursor_operation():
        start_time = time.time()
        try:
            cursor = await r.table(test_table).limit(100).run(connection)
            items = []
            async for item in cursor:
                items.append(item)
            return time.time() - start_time, len(items)
        except Exception as e:
            return time.time() - start_time, 0

    all_times = []
    total_items = 0

    start_time = time.time()

    # Run cursor operations with some concurrency
    tasks = [cursor_operation() for _ in range(num_operations)]
    results = await asyncio.gather(*tasks)

    for duration, items in results:
        all_times.append(duration)
        total_items += items

    total_time = time.time() - start_time

    # Cleanup
    try:
        await r.table_drop(test_table).run(connection)
    except:
        pass

    return {
        'total_time': total_time,
        'operations': num_operations,
        'total_items_processed': total_items,
        'operations_per_second': num_operations / total_time,
        'items_per_second': total_items / total_time,
        'avg_latency': statistics.mean(all_times),
        'median_latency': statistics.median(all_times),
    }


async def run_asyncio_benchmarks():
    """Run all benchmarks using asyncio backend"""
    print("=" * 60)
    print("RethinkDB Python Driver - Concurrency Benchmarks")
    print("=" * 60)

    # Set asyncio mode
    r.set_loop_type('asyncio')

    try:
        # Connect to RethinkDB
        connection = await r.connect('localhost', 28015, db='test')
        print("✓ Connected to RethinkDB")

        # Test 1: Basic concurrent queries
        print("\n1. Concurrent Query Benchmark")
        print("-" * 30)

        for concurrency in [1, 5, 10, 20]:
            result = await benchmark_concurrent_queries(connection, 100, concurrency)
            print(f"Concurrency {concurrency:2d}: {result['queries_per_second']:6.1f} QPS, "
                  f"avg latency: {result['avg_latency']*1000:5.1f}ms, "
                  f"p95: {result['p95_latency']*1000:5.1f}ms")

        # Test 2: Cursor operations
        print("\n2. Cursor Operations Benchmark")
        print("-" * 30)

        cursor_result = await benchmark_cursor_operations(connection, 20)
        if cursor_result:
            print(f"Cursor ops: {cursor_result['operations_per_second']:6.1f} ops/sec, "
                  f"items: {cursor_result['items_per_second']:6.0f} items/sec")
            print(f"Avg latency: {cursor_result['avg_latency']*1000:5.1f}ms")

        # Test 3: Connection pool simulation
        print("\n3. Connection Pool Simulation")
        print("-" * 30)

        # Simulate multiple connections working concurrently
        connections = []
        for i in range(5):
            conn = await r.connect('localhost', 28015, db='test')
            connections.append(conn)

        async def multi_conn_test(conn_id):
            conn = connections[conn_id % len(connections)]
            start_time = time.time()
            results = []

            for i in range(20):
                result = await r.expr(conn_id).add(i).run(conn)
                results.append(result)

            return time.time() - start_time, len(results)

        start_time = time.time()
        tasks = [multi_conn_test(i) for i in range(50)]
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_time

        total_ops = sum(count for _, count in results)
        print(f"Multi-connection: {total_ops/total_time:.1f} ops/sec across 5 connections")

        # Cleanup connections
        for conn in connections:
            await conn.close()
        await connection.close()

        print("\n✓ Benchmarks completed successfully")

    except Exception as e:
        print(f"✗ Benchmark failed: {e}")
        return False

    return True


def run_blocking_benchmarks():
    """Run benchmarks using blocking I/O for comparison"""
    print("\n" + "=" * 60)
    print("Blocking I/O Comparison Benchmark")
    print("=" * 60)

    try:
        # Connect using blocking mode
        connection = r.connect('localhost', 28015, db='test')
        print("✓ Connected to RethinkDB (blocking mode)")

        # Simple throughput test
        start_time = time.time()
        successful = 0

        for i in range(100):
            try:
                result = r.expr(1).add(1).run(connection)
                successful += 1
            except:
                pass

        total_time = time.time() - start_time
        print(f"Blocking I/O: {successful/total_time:.1f} QPS (sequential)")

        connection.close()

    except Exception as e:
        print(f"✗ Blocking benchmark failed: {e}")


if __name__ == "__main__":
    print("Starting RethinkDB concurrency benchmarks...")
    print("Make sure RethinkDB server is running on localhost:28015")
    print()

    # Run asyncio benchmarks
    asyncio.run(run_asyncio_benchmarks())

    # Run blocking comparison
    run_blocking_benchmarks()

    print("\nBenchmark Notes:")
    print("- Higher QPS (queries per second) is better")
    print("- Lower latency is better")
    print("- P95/P99 latencies show tail performance")
    print("- Results may vary based on system and network conditions")