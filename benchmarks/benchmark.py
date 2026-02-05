"""
Benchmarks for KV Store.
"""

import time
import os
import shutil
import subprocess
import threading
import signal

from client import KVClient


def start_server(port, data_dir):
    """Start a server."""
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    
    proc = subprocess.Popen(
        ["python", "run_server.py", "--port", str(port), "--data-dir", data_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(2)
    return proc


def stop_server(proc):
    """Stop server gracefully."""
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except:
        proc.kill()


def benchmark_write_throughput():
    """
    Benchmark: Write throughput (writes/second).
    Tests with different amounts of pre-populated data.
    """
    print("\n" + "="*60)
    print("BENCHMARK: Write Throughput")
    print("="*60)
    
    data_dir = "bench_data_write"
    port = 5010
    
    proc = start_server(port, data_dir)
    client = KVClient(port=port)
    
    for _ in range(10):
        if client.health():
            break
        time.sleep(0.3)
    
    # Test with different pre-populated sizes
    test_sizes = [0, 100, 500, 1000]
    
    for pre_size in test_sizes:
        # Pre-populate
        if pre_size > 0:
            items = [(f"pre_{i}", f"preval_{i}") for i in range(pre_size)]
            client.bulk_set(items)
        
        # Benchmark writes
        num_writes = 100
        start = time.time()
        
        for i in range(num_writes):
            client.set(f"bench_key_{pre_size}_{i}", f"bench_value_{i}")
        
        elapsed = time.time() - start
        throughput = num_writes / elapsed
        
        print(f"Pre-populated: {pre_size:5d} keys | "
              f"Writes: {num_writes} | "
              f"Time: {elapsed:.3f}s | "
              f"Throughput: {throughput:.1f} writes/sec")
    
    client.close()
    stop_server(proc)
    
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)


def benchmark_bulk_throughput():
    """
    Benchmark: Bulk write throughput.
    """
    print("\n" + "="*60)
    print("BENCHMARK: Bulk Write Throughput")
    print("="*60)
    
    data_dir = "bench_data_bulk"
    port = 5011
    
    proc = start_server(port, data_dir)
    client = KVClient(port=port)
    
    for _ in range(10):
        if client.health():
            break
        time.sleep(0.3)
    
    batch_sizes = [10, 50, 100]
    
    for batch_size in batch_sizes:
        num_batches = 10
        total_writes = num_batches * batch_size
        
        start = time.time()
        
        for b in range(num_batches):
            items = [(f"bulk_{b}_{i}", f"val_{b}_{i}") for i in range(batch_size)]
            client.bulk_set(items)
        
        elapsed = time.time() - start
        throughput = total_writes / elapsed
        
        print(f"Batch size: {batch_size:3d} | "
              f"Total writes: {total_writes:5d} | "
              f"Time: {elapsed:.3f}s | "
              f"Throughput: {throughput:.1f} writes/sec")
    
    client.close()
    stop_server(proc)
    
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)


def benchmark_durability():
    """
    Benchmark: Durability under crash conditions.
    """
    print("\n" + "="*60)
    print("BENCHMARK: Durability (Kill Test)")
    print("="*60)
    
    data_dir = "bench_data_durability"
    port = 5012
    
    total_runs = 3
    total_acknowledged = 0
    total_lost = 0
    
    for run in range(total_runs):
        proc = start_server(port, data_dir)
        client = KVClient(port=port)
        
        for _ in range(10):
            if client.health():
                break
            time.sleep(0.3)
        
        # Write data
        acknowledged = []
        for i in range(50):
            key = f"dur_run{run}_key_{i}"
            if client.set(key, f"value_{i}"):
                acknowledged.append(key)
        
        client.close()
        
        # SIGKILL
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
        
        time.sleep(1)
        
        # Restart and check
        proc2 = start_server(port, data_dir)
        client2 = KVClient(port=port)
        
        for _ in range(10):
            if client2.health():
                break
            time.sleep(0.3)
        
        lost = sum(1 for k in acknowledged if client2.get(k) is None)
        
        total_acknowledged += len(acknowledged)
        total_lost += lost
        
        print(f"Run {run+1}: Acknowledged={len(acknowledged)}, Lost={lost}")
        
        client2.close()
        stop_server(proc2)
    
    durability_rate = (1 - total_lost / total_acknowledged) * 100 if total_acknowledged > 0 else 100
    print(f"\nOverall Durability: {durability_rate:.2f}%")
    print(f"Total Acknowledged: {total_acknowledged}, Total Lost: {total_lost}")
    
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)


def benchmark_read_throughput():
    """
    Benchmark: Read throughput.
    """
    print("\n" + "="*60)
    print("BENCHMARK: Read Throughput")
    print("="*60)
    
    data_dir = "bench_data_read"
    port = 5013
    
    proc = start_server(port, data_dir)
    client = KVClient(port=port)
    
    for _ in range(10):
        if client.health():
            break
        time.sleep(0.3)
    
    # Pre-populate
    num_keys = 500
    items = [(f"read_key_{i}", f"read_value_{i}") for i in range(num_keys)]
    client.bulk_set(items)
    
    # Benchmark reads
    num_reads = 500
    start = time.time()
    
    for i in range(num_reads):
        client.get(f"read_key_{i % num_keys}")
    
    elapsed = time.time() - start
    throughput = num_reads / elapsed
    
    print(f"Reads: {num_reads} | Time: {elapsed:.3f}s | Throughput: {throughput:.1f} reads/sec")
    
    client.close()
    stop_server(proc)
    
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)


def run_all_benchmarks():
    """Run all benchmarks."""
    print("\n" + "#"*60)
    print("# KV STORE BENCHMARKS")
    print("#"*60)
    
    benchmark_write_throughput()
    benchmark_bulk_throughput()
    benchmark_read_throughput()
    benchmark_durability()
    
    print("\n" + "#"*60)
    print("# BENCHMARKS COMPLETE")
    print("#"*60)


if __name__ == "__main__":
    run_all_benchmarks()