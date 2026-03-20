"""Performance monitoring for CRUD operations."""
import time
from functools import wraps
from collections import defaultdict
from datetime import datetime

class PerformanceMonitor:
    """Tracks and reports performance metrics for operations."""
    
    def __init__(self):
        self.metrics = defaultdict(list)
        self.slow_query_threshold = 1.0  # seconds
    
    def record_operation(self, operation_name, duration_ms):
        """Record an operation's execution time."""
        self.metrics[operation_name].append(duration_ms)
    
    def get_stats(self, operation_name):
        """Get statistics for an operation."""
        if operation_name not in self.metrics:
            return None
        
        times = self.metrics[operation_name]
        if not times:
            return None
        
        sorted_times = sorted(times)
        return {
            "count": len(times),
            "total_ms": sum(times),
            "avg_ms": sum(times) / len(times),
            "min_ms": min(times),
            "max_ms": max(times),
            "p50_ms": sorted_times[len(sorted_times) // 2],
            "p95_ms": sorted_times[int(len(sorted_times) * 0.95)],
            "p99_ms": sorted_times[int(len(sorted_times) * 0.99)],
        }
    
    def get_all_stats(self):
        """Get statistics for all tracked operations."""
        result = {}
        for op in self.metrics.keys():
            stats = self.get_stats(op)
            if stats:
                result[op] = stats
        return result
    
    def print_report(self):
        """Print a formatted performance report."""
        stats = self.get_all_stats()
        if not stats:
            print("[Performance] No operations recorded yet")
            return
        
        print("\n" + "="*80)
        print(f"  PERFORMANCE REPORT (as of {datetime.now().isoformat()})")
        print("="*80)
        
        for operation, data in sorted(stats.items()):
            print(f"\n[{operation}]")
            print(f"  Count:    {data['count']:,} operations")
            print(f"  Total:    {data['total_ms']:.2f} ms")
            print(f"  Average:  {data['avg_ms']:.2f} ms")
            print(f"  Min/Max:  {data['min_ms']:.2f} / {data['max_ms']:.2f} ms")
            print(f"  P50/P95/P99: {data['p50_ms']:.2f} / {data['p95_ms']:.2f} / {data['p99_ms']:.2f} ms")
        
        print("\n" + "="*80 + "\n")
    
    def clear(self):
        """Clear all metrics."""
        self.metrics.clear()

# Global monitor instance
perf_monitor = PerformanceMonitor()

def track_performance(operation_name):
    """Decorator to track operation performance."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                duration_ms = (time.time() - start) * 1000
                perf_monitor.record_operation(f"{operation_name}_ERROR", duration_ms)
                raise
            
            duration_ms = (time.time() - start) * 1000
            perf_monitor.record_operation(operation_name, duration_ms)
            
            if duration_ms > perf_monitor.slow_query_threshold * 1000:
                print(f"[SLOW] {operation_name} took {duration_ms:.2f}ms")
            
            return result
        return wrapper
    return decorator
