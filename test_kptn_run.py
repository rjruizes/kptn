#!/usr/bin/env python3
"""Test script for kptn.run() API"""

import kptn

# Test 1: Run a single task
print("Test 1: Running single task 'fruit_summary'")
kptn.run("fruit_summary", project_dir="example/duckdb_example")

print("\n" + "="*60 + "\n")

# Test 2: Run multiple tasks as list
print("Test 2: Running multiple tasks as list")
kptn.run(["fruit_metrics", "fruit_summary"], project_dir="example/duckdb_example", force=True)

print("\n" + "="*60 + "\n")

# Test 3: Run all tasks
print("Test 3: Running all tasks")
kptn.run(project_dir="example/duckdb_example")

print("\nAll tests completed!")
