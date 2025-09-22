#!/bin/bash

# Test runner script for srunx SSH functionality
set -e

echo "Setting up environment..."
export PYTHONPATH="/Users/ksterx/Documents/Development/srunx/src"
cd /Users/ksterx/Documents/Development/srunx

echo "Running SSH tests..."
.venv/bin/pytest \
    tests/test_ssh_client.py \
    tests/test_ssh_config.py \
    tests/test_ssh_helpers.py \
    tests/test_ssh_integration_simple.py \
    tests/test_ssh_profiles_config.py \
    -v --tb=short --timeout=30

echo "SSH tests completed successfully!"

echo "Running all tests (excluding problematic integration tests)..."
.venv/bin/pytest tests/ \
    --ignore=tests/test_ssh_integration.py \
    --tb=short --timeout=30 \
    -x  # Stop on first failure

echo "All tests completed!"