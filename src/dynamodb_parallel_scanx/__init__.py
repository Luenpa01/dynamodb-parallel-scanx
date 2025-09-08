"""
dynamodb-parallel-scanx
High-performance parallel scan for DynamoDB with STS AssumeRole.

Uso rápido:
-----------
from dynamodb_parallel_scanx import ParallelScanPaginator
from dynamodb_parallel_scanx.sts import build_session, build_ddb_client
"""

from .paginator import ParallelScanPaginator
from .sts import build_session, build_ddb_client

__all__ = [
    "ParallelScanPaginator",
    "build_session",
    "build_ddb_client",
]
__version__ = "0.1.4"
