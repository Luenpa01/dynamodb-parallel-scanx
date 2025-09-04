from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait
from typing import Dict, Generator, Any

class ParallelScanPaginator:
    """Parallel scan paginator: one in-flight scan per segment."""
    def __init__(self, ddb_client):
        self._client = ddb_client

    def paginate(self, **scan_args) -> Generator[Dict[str, Any], None, None]:
        segments = int(scan_args.get("TotalSegments") or 1)
        if segments < 1:
            segments = 1
        with ThreadPoolExecutor(max_workers=segments) as ex:
            tasks = [{**scan_args, "Segment": i, "TotalSegments": segments} for i in range(segments)]
            futures = {ex.submit(self._client.scan, **t): t for t in tasks}
            while futures:
                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                for fut in done:
                    task = futures.pop(fut)
                    page = fut.result()
                    yield page
                    lek = page.get("LastEvaluatedKey")
                    if lek:
                        nxt = {**task, "ExclusiveStartKey": lek}
                        futures[ex.submit(self._client.scan, **nxt)] = nxt
