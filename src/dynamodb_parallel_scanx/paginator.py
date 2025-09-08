from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait
from typing import Dict, Generator, Any, Optional
import time
import random

class ParallelScanPaginator:
    """
    Parallel scan paginator con:
      - Límite de workers independiente de TotalSegments
      - 1 request en vuelo por segmento
      - Reintentos por página con backoff
      - Yield inmediato (streaming)
    """
    def __init__(self, ddb_client, workers: Optional[int] = None, max_retries: int = 3):
        self._client = ddb_client
        self._workers = workers  # si None, se calcula en paginate()
        self._max_retries = max_retries

    def paginate(self, **scan_args) -> Generator[Dict[str, Any], None, None]:
        total_segments = int(scan_args.get("TotalSegments") or 1)
        if total_segments < 1:
            total_segments = 1

        # Cap de workers: no más que TotalSegments y no más que, por ejemplo, 256 por defecto
        workers = self._workers if self._workers is not None else 256
        workers = max(1, min(workers, total_segments))

        # Estado por segmento (ExclusiveStartKey y reintentos)
        state = {
            seg: {
                "args": {**scan_args, "Segment": seg, "TotalSegments": total_segments},
                "lek": None,
                "retries": 0,
                "active": True,
            }
            for seg in range(total_segments)
        }

        def submit_for_segment(ex, seg):
            s = state[seg]
            call_args = dict(s["args"])
            if s["lek"]:
                call_args["ExclusiveStartKey"] = s["lek"]
            # Enviar la request del segmento
            return ex.submit(self._client.scan, **call_args)

        # Inicializa con hasta `workers` segmentos en vuelo
        with ThreadPoolExecutor(max_workers=workers) as ex:
            in_flight = {}
            seg_iter = iter(range(total_segments))

            # Pre-carga
            try:
                for _ in range(workers):
                    seg = next(seg_iter)
                    in_flight[submit_for_segment(ex, seg)] = seg
            except StopIteration:
                pass

            while in_flight:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    seg = in_flight.pop(fut)
                    s = state[seg]
                    if not s["active"]:
                        # segmento ya desactivado (raro), continúa
                        continue

                    try:
                        page = fut.result()
                        # Reset del contador de reintentos al tener éxito
                        s["retries"] = 0

                        # Emitimos la página (streaming)
                        yield page

                        # ¿Hay más?
                        s["lek"] = page.get("LastEvaluatedKey")
                        if s["lek"]:
                            # Encolar siguiente página de este segmento
                            in_flight[submit_for_segment(ex, seg)] = seg
                        else:
                            # Segmento terminado
                            s["active"] = False

                            # Lanzar otro segmento pendiente (si hay)
                            try:
                                nxt_seg = next(seg_iter)
                                in_flight[submit_for_segment(ex, nxt_seg)] = nxt_seg
                            except StopIteration:
                                pass

                    except Exception as e:
                        # Backoff simple con jitter y reintento por página
                        s["retries"] += 1
                        if s["retries"] <= self._max_retries:
                            sleep_s = min(2 ** (s["retries"] - 1), 4) + random.random()
                            time.sleep(sleep_s)
                            in_flight[submit_for_segment(ex, seg)] = seg
                        else:
                            # Desactiva el segmento tras exceder reintentos
                            s["active"] = False
                            # Opcional: loggea el error (usa tu logger)
                            # log.warning("Segment %s failed after %d retries: %s", seg, self._max_retries, e)

                            # Intenta ocupar el hueco con otro segmento (si hay)
                            try:
                                nxt_seg = next(seg_iter)
                                in_flight[submit_for_segment(ex, nxt_seg)] = nxt_seg
                            except StopIteration:
                                pass
