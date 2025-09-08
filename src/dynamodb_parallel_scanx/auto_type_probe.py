# dpscan/auto_type_probe.py
from collections import Counter
from typing import Optional, Tuple

# Tipos escalares más comunes en DynamoDB
SCALAR_TYPES = ["S", "N", "BOOL", "B"]
SET_TYPES = ["SS", "NS", "BS"]
COMPLEX_TYPES = ["M", "L", "NULL"]

class AutoTypeResult:
    def __init__(self, ddb_type: Optional[str], samples_seen: int, counts: Counter):
        self.ddb_type = ddb_type              # p.ej. "S", "N", "BOOL"
        self.samples_seen = samples_seen      # cuántos items con ese atributo vimos
        self.counts = counts                  # distribución por tipo
    def __repr__(self):
        return f"<AutoTypeResult type={self.ddb_type} seen={self.samples_seen} dist={dict(self.counts)}>"

def _attr_type_from_item(item: dict, attr_name: str) -> Optional[str]:
    """Dado un item en formato JSON de DynamoDB (low-level client),
    devuelve el tipo DDB del atributo (#S, #N, #BOOL, etc) o None si no está."""
    if attr_name not in item:
        return None
    av = item[attr_name]
    if not isinstance(av, dict) or len(av) != 1:
        # Formato inesperado, lo ignoramos
        return None
    t = next(iter(av.keys()))
    return t if t in (SCALAR_TYPES + SET_TYPES + COMPLEX_TYPES) else None

def probe_attribute_type(
    ddb_client,
    table_name: str,
    attribute_name: str,
    *,
    index_name: Optional[str] = None,
    sample_size: int = 20,
    max_pages: int = 3,
    consistent_read: bool = False,
    log=None,
) -> AutoTypeResult:
    """Sondea la tabla para inferir el tipo DDB de un atributo.
    Lee pocas RCUs usando proyección + filtro attribute_exists.
    Devuelve el tipo mayoritario entre las muestras encontradas.
    """
    if sample_size <= 0:
        sample_size = 20

    common_kwargs = {
        "TableName": table_name,
        "Select": "SPECIFIC_ATTRIBUTES",
        "ProjectionExpression": "#a",
        "ExpressionAttributeNames": {"#a": attribute_name},
        "FilterExpression": "attribute_exists(#a)",
        "Limit": sample_size,
        "ConsistentRead": consistent_read,
    }
    if index_name:
        common_kwargs["IndexName"] = index_name

    counts = Counter()
    seen = 0
    scanned = 0
    page = 0
    exclusive_start_key = None

    while page < max_pages and seen < sample_size:
        kwargs = dict(common_kwargs)
        if exclusive_start_key:
            kwargs["ExclusiveStartKey"] = exclusive_start_key
        resp = ddb_client.scan(**kwargs)
        scanned += resp.get("ScannedCount", 0)
        items = resp.get("Items", [])
        for it in items:
            t = _attr_type_from_item(it, attribute_name)
            if t:
                counts[t] += 1
                seen += 1
                if seen >= sample_size:
                    break
        exclusive_start_key = resp.get("LastEvaluatedKey")
        page += 1
        if not exclusive_start_key:
            break

    if log:
        log.debug(f"[auto-type] scanned={scanned} seen={seen} dist={dict(counts)}")

    if not counts:
        # No encontramos el atributo en las muestras. Devolvemos None (caller decide fallback).
        return AutoTypeResult(None, seen, counts)

    # Tipo mayoritario
    ddb_type, _ = counts.most_common(1)[0]
    return AutoTypeResult(ddb_type, seen, counts)

def build_attribute_value(ddb_type: str, raw_value: str) -> Tuple[str, dict]:
    """Construye ExpressionAttributeValues según el tipo inferido."""
    if ddb_type == "S":
        return (":v0", { "S": str(raw_value) })
    if ddb_type == "N":
        # Dynamo requiere string numérica
        return (":v0", { "N": str(raw_value) })
    if ddb_type == "BOOL":
        # aceptamos truthy strings
        v = str(raw_value).strip().lower()
        b = v in ("1", "true", "t", "yes", "y")
        return (":v0", { "BOOL": b })
    if ddb_type in SET_TYPES:
        # sets: split por coma
        items = [s.strip() for s in str(raw_value).split(",") if s.strip()]
        if ddb_type == "SS":
            return (":v0", { "SS": items })
        if ddb_type == "NS":
            return (":v0", { "NS": [str(x) for x in items] })
        if ddb_type == "BS":
            # no transformamos a binario aquí; caller podría no soportar
            raise ValueError("BS no soportado automáticamente")
    # Tipos complejos: no filtramos por ellos automáticamente
    raise ValueError(f"Tipo DDB no soportado para filtros automáticos: {ddb_type}")
