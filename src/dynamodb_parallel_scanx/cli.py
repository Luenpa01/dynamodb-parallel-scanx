import argparse
import json
import sys
import logging

from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

from .paginator import ParallelScanPaginator
from .sts import build_session, build_ddb_client
from .io_utils import write_jsonl_items, write_jsonl_pages, write_csv_items
from .auto_type_probe import probe_attribute_type, build_attribute_value


# ----------------------------
# Helpers
# ----------------------------
def _strip_wrapping_quotes(s: str) -> str:
    """Si el valor viene con comillas envueltas (p.ej. '"123"'), las remueve."""
    if s is None:
        return s
    s = str(s).strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        return s[1:-1]
    return s


# ----------------------------
# CLI
# ----------------------------
def main():
    p = argparse.ArgumentParser("dpscan: Parallel DynamoDB scan")
    # --- Filtros friendly (opcionales) ---
    p.add_argument("-f", "--filter-field", action="append",
                   help="Campo a filtrar (repetible). Ej: -f buyerName -f status")
    p.add_argument("-v", "--filter-value", action="append",
                   help="Valor del filtro (repetible, en mismo orden que -f). Ej: -v Luis -v ACTIVE")
    p.add_argument("--filter-logic", choices=["AND", "OR"], default="AND",
                   help="Cómo combinar múltiples filtros (-f/-v). Por defecto AND.")

    # Tipado de valores (nuevo: auto por defecto; puede ser único o repetible)
    p.add_argument("--value-type", action="append",
                   choices=["auto", "S", "N", "BOOL", "SS", "NS"],
                   help="Tipo(s) del valor. Si se pasa una sola vez aplica a todos los filtros. "
                        "Si se repite, debe haber el mismo número que -f/-v. Por defecto: auto.")
    p.add_argument("--auto-type-sample", type=int, default=20,
                   help="Muestras para inferencia automática por campo (default: 20).")
    p.add_argument("--auto-type-index",
                   help="IndexName para sondeo (opcional, útil si el atributo vive/está proyectado en un GSI).")
    p.add_argument("--auto-type-fallback", choices=["S", "N", "BOOL"], default="S",
                   help="Tipo de fallback si no se encuentran muestras para inferir (default: S).")

    # --- Dynamo args crudos (compatibles con boto3) ---
    p.add_argument("--table-name", required=True)
    p.add_argument("--index-name")
    p.add_argument("--limit", type=int)
    p.add_argument("--consistent-read", action="store_true")
    p.add_argument("--projection-expression")
    p.add_argument("--filter-expression")
    p.add_argument("--expression-attribute-names", type=json.loads)
    p.add_argument("--expression-attribute-values", type=json.loads)
    p.add_argument("--return-consumed-capacity")
    p.add_argument("--total-segments", type=int, default=32)

    # --- Logging ---
    p.add_argument("--log-level", default="INFO",
                   choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
                   help="Nivel de log (default: INFO)")
    p.add_argument("--log-format", default="text",
                   choices=["text", "json"],
                   help="Formato de log: text | json (default: text)")

    # --- STS / región ---
    p.add_argument("--region")
    p.add_argument("--role-arn")
    p.add_argument("--external-id")
    p.add_argument("--role-session-name", default="dpscan-session")
    p.add_argument("--role-duration-seconds", type=int)

    # --- Pool / timeouts / retries ---
    p.add_argument("--max-pool-connections", type=int, default=128)
    p.add_argument("--read-timeout", type=int, default=60)
    p.add_argument("--connect-timeout", type=int, default=10)
    p.add_argument("--retries-max-attempts", type=int, default=10)

    # --- Output ---
    p.add_argument("--output-items", action="store_true", help="Emit items instead of full pages")
    p.add_argument("--output", help="Ruta de salida. .csv → CSV; .json/.jsonl → JSONL; si se omite → stdout JSONL")

    p.add_argument("--workers", type=int, help="Hilos para el paginator (<= TotalSegments)")

    args = p.parse_args()

    # ---- Logging setup ----
    level = getattr(logging, args.log_level.upper(), logging.INFO)
    if args.log_format == "json":
        class _JsonHandler(logging.StreamHandler):
            def emit(self, record):
                import json as _json, time as _time
                msg = {
                    "ts": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime(record.created)),
                    "level": record.levelname,
                    "msg": record.getMessage(),
                    "logger": record.name,
                }
                self.stream.write(_json.dumps(msg) + "\n")
                self.flush()
        logging.basicConfig(level=level, handlers=[_JsonHandler()])
    else:
        logging.basicConfig(level=level, format="[%(levelname)s] %(message)s")

    log = logging.getLogger("dpscan")

    try:
        # -------- sesión / cliente --------
        log.info("starting dpscan table=%s region=%s segments=%s",
                 args.table_name, args.region, args.total_segments)

        sess = build_session(
            role_arn=args.role_arn,
            region=args.region,
            session_name=args.role_session_name,
            external_id=args.external_id,
            duration_seconds=args.role_duration_seconds
        )
        if args.role_arn:
            log.info("assumed role: %s", args.role_arn)

        ddb = build_ddb_client(
            sess, region=args.region,
            max_pool=args.max_pool_connections,
            read_timeout=args.read_timeout,
            connect_timeout=args.connect_timeout,
            retries_max=args.retries_max_attempts
        )
        log.debug("botocore: max_pool=%d read_timeout=%d connect_timeout=%d retries=%d",
                  args.max_pool_connections, args.read_timeout, args.connect_timeout, args.retries_max_attempts)

        # -------- scan args base --------
        scan_args = {"TableName": args.table_name, "TotalSegments": args.total_segments}
        if args.index_name: scan_args["IndexName"] = args.index_name
        if args.limit: scan_args["Limit"] = args.limit
        if args.consistent_read: scan_args["ConsistentRead"] = True
        if args.projection_expression:
            scan_args["ProjectionExpression"] = args.projection_expression
            log.debug("projection: %s", args.projection_expression)
        if args.filter_expression:
            scan_args["FilterExpression"] = args.filter_expression
        if args.expression_attribute_names:
            scan_args["ExpressionAttributeNames"] = args.expression_attribute_names
        if args.expression_attribute_values:
            scan_args["ExpressionAttributeValues"] = args.expression_attribute_values
        if args.return_consumed_capacity:
            scan_args["ReturnConsumedCapacity"] = args.return_consumed_capacity

        # -------- filtros friendly (si NO hay modo crudo) --------
        friendly_fields = getattr(args, "filter_field", None)
        friendly_values = getattr(args, "filter_value", None)
        value_types = getattr(args, "value_type", None)  # puede ser None, una lista de 1, o lista de N
        already_has_raw = any(
            k in scan_args for k in ("FilterExpression", "ExpressionAttributeNames", "ExpressionAttributeValues")
        )

        if (friendly_fields or friendly_values) and not already_has_raw:
            if not (friendly_fields and friendly_values) or len(friendly_fields) != len(friendly_values):
                sys.exit("Error: debes pasar el mismo número de --filter-field (-f) y --filter-value (-v).")

            # Normaliza value_types: si None → ['auto'] * N; si 1 → réplica; si N → debe coincidir
            n = len(friendly_fields)
            if not value_types:
                norm_types = ["auto"] * n
            elif len(value_types) == 1:
                norm_types = [value_types[0]] * n
            elif len(value_types) == n:
                norm_types = value_types
            else:
                sys.exit("Error: --value-type debe usarse 1 vez o N veces (N = cantidad de -f/-v).")

            expr_parts, expr_names, expr_values = [], {}, {}

            for i, (field, raw_val, vt) in enumerate(zip(friendly_fields, friendly_values, norm_types)):
                if not field:
                    sys.exit("Error: --filter-field vacío no es válido.")

                name_key, value_key = f"#f{i}", f":v{i}"
                expr_names[name_key] = field

                # Limpieza de comillas envolventes del valor pasado por CLI
                normalized_raw = _strip_wrapping_quotes(raw_val)

                # Inferencia/forzado de tipo por campo
                inferred_type = vt
                if vt == "auto":
                    res = probe_attribute_type(
                        ddb_client=ddb,
                        table_name=args.table_name,
                        attribute_name=field,
                        index_name=args.auto_type_index,
                        sample_size=args.auto_type_sample,
                        consistent_read=args.consistent_read,
                        log=log
                    )
                    if res.ddb_type is None:
                        inferred_type = args.auto_type_fallback
                        log.warning("[auto-type] '%s' sin muestras; usando fallback=%s", field, inferred_type)
                    else:
                        inferred_type = res.ddb_type
                        if res.counts and len(res.counts) > 1:
                            log.warning("[auto-type] '%s' tipos mixtos=%s; usando mayoritario=%s",
                                        field, dict(res.counts), inferred_type)
                    log.debug("[auto-type] field=%s inferred=%s", field, inferred_type)

                # Arma AttributeValue según tipo decidido
                try:
                    key_alias, attr_value = build_attribute_value(inferred_type, normalized_raw)
                except ValueError as e:
                    sys.exit(f"Error en build_attribute_value para campo '{field}': {e}")

                # build_attribute_value siempre devuelve ':v0'; reasignamos a nuestro placeholder ':v{i}'
                if key_alias != value_key:
                    # renombramos la key para mantener consistencia
                    attr_value = attr_value  # mismo dict
                expr_values[value_key] = attr_value

                expr_parts.append(f"({name_key} = {value_key})")

            scan_args["FilterExpression"] = f" {args.filter_logic} ".join(expr_parts)
            scan_args["ExpressionAttributeNames"] = {
                **scan_args.get("ExpressionAttributeNames", {}), **expr_names
            }
            scan_args["ExpressionAttributeValues"] = {
                **scan_args.get("ExpressionAttributeValues", {}), **expr_values
            }
            log.debug("friendly filter: %s", scan_args["FilterExpression"])

        # -------- ejecución del scan --------

        paginator = ParallelScanPaginator(ddb, workers=args.workers, max_retries=3)
        pages = paginator.paginate(**scan_args)


        # -------- Output selector --------
        if args.output:
            path = args.output.lower()
            if path.endswith(".csv"):
                # CSV → items (aunque el user no ponga --output-items)
                count = write_csv_items(pages, args.output)
                log.info("csv written path=%s items=%d", args.output, count)
                print(f"wrote {count} items to {args.output}")
            else:
                # .json / .jsonl / otra ext → JSONL
                with open(args.output, "w", encoding="utf-8") as f:
                    (write_jsonl_items if args.output_items else write_jsonl_pages)(pages, f)
                log.info("jsonl written path=%s items_only=%s", args.output, bool(args.output_items))
                print(f"wrote JSONL to {args.output}")
        else:
            out = sys.stdout
            (write_jsonl_items if args.output_items else write_jsonl_pages)(pages, out)

        log.info("done")

    except NoCredentialsError:
        sys.exit("ERROR: No se encontraron credenciales AWS. Configura env vars/perfiles o usa --role-arn.")
    except EndpointConnectionError as e:
        sys.exit(f"ERROR: No se pudo conectar a DynamoDB ({e}). Revisa --region o conectividad.")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "ClientError")
        msg = e.response.get("Error", {}).get("Message", str(e))
        sys.exit(f"ERROR {code}: {msg}")
    except Exception as e:
        sys.exit(f"ERROR inesperado: {e}")


if __name__ == "__main__":
    main()
