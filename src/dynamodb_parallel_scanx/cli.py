import argparse
import json
import sys
import logging

from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

from .paginator import ParallelScanPaginator
from .sts import build_session, build_ddb_client
from .io_utils import write_jsonl_items, write_jsonl_pages, write_csv_items


# ----------------------------
# Helpers
# ----------------------------
def _infer_dynamo_type(raw: str):
    """
    Inferir tipo Dynamo a partir de una cadena CLI.
    Devuelve dict estilo Dynamo: {"S": "..."} | {"N": "..."} | {"BOOL": True/False} | {"NULL": True}
    """
    if raw is None:
        return {"NULL": True}

    s = str(raw).strip()

    # NULL
    if s.lower() in ("null", "none"):
        return {"NULL": True}

    # BOOL
    if s.lower() in ("true", "false"):
        return {"BOOL": s.lower() == "true"}

    # NUMBER (int/float, notación científica permitida)
    try:
        float(s)  # valida
        return {"N": s}
    except ValueError:
        pass

    # STRING (fallback)
    return {"S": s}


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
        already_has_raw = any(
            k in scan_args for k in ("FilterExpression", "ExpressionAttributeNames", "ExpressionAttributeValues")
        )

        if (friendly_fields or friendly_values) and not already_has_raw:
            if not (friendly_fields and friendly_values) or len(friendly_fields) != len(friendly_values):
                sys.exit("Error: debes pasar el mismo número de --filter-field (-f) y --filter-value (-v).")

            expr_parts, expr_names, expr_values = [], {}, {}
            for i, (field, raw_val) in enumerate(zip(friendly_fields, friendly_values)):
                if not field:
                    sys.exit("Error: --filter-field vacío no es válido.")
                name_key, value_key = f"#f{i}", f":v{i}"
                expr_names[name_key] = field
                expr_values[value_key] = _infer_dynamo_type(raw_val)
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
        paginator = ParallelScanPaginator(ddb)
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