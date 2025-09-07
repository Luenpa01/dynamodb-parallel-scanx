import argparse, json, sys
from .paginator import ParallelScanPaginator
from .sts import build_session, build_ddb_client
from .io_utils import write_jsonl_items, write_jsonl_pages, write_csv_items

# Helper

def _infer_dynamo_type(raw: str):
    """
    Inferir tipo Dynamo a partir de una cadena CLI.
    Regresa un dict estilo Dynamo: {"S": "..."} | {"N": "..."} | {"BOOL": True/False} | {"NULL": True}
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

    # NUMBER (int o float)
    # Acepta: 123, -12, 3.14, -0.5, 1e-9, 2E10
    try:
        float(s)  # valida que es número; Dynamo espera N como string
        # validación adicional para formato numérico:
        # si no es un número "raro", lo pasamos como N literal
        return {"N": s}
    except ValueError:
        pass

    # STRING (fallback)
    return {"S": s}


def main():
    p = argparse.ArgumentParser("dpscan: Parallel DynamoDB scan")
    # Dynamo args
    p.add_argument("-f", "--filter-field", action="append",
               help="Campo a filtrar (repetible). Ej: -f buyerName -f status")
    p.add_argument("-v", "--filter-value", action="append",
               help="Valor del filtro (repetible, en mismo orden que -f). Ej: -v Luis -v ACTIVE")
    p.add_argument("--filter-logic", choices=["AND", "OR"], default="AND",
               help="Cómo combinar múltiples filtros (-f/-v). Por defecto AND.")
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
    # STS / region
    p.add_argument("--region")
    p.add_argument("--role-arn")
    p.add_argument("--external-id")
    p.add_argument("--role-session-name", default="dpscan-session")
    p.add_argument("--role-duration-seconds", type=int)
    # Pool / timeouts
    p.add_argument("--max-pool-connections", type=int, default=128)
    p.add_argument("--read-timeout", type=int, default=60)
    p.add_argument("--connect-timeout", type=int, default=10)
    p.add_argument("--retries-max-attempts", type=int, default=10)
    # Output
    p.add_argument("--output-items", action="store_true", help="Emit items instead of full pages")
    p.add_argument("--output", help="CSV path; if omitted, prints JSONL to stdout")
    args = p.parse_args()

    sess = build_session(
        role_arn=args.role_arn,
        region=args.region,
        session_name=args.role_session_name,
        external_id=args.external_id,
        duration_seconds=args.role_duration_seconds
    )
    ddb = build_ddb_client(sess, region=args.region,
                           max_pool=args.max_pool_connections,
                           read_timeout=args.read_timeout,
                           connect_timeout=args.connect_timeout,
                           retries_max=args.retries_max_attempts)

    scan_args = {"TableName": args.table_name, "TotalSegments": args.total_segments}
    if args.index_name: scan_args["IndexName"] = args.index_name
    if args.limit: scan_args["Limit"] = args.limit
    if args.consistent_read: scan_args["ConsistentRead"] = True
    if args.projection_expression: scan_args["ProjectionExpression"] = args.projection_expression
    if args.filter_expression: scan_args["FilterExpression"] = args.filter_expression
    if args.expression_attribute_names: scan_args["ExpressionAttributeNames"] = args.expression_attribute_names
    if args.expression_attribute_values: scan_args["ExpressionAttributeValues"] = args.expression_attribute_values
    if args.return_consumed_capacity: scan_args["ReturnConsumedCapacity"] = args.return_consumed_capacity

    # --- Friendly filters: si el usuario NO pasó el modo crudo, construimos por él ---
    friendly_fields = getattr(args, "filter_field", None)
    friendly_values = getattr(args, "filter_value", None)

    # Si el usuario ya pasó el modo crudo, damos prioridad a ese y no hacemos nada aquí.
    already_has_raw = any(k in scan_args for k in ("FilterExpression", "ExpressionAttributeNames", "ExpressionAttributeValues"))

    if (friendly_fields or friendly_values) and not already_has_raw:
        if not (friendly_fields and friendly_values) or len(friendly_fields) != len(friendly_values):
            sys.exit("Error: debes pasar el mismo número de --filter-field (-f) y --filter-value (-v).")

        expr_parts = []
        expr_names = {}
        expr_values = {}

        # Construimos: (#f0 = :v0) <AND/OR> (#f1 = :v1) ...
        for i, (field, raw_val) in enumerate(zip(friendly_fields, friendly_values)):
            if not field:
                sys.exit("Error: --filter-field vacío no es válido.")
            name_key = f"#f{i}"
            value_key = f":v{i}"

            expr_names[name_key] = field
            expr_values[value_key] = _infer_dynamo_type(raw_val)
            expr_parts.append(f"({name_key} = {value_key})")

        scan_args["FilterExpression"] = f" {args.filter_logic} ".join(expr_parts)
        # Fusionar con los que ya existan (no deberían, por already_has_raw, pero por seguridad):
        scan_args["ExpressionAttributeNames"] = {**scan_args.get("ExpressionAttributeNames", {}), **expr_names}
        scan_args["ExpressionAttributeValues"] = {**scan_args.get("ExpressionAttributeValues", {}), **expr_values}

    paginator = ParallelScanPaginator(ddb)
    pages = paginator.paginate(**scan_args)

    if args.output:
        if not args.output_items:
            sys.exit("CSV soportado solo para --output-items (simple y útil).")
        count = write_csv_items(pages, args.output)
        print(f"wrote {count} items to {args.output}")
    else:
        out = sys.stdout
        (write_jsonl_items if args.output_items else write_jsonl_pages)(pages, out)
