import argparse, json, sys
from .paginator import ParallelScanPaginator
from .sts import build_session, build_ddb_client
from .io_utils import write_jsonl_items, write_jsonl_pages, write_csv_items

def main():
    p = argparse.ArgumentParser("dpscan: Parallel DynamoDB scan")
    # Dynamo args
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
