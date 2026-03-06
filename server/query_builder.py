"""Builds parameterized SQL from a QueryRequest. No string interpolation of user values."""

from server.models import QueryRequest
from server.presets import get_preset
from server.config import settings

# Column mapping: user-facing names → actual DB columns
GROUP_COLUMNS = {
    "state": "state",
    "hcpcs_code": "HCPCS_CODE",
    "category": "category",
    "claim_month": "CLAIM_FROM_MONTH",
    "claim_year": "CAST(LEFT(CLAIM_FROM_MONTH, 4) AS INTEGER)",
    "zip3": "zip3",
    "billing_npi": "BILLING_PROVIDER_NPI_NUM",
    "taxonomy": "taxonomy",
}

# Allowed aggregate functions
ALLOWED_AGGS = {"sum", "avg", "count", "count_distinct", "median"}

# Columns safe for aggregation
METRIC_COLUMNS = {
    "TOTAL_PAID", "TOTAL_CLAIMS", "TOTAL_UNIQUE_BENEFICIARIES",
}


def build_query(req: QueryRequest) -> tuple[str, list]:
    """
    Build a SQL query from the request. Returns (sql_string, params_list).
    All user values are passed as parameters ($1, $2, ...) — never interpolated.
    """
    params: list = []
    where_clauses: list[str] = []

    # Apply preset first (may add codes/ordering)
    if req.preset:
        preset = get_preset(req.preset)
        if preset and preset.codes:
            if preset.filter_type == "hcpcs_codes":
                req.hcpcs_codes = list(set(req.hcpcs_codes + preset.codes))

    # Build WHERE clauses
    if req.states:
        placeholders = ", ".join(f"${len(params) + i + 1}" for i in range(len(req.states)))
        where_clauses.append(f"state IN ({placeholders})")
        params.extend(req.states)

    if req.hcpcs_codes:
        placeholders = ", ".join(f"${len(params) + i + 1}" for i in range(len(req.hcpcs_codes)))
        where_clauses.append(f"HCPCS_CODE IN ({placeholders})")
        params.extend(req.hcpcs_codes)

    if req.categories:
        placeholders = ", ".join(f"${len(params) + i + 1}" for i in range(len(req.categories)))
        where_clauses.append(f"category IN ({placeholders})")
        params.extend(req.categories)

    if req.npi:
        placeholders = ", ".join(f"${len(params) + i + 1}" for i in range(len(req.npi)))
        where_clauses.append(f"CAST(BILLING_PROVIDER_NPI_NUM AS VARCHAR) IN ({placeholders})")
        params.extend(req.npi)

    if req.taxonomy:
        placeholders = ", ".join(f"${len(params) + i + 1}" for i in range(len(req.taxonomy)))
        where_clauses.append(f"taxonomy IN ({placeholders})")
        params.extend(req.taxonomy)

    if req.provider_name:
        params.append(f"%{req.provider_name}%")
        where_clauses.append(f"provider_name ILIKE ${len(params)}")

    if req.date_from:
        params.append(req.date_from)
        where_clauses.append(f"CLAIM_FROM_MONTH >= ${len(params)}")

    if req.date_to:
        params.append(req.date_to)
        where_clauses.append(f"CLAIM_FROM_MONTH <= ${len(params)}")

    if req.zip3:
        placeholders = ", ".join(f"${len(params) + i + 1}" for i in range(len(req.zip3)))
        where_clauses.append(f"zip3 IN ({placeholders})")
        params.extend(req.zip3)

    # Build SELECT and GROUP BY
    select_parts: list[str] = []
    group_parts: list[str] = []

    for gb in req.group_by:
        col_expr = GROUP_COLUMNS.get(gb)
        if not col_expr:
            continue
        if gb == "claim_year":
            select_parts.append(f"{col_expr} AS claim_year")
            group_parts.append(col_expr)
        else:
            select_parts.append(col_expr)
            group_parts.append(col_expr)

    # Default metrics if none specified
    if not req.metrics:
        select_parts.extend([
            "SUM(TOTAL_PAID) AS total_paid",
            "SUM(TOTAL_CLAIMS) AS total_claims",
            "SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries",
            "COUNT(*) AS row_count",
        ])
        if req.include_avg_rate:
            select_parts.append(
                "CASE WHEN SUM(TOTAL_CLAIMS) > 0 "
                "THEN SUM(TOTAL_PAID) / SUM(TOTAL_CLAIMS) "
                "ELSE 0 END AS avg_rate"
            )
        if req.include_per_bene:
            select_parts.append(
                "CASE WHEN SUM(TOTAL_UNIQUE_BENEFICIARIES) > 0 "
                "THEN SUM(TOTAL_PAID) / SUM(TOTAL_UNIQUE_BENEFICIARIES) "
                "ELSE 0 END AS per_bene"
            )
    else:
        for m in req.metrics:
            if m.column not in METRIC_COLUMNS:
                continue
            if m.agg not in ALLOWED_AGGS:
                continue
            if m.agg == "count_distinct":
                select_parts.append(f"COUNT(DISTINCT {m.column}) AS {m.column}_{m.agg}")
            elif m.agg == "median":
                select_parts.append(f"MEDIAN({m.column}) AS {m.column}_{m.agg}")
            else:
                select_parts.append(f"{m.agg.upper()}({m.column}) AS {m.column}_{m.agg}")

    # HAVING clauses for volume filters
    having_clauses: list[str] = []
    if req.min_claims is not None:
        params.append(req.min_claims)
        having_clauses.append(f"SUM(TOTAL_CLAIMS) >= ${len(params)}")

    if req.min_beneficiaries is not None:
        params.append(req.min_beneficiaries)
        having_clauses.append(f"SUM(TOTAL_UNIQUE_BENEFICIARIES) >= ${len(params)}")

    # Assemble SQL
    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    group_sql = (" GROUP BY " + ", ".join(group_parts)) if group_parts else ""
    having_sql = (" HAVING " + " AND ".join(having_clauses)) if having_clauses else ""

    # Order
    order_col = req.order_by or ("total_paid" if not req.group_by else "total_paid")
    order_dir = "DESC" if req.order_dir.lower() == "desc" else "ASC"
    # For top_spending preset, always order by total_paid desc
    if req.preset == "top_spending":
        order_col = "total_paid"
        order_dir = "DESC"

    # Limit/offset
    limit = min(req.limit, settings.max_rows)
    offset = max(req.offset, 0)

    select_clause = ", ".join(select_parts) if select_parts else "COUNT(*) AS total"

    sql = (
        f"SELECT {select_clause} FROM spending"
        f"{where_sql}{group_sql}{having_sql}"
        f" ORDER BY {order_col} {order_dir}"
        f" LIMIT {limit} OFFSET {offset}"
    )

    return sql, params
