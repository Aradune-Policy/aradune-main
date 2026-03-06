from pydantic import BaseModel, Field


class MetricDef(BaseModel):
    column: str
    agg: str = "sum"  # sum, avg, count, count_distinct, median


class QueryRequest(BaseModel):
    # Filters
    states: list[str] = Field(default_factory=list)
    hcpcs_codes: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    npi: list[str] = Field(default_factory=list)
    taxonomy: list[str] = Field(default_factory=list)
    provider_name: str | None = None
    date_from: str | None = None  # YYYY-MM format
    date_to: str | None = None
    zip3: list[str] = Field(default_factory=list)
    min_claims: int | None = None
    min_beneficiaries: int | None = None

    # Grouping
    group_by: list[str] = Field(default_factory=list)

    # Metrics
    metrics: list[MetricDef] = Field(default_factory=list)

    # Derived metrics
    include_avg_rate: bool = True
    include_per_bene: bool = False

    # Ordering / pagination
    order_by: str | None = None
    order_dir: str = "desc"
    limit: int = 1000
    offset: int = 0

    # Preset
    preset: str | None = None


class QueryRow(BaseModel):
    """A single result row — dynamic keys based on group_by and metrics."""
    pass


class QueryResponse(BaseModel):
    rows: list[dict]
    total_rows: int
    query_ms: float
    sql_preview: str | None = None


class QueryMeta(BaseModel):
    states: list[str]
    categories: list[str]
    date_min: str | None = None
    date_max: str | None = None
    columns: list[str]
    total_rows: int
    presets: list[str]


class PresetInfo(BaseModel):
    id: str
    name: str
    description: str
    codes: list[str]
    filter_type: str = "hcpcs_codes"
