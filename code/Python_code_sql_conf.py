"""
=====================================================================
  SQL & Pipeline Config Code Generator
  -------------------------------------
  Problem  : Manually creating SQL DDL + pipeline config files for
             every table × every environment (dev / staging / prod)
             was slow (~3-4 hrs/table), inconsistent, and error-prone.
  Solution : Engineers edit ONE YAML file per table. This script
             validates the YAML with Pydantic, then renders SQL DDL
             and pipeline config files for all environments using
             Jinja2 templates — in under 5 minutes.
=====================================================================
"""

# --- Standard library ---------------------------------------------------------------------
import os
import sys
from pathlib import Path
from typing import List, Optional
from datetime import datetime

# --- Third-party ----------------------------------------------------------------------------
import yaml                          # PyYAML  - reads YAML config files
from jinja2 import Environment, FileSystemLoader, StrictUndefined
                                     # Jinja2  - renders SQL / config templates
from pydantic import BaseModel, Field, field_validator, ValidationError
                                     # Pydantic - validates YAML schema at runtime


# -------------------------------------------------------------------------------------------------
#  SECTION 1 - PYDANTIC MODELS  (schema validation layer)
#
#  Pydantic converts raw Python dicts (parsed from YAML) into typed
#  objects. If the YAML is missing a required field or contains an
#  invalid value, Pydantic raises a ValidationError BEFORE we write
#  a single file — so broken configs never make it to disk.
# -------------------------------------------------------------------------------------------------

# Allowed SQL data types — centralised so validation stays in sync
# with whatever types your target database actually supports.
ALLOWED_DATA_TYPES = {
    "STRING", "INTEGER", "BIGINT", "FLOAT", "DOUBLE",
    "BOOLEAN", "DATE", "TIMESTAMP", "DECIMAL", "JSON"
}


class ColumnDefinition(BaseModel):
    """
    Represents a single column in a table.

    Fields
    ------
    name        : column identifier  (e.g. "user_id")
    data_type   : SQL type           (must be in ALLOWED_DATA_TYPES)
    nullable    : whether NULL is permitted  (default True)
    description : free-text comment written into the DDL as metadata
    """
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None

    # --- Validator: normalise data_type to uppercase and check the
    #    allowlist before Jinja2 ever sees it.
    @field_validator("data_type")
    @classmethod
    def validate_data_type(cls, value: str) -> str:
        normalised = value.upper()
        if normalised not in ALLOWED_DATA_TYPES:
            raise ValueError(
                f"Unsupported data type '{value}'. "
                f"Allowed: {sorted(ALLOWED_DATA_TYPES)}"
            )
        return normalised


class PartitionSpec(BaseModel):
    """
    Defines how the table is partitioned in storage.

    Fields
    ------
    columns  : list of column names used as partition keys
    strategy : partitioning strategy  (range | list | hash)
    """
    columns: List[str]
    strategy: str = "range"

    @field_validator("strategy")
    @classmethod
    def validate_strategy(cls, value: str) -> str:
        allowed = {"range", "list", "hash"}
        if value.lower() not in allowed:
            raise ValueError(
                f"Unknown partition strategy '{value}'. Allowed: {allowed}"
            )
        return value.lower()


class PipelineConfig(BaseModel):
    """
    Pipeline-specific settings that travel alongside the DDL.

    These values are injected into the pipeline config template so
    orchestration tools (Airflow, dbt, Spark, etc.) know how to
    schedule and load the table.
    """
    schedule: str = "daily"          # cron alias or cron expression
    load_type: str = "incremental"   # full | incremental | snapshot
    primary_key: List[str] = Field(default_factory=list)
    watermark_column: Optional[str] = None   # used for incremental loads

    @field_validator("load_type")
    @classmethod
    def validate_load_type(cls, value: str) -> str:
        allowed = {"full", "incremental", "snapshot"}
        if value.lower() not in allowed:
            raise ValueError(
                f"Unknown load_type '{value}'. Allowed: {allowed}"
            )
        return value.lower()


class TableSchema(BaseModel):
    """
    Top-level model: describes everything about one table.

    This is the root object we parse from each table's YAML file.
    Pydantic validates the full tree recursively — a bad ColumnDefinition
    nested three levels deep will still surface a clear error message.
    """
    table_name: str
    database: str
    owner: str
    columns: List[ColumnDefinition]
    partition: Optional[PartitionSpec] = None
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    tags: List[str] = Field(default_factory=list)  # optional metadata

    @field_validator("columns")
    @classmethod
    def at_least_one_column(cls, cols: List[ColumnDefinition]):
        """A table with zero columns is always a config mistake."""
        if not cols:
            raise ValueError("Table must define at least one column.")
        return cols


# -------------------------------------------------------------------------------------------------
#  SECTION 2 — JINJA2 TEMPLATES  (inline for portability)
#
#  In a real project these would live in a templates/ directory.
#  We define them as multi-line strings here so this file is fully
#  self-contained and easy to review alongside the logic.
# -------------------------------------------------------------------------------------------------

# --- Template A: CREATE TABLE DDL ---------------------------------------------------
#
#  Jinja2 syntax recap used below:
#    {{ var }}           — interpolate a variable
#    {% for x in xs %}  — loop
#    {% if cond %}       — conditional
#    {# comment #}       — template comment (stripped from output)
#    loop.last           — Jinja2 built-in; True on the final iteration

DDL_TEMPLATE = """\
{# ----------------------------------------------------------------
   Auto-generated by sql_generator/generate.py
   DO NOT EDIT MANUALLY — update the YAML and re-run the generator.
   Generated : {{ generated_at }}
   Environment: {{ env }}
---------------------------------------------------------------- #}
-- Table   : {{ schema.database }}_{{ env }}.{{ schema.table_name }}
-- Owner   : {{ schema.owner }}
-- Tags    : {{ schema.tags | join(", ") if schema.tags else "none" }}

CREATE TABLE IF NOT EXISTS {{ schema.database }}_{{ env }}.{{ schema.table_name }} (
{% for col in schema.columns %}
    {# Emit the column definition; omit trailing comma on last column #}
    {{ col.name }} {{ col.data_type }}{% if not col.nullable %} NOT NULL{% endif %}{% if not loop.last %},{% endif %}
    {# Attach the description as an inline SQL comment when present #}
    {% if col.description %}  -- {{ col.description }}{% endif %}

{% endfor %}
)
{% if schema.partition %}
PARTITION BY {{ schema.partition.strategy | upper }}(
    {{ schema.partition.columns | join(", ") }}
)
{% endif %}
;
"""

# --- Template B: Pipeline configuration (YAML-style config file) ----
#
#  Orchestration tools often consume their own YAML/JSON configs that
#  describe *how* to load a table, not just its schema.  This template
#  produces that file, environment-aware, from the same source of truth.

PIPELINE_TEMPLATE = """\
# Auto-generated pipeline configuration
# DO NOT EDIT — update the source YAML and re-run generate.py
# Generated : {{ generated_at }}

pipeline:
  table: "{{ schema.database }}_{{ env }}.{{ schema.table_name }}"
  environment: "{{ env }}"
  schedule: "{{ schema.pipeline.schedule }}"
  load_type: "{{ schema.pipeline.load_type }}"
{% if schema.pipeline.primary_key %}
  primary_key:
{% for pk in schema.pipeline.primary_key %}
    - "{{ pk }}"
{% endfor %}
{% endif %}
{% if schema.pipeline.watermark_column %}
  watermark_column: "{{ schema.pipeline.watermark_column }}"
{% endif %}
  columns:
{% for col in schema.columns %}
    - name: "{{ col.name }}"
      type: "{{ col.data_type }}"
      nullable: {{ col.nullable | lower }}
{% endfor %}
"""


# -------------------------------------------------------------------------------------------------
#  SECTION 3 — GENERATOR CLASS
#
#  Orchestrates the full pipeline:
#    load YAML → validate with Pydantic → render templates → write files
# -------------------------------------------------------------------------------------------------

class SQLGenerator:
    """
    Reads table YAML configs, validates them, and writes SQL DDL +
    pipeline config files for every target environment.

    Parameters
    ----------
    config_dir   : directory that contains <table>.yaml files
    output_dir   : root directory for generated output
    environments : list of environment names (default: dev/staging/prod)
    """

    # Environments to generate files for — engineers add/remove here.
    DEFAULT_ENVIRONMENTS = ["dev", "staging", "prod"]

    def __init__(
        self,
        config_dir: str = "configs",
        output_dir: str = "output",
        environments: Optional[List[str]] = None,
    ):
        self.config_dir = Path(config_dir)
        self.output_dir = Path(output_dir)
        self.environments = environments or self.DEFAULT_ENVIRONMENTS

        # --- Set up Jinja2 with StrictUndefined so that a typo in a
        #    template variable raises an error instead of silently
        #    rendering an empty string.
        self.jinja_env = Environment(
            loader=FileSystemLoader("."),   # base path for external templates
            undefined=StrictUndefined,       # fail loudly on missing vars
            trim_blocks=True,                # strip newline after block tags
            lstrip_blocks=True,              # strip leading whitespace before tags
        )

        # Load our inline template strings directly into Jinja2
        self.ddl_template      = self.jinja_env.from_string(DDL_TEMPLATE)
        self.pipeline_template = self.jinja_env.from_string(PIPELINE_TEMPLATE)

    # --- Public entry point -------------------------------------------------------------

    def run(self) -> None:
        """
        Main driver — discovers all YAML files in config_dir,
        validates + generates each one, then prints a summary.
        """
        yaml_files = list(self.config_dir.glob("*.yaml"))
        if not yaml_files:
            print(f"[WARN] No YAML files found in '{self.config_dir}/'")
            return

        success, failure = 0, 0

        for yaml_path in sorted(yaml_files):
            print(f"\n{'-'*60}")
            print(f"  Processing : {yaml_path.name}")
            try:
                schema = self._load_and_validate(yaml_path)
                self._generate_all_environments(schema)
                success += 1
                print(f"  ✓ Done      : {schema.table_name}")
            except ValidationError as exc:
                # Pydantic gives us a clean, human-readable error list.
                print(f"  ✗ Schema validation failed for {yaml_path.name}:")
                for error in exc.errors():
                    # loc is a tuple like ('columns', 2, 'data_type')
                    location = " → ".join(str(l) for l in error["loc"])
                    print(f"      [{location}] {error['msg']}")
                failure += 1
            except Exception as exc:
                print(f"  ✗ Unexpected error: {exc}")
                failure += 1

        # --- Summary banner -------------------------------------------------------------
        print(f"\n{'═'*60}")
        print(f"  Tables processed : {success + failure}")
        print(f"  ✓ Success        : {success}")
        print(f"  ✗ Failed         : {failure}")
        print(f"  Output directory : {self.output_dir}/")
        print(f"{'═'*60}\n")

    # --- Internal helpers ----------------------------------------------------------------

    def _load_and_validate(self, yaml_path: Path) -> TableSchema:
        """
        Step 1: Parse YAML → Step 2: Validate with Pydantic.

        Why separate? If we passed raw YAML dicts straight to Jinja2,
        typos and missing keys would produce silently broken SQL.
        Pydantic's strict validation catches those mistakes here,
        before any file I/O happens.
        """
        with yaml_path.open("r") as fh:
            raw_data = yaml.safe_load(fh)   # safe_load prevents arbitrary Python execution

        # Pydantic constructs a fully validated, typed TableSchema object.
        # Any error surfaces as a ValidationError with field-level messages.
        return TableSchema(**raw_data)

    def _generate_all_environments(self, schema: TableSchema) -> None:
        """
        Iterates over every target environment and calls the per-environment
        generator.  Keeping env-iteration here (not in run()) means we
        can easily unit-test a single environment in isolation.
        """
        for env in self.environments:
            self._generate_for_environment(schema, env)

    def _generate_for_environment(self, schema: TableSchema, env: str) -> None:
        """
        Renders and writes TWO files for one (table, environment) pair:
          1. <output>/<env>/ddl/<table>.sql          — CREATE TABLE statement
          2. <output>/<env>/pipeline/<table>.yaml    — pipeline config
        """
        # Build directory paths and create them if they don't exist yet.
        ddl_dir      = self.output_dir / env / "ddl"
        pipeline_dir = self.output_dir / env / "pipeline"
        ddl_dir.mkdir(parents=True, exist_ok=True)
        pipeline_dir.mkdir(parents=True, exist_ok=True)

        # Template context — every variable referenced in {{ ... }} blocks
        # must appear in this dict.  StrictUndefined will raise if anything
        # is missing, so this acts as a second validation layer.
        context = {
            "schema": schema,
            "env": env,
            "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        }

        # --- Render DDL ------------------------------------------------------------------
        ddl_sql  = self.ddl_template.render(**context)
        ddl_file = ddl_dir / f"{schema.table_name}.sql"
        ddl_file.write_text(ddl_sql, encoding="utf-8")
        print(f"    → wrote {ddl_file}")

        # --- Render Pipeline Config ------------------------------------------------
        pipeline_yaml  = self.pipeline_template.render(**context)
        pipeline_file  = pipeline_dir / f"{schema.table_name}.yaml"
        pipeline_file.write_text(pipeline_yaml, encoding="utf-8")
        print(f"    → wrote {pipeline_file}")


# -------------------------------------------------------------------------------------------------
#  SECTION 4 — DEMO / ENTRY POINT
#
#  When you run  `python generate.py`  we:
#    1. Write sample YAML config files to configs/
#    2. Run the generator against those files
#  This makes the script self-contained for review and testing.
# -------------------------------------------------------------------------------------------------

# --- Sample YAML configs (would normally live in a configs/ repo) ---

SAMPLE_USERS_YAML = """\
table_name: users
database: analytics
owner: data-platform-team

columns:
  - name: user_id
    data_type: BIGINT
    nullable: false
    description: "Unique user identifier (surrogate key)"
  - name: email
    data_type: STRING
    nullable: false
    description: "User email address"
  - name: created_at
    data_type: TIMESTAMP
    nullable: false
    description: "Account creation timestamp"
  - name: country_code
    data_type: STRING
    nullable: true
    description: "ISO 3166-1 alpha-2 country code"
  - name: is_active
    data_type: BOOLEAN
    nullable: false
    description: "Whether the account is currently active"

partition:
  columns: [created_at]
  strategy: range

pipeline:
  schedule: "@daily"
  load_type: incremental
  primary_key: [user_id]
  watermark_column: created_at

tags: [pii, core-entity]
"""

SAMPLE_EVENTS_YAML = """\
table_name: page_events
database: analytics
owner: product-analytics-team

columns:
  - name: event_id
    data_type: STRING
    nullable: false
    description: "UUID for this event"
  - name: user_id
    data_type: BIGINT
    nullable: true
    description: "FK to users.user_id (null for anonymous)"
  - name: event_type
    data_type: STRING
    nullable: false
    description: "e.g. page_view, click, form_submit"
  - name: occurred_at
    data_type: TIMESTAMP
    nullable: false
    description: "When the event happened (UTC)"
  - name: properties
    data_type: JSON
    nullable: true
    description: "Freeform event metadata"

partition:
  columns: [occurred_at]
  strategy: range

pipeline:
  schedule: "0 * * * *"   # hourly
  load_type: incremental
  primary_key: [event_id]
  watermark_column: occurred_at

tags: [clickstream, high-volume]
"""

# --- A deliberately broken YAML to showcase Pydantic validation ------

SAMPLE_BROKEN_YAML = """\
table_name: broken_table
database: analytics
owner: ""

columns:
  - name: weird_col
    data_type: FOOBARTYPE    # ← invalid: not in ALLOWED_DATA_TYPES
    nullable: false

pipeline:
  load_type: magic_load      # ← invalid: not in {full, incremental, snapshot}
"""


def setup_demo_configs() -> None:
    """Write sample YAML files so the demo runs without external files."""
    config_dir = Path("configs")
    config_dir.mkdir(exist_ok=True)

    (config_dir / "users.yaml").write_text(SAMPLE_USERS_YAML)
    (config_dir / "page_events.yaml").write_text(SAMPLE_EVENTS_YAML)
    (config_dir / "broken_table.yaml").write_text(SAMPLE_BROKEN_YAML)

    print("Demo configs written to configs/")


if __name__ == "__main__":
    print("\n" + "═"*60)
    print("  SQL + Pipeline Config Generator — Demo Run")
    print("═"*60)

    # 1. Create sample YAML configs
    setup_demo_configs()

    # 2. Instantiate the generator (could also accept CLI args here
    #    e.g. via argparse to choose environments or a config directory)
    generator = SQLGenerator(
        config_dir="configs",
        output_dir="output",
        environments=["dev", "staging", "prod"],
    )

    # 3. Run — validates every YAML, renders templates, writes files
    generator.run()
