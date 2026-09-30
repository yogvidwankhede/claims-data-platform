{#-
  Carries RAW's governance into the marts on Snowflake:
    * columns with `meta: {pii: <kind>}` get the GOVERNANCE.POLICIES.PII tag, whose
      masking policy then applies automatically (see snowflake/migrations/V004);
    * models with `meta: {row_access_column: <col>}` get the line-of-business row
      access policy on that column.
  A no-op on DuckDB, which has no policies.
-#}
{% macro apply_governance() %}
    {%- if target.type != 'snowflake' or not execute -%}{{ return('') }}{%- endif -%}
    {%- set statements = [] -%}
    {%- for name, col in model.columns.items() -%}
        {%- if col.meta.get('pii') -%}
            {%- do statements.append(
                "alter table " ~ this ~ " modify column " ~ adapter.quote(name | upper)
                ~ " set tag GOVERNANCE.POLICIES.PII = '" ~ col.meta['pii'] ~ "'") -%}
        {%- endif -%}
    {%- endfor -%}
    {%- set rac = model.config.get('meta', {}).get('row_access_column') -%}
    {%- if rac and model.config.materialized != 'table' -%}
        {{ exceptions.raise_compiler_error(model.name ~ ": row_access_column needs a table materialization "
           ~ "(a policy re-added to a persisted incremental table would fail)") }}
    {%- endif -%}
    {%- if rac -%}
        {%- do statements.append(
            "alter table " ~ this ~ " add row access policy GOVERNANCE.POLICIES.LOB_ROW_ACCESS on ("
            ~ rac ~ ")") -%}
    {%- endif -%}
    {{ return(statements | join(';\n')) }}
{% endmacro %}
