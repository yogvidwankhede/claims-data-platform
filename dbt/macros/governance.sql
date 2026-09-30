{#-
  Carries RAW's governance into the marts on Snowflake:
    * columns with `meta: {pii: <kind>}` get the GOVERNANCE.POLICIES.PII tag, whose
      masking policy then applies automatically (see snowflake/migrations/V004);
    * models with `meta: {row_access_column: <col>}` get the line-of-business row
      access policy on that column.
  Both are idempotent: SET TAG overwrites, and the policy is only added when the
  table does not already carry one (an incremental table keeps its policy between
  runs; a rebuilt table starts without one). A no-op on DuckDB, which has no policies.
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
    {%- if rac and not _has_row_access_policy(this) -%}
        {%- do statements.append(
            "alter table " ~ this ~ " add row access policy GOVERNANCE.POLICIES.LOB_ROW_ACCESS on ("
            ~ rac ~ ")") -%}
    {%- endif -%}
    {{ return(statements | join(';\n')) }}
{% endmacro %}

{% macro _has_row_access_policy(relation) %}
    {%- set sql -%}
        select count(*)
        from table({{ relation.database }}.information_schema.policy_references(
            ref_entity_name => '{{ relation }}', ref_entity_domain => 'table'))
        where policy_kind = 'ROW_ACCESS_POLICY'
    {%- endset -%}
    {{ return(run_query(sql).columns[0].values()[0] > 0) }}
{% endmacro %}
