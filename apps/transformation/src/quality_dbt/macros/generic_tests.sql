{#
  Generic tests used by models/sources.yml. A generic test returns the rows that violate it;
  dbt counts them as failures. Kept local (no dbt packages) so nothing is downloaded at run time.
#}

{% test non_negative(model, column_name) %}
select {{ column_name }} from {{ model }} where {{ column_name }} < 0
{% endtest %}

{% test not_in_future(model, column_name, tolerance_minutes=60) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} > now() + interval '{{ tolerance_minutes }} minutes'
{% endtest %}

{# Fails (one row) when the newest value is older than max_age_hours, or the table is empty. #}
{% test recent(model, column_name, max_age_hours) %}
select max({{ column_name }}) as newest
from {{ model }}
having max({{ column_name }}) is null
    or max({{ column_name }}) < now() - interval '{{ max_age_hours }} hours'
{% endtest %}

{# Fails (one row) when more than max_fraction of the rows have a NULL in the column. #}
{% test max_null_fraction(model, column_name, max_fraction) %}
select count(*) filter (where {{ column_name }} is null) * 1.0 / nullif(count(*), 0) as null_fraction
from {{ model }}
having count(*) filter (where {{ column_name }} is null) * 1.0 / nullif(count(*), 0) > {{ max_fraction }}
{% endtest %}
