from dr.uc_dr_objects.objects import ObjectType
from jinja2 import Template

BASE_DDL = {
    "CATALOG": ("""CREATE {% if catalog_type.lower() == 'foreign' and not delta_sharing|default(false) %}FOREIGN {% endif %}CATALOG {% if not_exists|default(false) %}IF NOT EXISTS{% endif %} `{{ catalog_name }}`
                {%- if delta_sharing|default(false) and catalog_type.lower() != 'foreign' %}\nUSING SHARE `{{ provider_name }}`.`{{ share_name }}`{%- endif %}
                {%- if catalog_type.lower() == 'foreign' and not delta_sharing|default(false) %}\nUSING CONNECTION {{ connection_name }}{%- endif %}
                {%- if storage_root|default(false) %}\nMANAGED LOCATION '{{ storage_root.lower() }}'{%- endif %}
                {%- if comment|default(false) %}\nCOMMENT '{{ comment }}'{%- endif %}
                {%- if catalog_type.lower() == 'foreign' and options|default(false) and not delta_sharing|default(false) %}\nOPTIONS (
                {%- for key, value in options.items() %}\n\t{{ key }} '{{ value }}'{% if not loop.last %},{% endif %}{%- endfor %}\n)
                {%- endif %}"""),

    "SCHEMA": ("""CREATE SCHEMA {% if not_exists|default(false) %}IF NOT EXISTS{% endif %} {{ schema_name }}
                   {%- if comment|default(false) %}\nCOMMENT '{{ comment }}'{%- endif %}
                   {%- if managed_location|default(false) %}\nMANAGED LOCATION '{{ managed_location }}'{%- endif %}
                   {%- if properties|default(false) %}\nWITH DBPROPERTIES (
                   {%- for key, value in properties.items() %}\n\t'{{ key }}' = '{{ value }}'{% if not loop.last %},{% endif %}{%- endfor %}\n)
                   {%- endif %}"""),

    "TABLE": ("""CREATE {% if replace|default(false) and not table_type|default(false) == 'EXTERNAL' %}OR REPLACE {% endif %}{% if not replace|default(false) and table_type|default(false) == 'EXTERNAL' %}EXTERNAL {% endif %}TABLE {% if not_exists|default(false) %}IF NOT EXISTS{% endif %} {{ full_name }}
                    {%- if columns_ddl|default(false) %}\n({{ columns_ddl }}){%- endif %}
                    {%- if data_source_format|default(false) %}\nUSING {{ data_source_format }}{%- endif %}
                    {%- if table_type|default(false) != 'MANAGED' and storage_location|default(false) %}\nLOCATION '{{ storage_location }}'{%- if storage_credential|default(false) %} WITH (CREDENTIAL {{ storage_credential }}){%- endif %}{%- endif %}
                    {%- if not cluster_columns|default(false) and partition_columns|default(false) %}\nPARTITIONED BY ({{ partition_columns }}){%- endif %}
                    {%- if not partition_columns|default(false) and cluster_columns|default(false) %}\nCLUSTER BY ({{ cluster_columns }}){%- endif %}
                    {%- if comment|default(false) %}\nCOMMENT '{{ comment }}'{%- endif %}
                    {%- if properties|default(false) %}\nTBLPROPERTIES (
                    {%- for key, value in properties.items() %}\n\t'{{ key }}' = '{{ value }}'{% if not loop.last %},{% endif %}{%- endfor %}\n)
                    {%- endif %}
                    {%- if row_filter|default(false) %}\nWITH {{ row_filter }}{%- endif %}"""),

    "VOLUME": ("""CREATE {% if type|default(false) == 'EXTERNAL' %}EXTERNAL {% endif %}VOLUME {% if not_exists|default(false) %}IF NOT EXISTS{% endif %} {{ volume_name }}
                   {%- if type|default(false) == 'EXTERNAL' and storage_location|default(false) %}\nLOCATION '{{ storage_location }}'{%- endif %}
                   {%- if comment|default(false) %}\nCOMMENT '{{ comment }}'{%- endif %}"""),

    "FUNCTION": ("""CREATE {% if replace|default(true) and not not_exists|default(false) %}OR REPLACE {% endif %}FUNCTION {% if not replace|default(true) and not_exists|default(false) %}IF NOT EXISTS{% endif %} {{ function_name }}(
                   {%- for input in inputs %}
                        {{ input }}{% if not loop.last %},{% endif %}
                   {%- endfor %})
                   {%- if type|default(false) == 'scalar' %}\nRETURNS {{ return[0] }}{%- endif %}
                   {%- if type|default(false) == 'table' %}\nRETURNS TABLE ({%- for ret in return %}
                        {{ ret }}{% if not loop.last %},{% endif %}
                   {%- endfor %}){%- endif %}
                   {%- if language|default(false) %}\nLANGUAGE {{ language }}{%- endif %}
                   {%- if comment|default(false) %}\nCOMMENT '{{ comment }}'{%- endif %}
                   {%- if body|default(false) and language|default(false) != 'python' %}\nRETURN {{ body }}{%- endif %}
                   {%- if body|default(false) and language|default(false) == 'python' %}\nAS $$ {{ body }} $${%- endif %}"""),

    "CONNECTION": ("""CREATE {% if replace|default(true) and not not_exists|default(false) %}OR REPLACE {% endif %}CONNECTION {% if not replace|default(true) and not_exists|default(false) %}IF NOT EXISTS{% endif %} `{{ connection_name }}`
                   {%- if connection_type|default(true) %}\nTYPE {{ connection_type }}{%- endif %}
                   {%- if options|default(false) %}\nOPTIONS (
                   {%- for key, value in options.items() %}\n\t{{ key }} '{{ value }}'{% if not loop.last %},{% endif %}{%- endfor %}\n)
                   {%- endif %}
                   {%- if comment|default(false) %}\nCOMMENT '{{ comment }}'{%- endif %}"""),

    "EXTERNAL LOCATION": ("""CREATE EXTERNAL LOCATION {% if not_exists|default(false) %}IF NOT EXISTS{% endif %} `{{ external_location_name }}`
                    {%- if url %}\nURL '{{ url }}'{%- endif %}
                    {%- if storage_credential_name %}\nWITH (STORAGE CREDENTIAL `{{ storage_credential_name }}`){%- endif %}
                    {%- if comment|default(false) %}\nCOMMENT '{{ comment }}'{%- endif %}"""),

    "RECIPIENT": ("""CREATE RECIPIENT {% if not_exists|default(false) %}IF NOT EXISTS{% endif %}`{{ recipient_name }}`
                    {%- if delta_sharing_id|default(false) %}\nUSING ID '{{ delta_sharing_id }}'{%- endif %}
                    {%- if comment|default(false) %}\nCOMMENT '{{ comment }}'{%- endif %}"""),

    "SHARE": ("""CREATE SHARE {% if not_exists|default(false) %}IF NOT EXISTS{% endif %}`{{ share_name }}`
                    {%- if comment|default(false) %}\nCOMMENT '{{ comment }}'{%- endif %}"""),

    "DELTA SHARING SHARE OBJECT": ("""ALTER SHARE `{{ share_name }}`
                                    {%- if schema|default(false) %}\nADD SCHEMA {{ schema }}{%- endif %}
                                    {%- if table|default(false) %}\nADD TABLE {{ table }}{%- endif %}
                                    {%- if materialized_view|default(false) %}\nADD MATERIALIZED VIEW {{ materialized_view }}{%- endif %}
                                    {%- if view|default(false) %}\nADD VIEW {{ view }}{%- endif %}
                                    {%- if volume|default(false) %}\nADD VOLUME {{ volume }}{%- endif %}
                                    {%- if model|default(false) %}\nADD MODEL {{ model }}{%- endif %}
                                    {%- if comment|default(false) %}\nCOMMENT '{{ comment }}'{%- endif %}
                                    {%- if table|default(false) and partition|default(false) %}\nPARTITION {{ partition }}{%- endif %}
                                    {%- if alias|default(false) and ( materialized_view|default(false) or table|default(false) or view|default(false) or model|default(false) ) %}\nAS {{ alias }}{%- endif %}
                                    {%- if table|default(false) and history|default(false) %}\nWITH HISTORY{%- endif %}""")
}


class DDL:

    def __init__(self, object_type: ObjectType) -> None:
        self._raw_ddl = Template(BASE_DDL[object_type.value])

    def __call__(self, **kwargs) -> str:
        return self._compile(self._raw_ddl, **kwargs)
    
    def _compile(cls, raw_ddl: Template, **kwargs) -> str:
        return raw_ddl.render(**kwargs)