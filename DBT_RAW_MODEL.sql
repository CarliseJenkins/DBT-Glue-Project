


-- DBT - SQL SCHEMA GENERATION 
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%} 

{%- endmacro %}


--copy_into_snowflake.sql
  
{% macro copy_json(table_nm) %}

 

--Delete the data from the copy table before running the copy command

delete from {{var ('target_db') }}.{{var ('target_schema')}}.{{ table_nm }};

 

--Copy the data from the snowflake external stage to snowflake table

COPY INTO {{var ('target_db') }}.{{var ('target_schema')}}.{{ table_nm }}

FROM

(

SELECT

$1 AS DATA

FROM @{{ var('stage_name') }}

)

FILE_FORMAT = (TYPE = JSON)

FORCE = TRUE;


-- Passing variable to dbt_project.yml for copy into data into the target database

vars:

   target_db: GLUEDB

   target_schema: PUBLIC

   stage_name: GLUEDB.PUBLIC.GLUE_S3_STAGE


  -- country_details_raw.sql -- RAW DATA TABLE Creations

  {{

config

({

"materialized":'table',

"pre_hook": copy_json('COUNTRY_DETAILS_CP'),

"schema": 'RAW'

})

}}

 

WITH country_details_raw AS

(

SELECT X.VALUE AS SOURCE_DATA,

CURRENT_TIMESTAMP(6) AS INSERT_DTS

FROM {{source('country','COUNTRY_DETAILS_CP')}} A,

LATERAL FLATTEN (A.DATA) X

)

 

SELECT

CAST(SOURCE_DATA AS VARIANT) AS SOURCE_DATA,

CAST(INSERT_DTS AS TIMESTAMP(6)) AS INSERT_DTS

FROM country_details_raw

 
{% endmacro %}


-- raw.yml -- Define the differences source for the raw layer


version: 2

 

models:

    - name: country_details_raw

      description: "dbt model to create the country detail raw table"

      columns:

          - name: source_data

            description: "The primary key for this table"

            tests:

                - unique

                - not_null

sources:

    - name: country

      database: GLUEDB

      schema: PUBLIC

      tables:

        - name: COUNTRY_DETAILS_CP


