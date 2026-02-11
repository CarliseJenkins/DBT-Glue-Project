# DBT-Glue-Project

# Objective - In this project, I'm  utilizing AWS Glue to extract data from an external API and store it in JSON format within an S3 bucket. 

After the data has been loaded into the S3 bucket, we will employ dbt (Data Build Tool) to transfer the data from the S3 bucket into a staging table in Snowflake, using dbt macros for automation. 

Additionally, we are taking advantage of dbt's modeling capabilities to create the necessary tables in Snowflake across different layers: raw, transform, and mart.

