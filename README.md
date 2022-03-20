# Project background
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. I was tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

# Project description
- The "AWS Redshift setup & cleanup.ipynb" file programmatically create/access/delete the Redshift cluster and IAM role using a python library boto (IaC).
- The "create_tables.py" file drops/creates all the staging and analytics tables.
- The "etl.py" file first stage the data from S3 to Redshift then transform the data into a star schema.
