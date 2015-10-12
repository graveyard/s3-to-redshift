# redshifter

`redshifter` is responsible for getting data from any source into AWS Redshift for data analysis.
Currently, its only source of data is files on `s3`, a pattern which we should strive to keep for simplicity's sake.

## Motivation

[AWS Redshift](http://aws.amazon.com/redshift/) is a columnar storage based data warehouse solution.
It is optimized for analysis and business intelligence and has many useful integrations.

However, getting bulk data into `Redshift` can be tricky and requires many steps, like:
- Modifying tables in `Redshift`
- Modifying whatever process is collecting your data
- Modifying whatever process is submitting your data into `Redshift`

We are trying to minimize the amount of work to add or modify data going into `Redshift` by automatically:
- Finding the latest data
- Modifying the destination `Redshift` tables
- Loading in the data efficiently using the `COPY` command

## Running

We split up environment variables and command line flags based on realistic ways one might run this in production.
Essentially, for data that corresponds to `Redshift` or `s3` connections, the config is stored in environment variables.
Otherwise, it is likely one will want to change data such as `schema` or `table` between runs, so information like that is expected in flag form.

### Running the `s3-to-redshift` worker:

```
$ AWS_ACCESS_KEY_ID=<access_key_id> \
AWS_SECRET_ACCESS_KEY=<secret_access_key> \
AWS_REGION=<s3_bucket_region> \
REDSHIFT_HOST=<redshift_cluster_host> \
REDSHIFT_PORT=5439 \
REDSHIFT_DB=<redshift_db_name> \
REDSHIFT_USER=<redshift_user> \
REDSHIFT_PASSWORD=<redshift_pass> \
go run cmd/s3_to_redshift.go \
-schema=<target_schema> \
-tables=<target_tables> \
-bucket=<s3_bucket_to_pull_from> \
```

All environment variables are required.
The `schema`, `tables`, and `bucket` flags also are critical.

### Possible flags and their meanings:
- `schema`: destination `Redshift` schema to insert into
- `tables`: destination `Redshift` tables to insert into, comma separated
- `bucket`: `s3` bucket to pull from
- `truncate`: clear the table before inserting
- `force`: refresh the data even if the data date is after the current `s3` input date
- `date`: override to look for a specific date instead of the most recent item
- `config`: override of the usual auto-discovery of the config

## Testing

Tests for the repository can be run using `$ make test`
