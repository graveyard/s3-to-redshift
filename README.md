# s3-to-redshift

`s3-to-redshift` is responsible for getting data from `s3` into AWS Redshift for data analysis.

*Note*: this repository formerly was called `redshifter`, but has been modified to fit a slightly different design pattern.

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

## Deploying

This is a standard mesos deploy, `fab mesos.apps.deploy:s3-to-redshift,env=production`

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

#### Note on general usage:

This worker is intended to have a good amount of power and intelligence, instead of being a simple connector.
Thus, it looks for the right data automatically, and compares against what's in `Redshift` already.

After `s3-to-redshift` has determined the correct date to process by looking at `s3`, the worker inspects the target `Redshift` table.
- If there is not data in the table, no checks are needed and the process continues.
- If there is already data in the table, `s3-to-redshift` finds the column that corresponds to the date of that data and compares with the date of the latest data in `Redshift`.

Note that this "data date" is not necessarily the date the data itself was written to disk - it is not modified time, but instead the actual time the data was collected at its source.

#### Using `--date`
In normal operation, the worker finds the latest data in `s3` and loads it into `Redshift`.
We look for the most recent file in `s3` by date included in filename timestamp, not modified or created time.
We also can choose the date to process, for instance if:
- Our worker has failed for a few days and we need to fill up the missing data
- Our upstream process did not write the correct data to `s3` and we need to specify a specific time to rerun

In this case, you pass the specific, full RFC3999 date, such as: `--date=2015-07-01T00:00:00Z`

#### Using `--force`
When the data already in the database is newer by "data date" than the data in `s3`, we do not overwrite it or insert it.
This should protect us from accidental duplicate information or replacing newer data with older data.
However, if you do need to overwrite, passing the `--force` flag will skip this check.

The `--force` flag may be useful when:
- Business logic has changed and data needs to be overwritten
- An upstream process has written incorrect data which needs to be reinserted into `Redshift`
- Upstream processes write data out-of-order by design, and each run of `s3-to-redshift` is invoked with the `force` and `date` parameters

#### Using `--config`
In normal operation, the worker looks for a config file for each schema/table combination.
This takes the form: `config_<data filename without suffix>.yml`

Note that if the `--date` parameter was specified, the worker looks for a config file with that date as the data timestamp.

Your upstream producer might not want to write a config file for each set of data, or perhaps you have a central configuration location.
In this case, you can use the `--config` parameter to pass a specific config file.
This file is accessed via [Pathio](https://github.com/Clever/pathio), so the file may reside on `s3` or locally.

#### Using `--truncate`
Without the `--truncate` option set, `s3-to-redshift` will insert into an existing table but leave any data already remaining in the table.
Additionally, `s3-to-redshift` will not insert or overwrite for a particular time period thus the worker is idempotent and duplicate data is not a concern.
This behavior is ideal if you are adding time-series data / fact data to `Redshift`.

If you instead are adding snapshot / dimension data to `Redshift`, you should use the `--truncate` option to clear out the existing data before inserting the current "state of the world".

*One caveat:* the `--truncate` option does not also imply `--force`!
If the data in `s3` is not newer than the data in `Redshift`, the worker will refuse to truncate and replace the data without `--force`.

Also please note that this can cause performance problems if you are not running a vacuum at least weekly.

### Example run:
Assuming that environment variables have been set:
```
go run cmd/s3_to_redshift.go -schema=api_hits -tables=pages,sessions \
  -bucket=analytics -config=s3://analytics/api.yml -date=2015-07-01T00:00:00Z -force=true
```

## Vendoring

Please view the [dev-handbook for instructions](https://github.com/Clever/dev-handbook/blob/master/golang/godep.md).
