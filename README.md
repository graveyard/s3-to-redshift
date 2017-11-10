# s3-to-redshift

[![GoDoc](https://godoc.org/github.com/Clever/s3-to-redshift?status.svg)](https://godoc.org/github.com/Clever/s3-to-redshift)

`s3-to-redshift` is responsible for syncing data from `s3` into AWS Redshift for data analysis.

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
- Modifying the destination `Redshift` tables, if necessary
- Refreshing the latest `Redshift` data (see: the `granularity` flag) by efficiently loading s3 data using the `COPY` command

## Running

We split up environment variables and command line flags based on realistic ways one might run this in production.
Essentially, for data that corresponds to `Redshift` or `s3` connections, the config is stored in environment variables.
Otherwise, it is likely one will want to change data such as `schema` or `table` between runs, so information like that is expected in flag form.

### Running locally:

Testing can be done manually by running `s3-to-redshift` locally with the desired parameters:

```
SCHEMA=api TABLES=business_metrics_auth_counts ark start s3-to-redshift -e clever-dev -l
```

See the Makefile for a complete list of parameters you can use for testing.

### Possible flags and their meanings:
- `schema`: destination `Redshift` schema to insert into
- `tables`: destination `Redshift` tables to insert into, comma separated
- `bucket`: `s3` bucket to pull from
- `truncate`: clear the table before inserting
- `force`: refresh the data even if the data date is after the current `s3` input date
- `date`:  the date string for the data in question
- `config`: override of the usual auto-discovery of the config
- `delimiter`: required to use CSV files, what the file is delimited in (likely use the '|' pipe character as that is AWS' default). If `""` then JSON copy is assumed
- `granularity`: how often we expect to append new data for each table (i.e. daily, or hourly buckets)
- `timezone`: specifies what timezone the target data is in (i.e. 'America/Los_Angeles'). Must be in the IANA Time Zone database.

#### Note on general usage:

This worker is intended to have a good amount of power and intelligence, instead of being a simple connector.
Thus, it looks for the right data automatically, and compares against what's in `Redshift` already.

After `s3-to-redshift` has determined the s3 file exists, the worker inspects the target `Redshift` table.
- If there is not data in the table, no checks are needed and the process continues.
- If there is already data in the table, `s3-to-redshift` finds the column that corresponds to the date of that data and compares with the date of the latest data in `Redshift`.

Note that this "data date" is not necessarily the date the data itself was written to disk - it is not modified time, but instead the actual time the data was collected at its source.

#### Using `--date`
The date parameter is required, and should match the date in the file name of the data file to transform.

This parameter should be the specific, full RFC3999 date, such as: `--date=2015-07-01T00:00:00Z`

#### Using `--force`
When the data already in the database is newer by "data date" than the data in `s3`, we do not overwrite it or insert it.
This should protect us from accidental duplicate information or replacing newer data with older data.
However, if you do need to overwrite, passing the `--force` flag will skip this check.

The `--force` flag may be useful when:
- Business logic has changed and data needs to be overwritten
- An upstream process has written incorrect data which needs to be reinserted into `Redshift`
- Upstream processes write data out-of-order by design, and each run of `s3-to-redshift` is invoked with the `force` parameter

#### Using `--config`
In normal operation, the worker looks for a config file for each schema/table combination.
This takes the form: `config_<data filename without suffix>.yml`

The worker looks for a config file with that date as the data timestamp.

Your upstream producer might not want to write a config file for each set of data, or perhaps you have a central configuration location.
In this case, you can use the `--config` parameter to pass a specific config file.
This file is accessed via [Pathio](https://github.com/Clever/pathio), so the file may reside on `s3` or locally.

#### Using `--truncate`
Without the `--truncate` option set, `s3-to-redshift` will insert into an existing table but leave any data already remaining in the table (except for the most recent data within the past granularity time range, which will be refreshed as new syncs come in).

Additionally, `s3-to-redshift` will not insert or overwrite for a particular time period thus the worker is idempotent and duplicate data is not a concern.

If you instead are adding snapshot / dimension data to `Redshift`, you should use the `--truncate` option to clear out the existing data before inserting the current "state of the world".

*One caveat:* the `--truncate` option does not also imply `--force`!
If the data in `s3` is not newer than the data in `Redshift`, the worker will refuse to truncate and replace the data without `--force`.

Also please note that this can cause performance problems if you are not running a vacuum at least weekly.

#### Using `--granularity`
The `--granularity` flag describes how often we expect to append new data to the destination table. For instance, perhaps we would like to track daily school counts in `Redshift`. Therefore, we expect one set of values per day to be stored in this table (and we specify this with `--granularity=day`). Multiple `s3-to-redshift` syncs updating the daily school count can still happen each day, but only the most recent sync data will be stored (as `s3-to-redshift` will simply overwrite the existing school counts for the most recent day). As a result, `s3-to-redshift` refreshes data in the latest time range, while leaving historical data untouched (and modifiable only via `--force`). The width of this time range is specified by `--granularity`.

We also support a 'streaming' granularity which means that we don't have a fixed granularity and we can pull data whenever. If the granularity is streaming we only add new data to the table.

Currently supported granularities are `hour`, `day`, `stream`.

### Example run:
Assuming that environment variables have been set:
```
go run cmd/main.go -schema=api_hits -tables=pages,sessions \
  -bucket=analytics -config=s3://analytics/api.yml -date=2015-07-01T00:00:00Z -force=true -delimiter="|"
```

## Vendoring

Please view the [dev-handbook for instructions](https://github.com/Clever/dev-handbook/blob/master/golang/godep.md).
