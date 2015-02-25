# redshifter

`redshifter` is responsible for getting data from any source into AWS Redshift for data analysis.

## Motivation

[AWS Redshift](http://aws.amazon.com/redshift/) is a columnar storage based data warehouse solution.
It is optimized for analysis and business intelligence and has integration with many tools.

## Features

Redshifter contains libraries for transferring data from various data sources like postgres,
mixpanel to [s3](http://aws.amazon.com/s3/) and a library to copy data into redshift from s3.

The details of each of the libraries can be found in their respective READMEs.

## Running

Mixpanel data can be exported to redshift using the following command. Note that all the options
specified are required to run the script.

```
$ AWS_ACCESS_KEY_ID=<access_key_id> \
AWS_SECRET_ACCESS_KEY=<secret_access_key> \
AWS_REGION=<s3_bucket_region> \
godep go run mixpanel_to_redshift.go \
-redshifthost=<redshift_host_url> \
-redshiftport=<redshift_port> \
-redshiftuser=<redshift_username> \
-redshiftpassword=<redshift_password> \
-redshiftdatabase=<redshift_database_name> \
-redshiftschema=<redshift_schema> \
-redshifttable=<redshift_table>
-jsonpathsfile=<jsonpaths_file> \
-mixpanelevents=<mixpanel_events_csv> \
-exportdir=<s3_export_dir> \
-mixpanelapikey=<api_key> \
-mixpanelapisecret=<api_secret>
```

There are other optional flags that can be found using:

```
godep go run mixpanel_to_redshift.go --help
```

## Testing

Tests for the repository can be run using `$ make test`
