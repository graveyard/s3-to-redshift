# redshifter

`redshifter` is responsible for getting data from any source into AWS Redshift for data analysis.

## Motivation

[AWS Redshift](http://aws.amazon.com/redshift/) is a columnar storage based data warehouse solution.
It is optimized for analysis and business intelligence and has integration with many tools.

## Features

Right now this repository contains a single script that exports events data from Mixpanel into
Redshift via S3.

If the repository grows to include more use cases, this should be changed to a more of a
plugin/mix-in model.

## Running

This script should be run daily using a cron job.

TODO: details
