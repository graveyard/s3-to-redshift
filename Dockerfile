FROM debian:stretch
RUN apt-get -y update && \
    apt-get install -y ca-certificates
COPY kvconfig.yml /usr/bin/kvconfig.yml
COPY bin/s3-to-redshift /usr/bin/s3-to-redshift
COPY bin/sfncli /usr/bin/sfncli
CMD ["/usr/bin/sfncli", "--activityname", "${_DEPLOY_ENV}--${_APP_NAME}", "--region", "us-west-2", "--cloudwatchregion", "${_POD_REGION}", "--workername", "MAGIC_ECS_TASK_ID", "--cmd", "/usr/bin/s3-to-redshift"]
