FROM ubuntu:14.04

RUN apt-get update -y && \
    apt-get install -y ca-certificates

COPY kvconfig.yml /usr/bin/kvconfig.yml
COPY bin/s3-to-redshift /usr/bin/s3-to-redshift
COPY bin/sfncli /usr/bin/sfncli

CMD ["sfncli", "--cmd", "/usr/bin/s3-to-redshift", "--activityname", "${_DEPLOY_ENV}--${_APP_NAME}", "--region", "us-west-2", "--workername", "MAGIC_ECS_TASK_ARN"]
