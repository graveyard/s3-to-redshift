FROM ubuntu:14.04

RUN apt-get update -y && \
    apt-get install -y ca-certificates && \
    apt-get update -y && \
    apt-get install -y curl

RUN curl -L https://github.com/Clever/batchcli/releases/download/0.0.5/batchcli-v0.0.5-linux-amd64.tar.gz | tar xz -C /usr/local/bin --strip-components 1

COPY kvconfig.yml /usr/bin/kvconfig.yml
COPY bin/s3-to-redshift /usr/bin/s3-to-redshift

ENTRYPOINT ["batchcli", "--cmd", "s3-to-redshift"]
