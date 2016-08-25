FROM ubuntu:14.04

RUN apt-get update -y && \
    apt-get install -y ca-certificates && \
    apt-get update -y && \
    apt-get install -y curl

RUN curl -L https://github.com/Clever/gearcmd/releases/download/v0.8.4/gearcmd-v0.8.4-linux-amd64.tar.gz | tar xz -C /usr/local/bin --strip-components 1

COPY bin/s3-to-redshift /usr/bin/s3-to-redshift

CMD ["gearcmd", "--name", "s3-to-redshift", "--cmd", "s3-to-redshift", "--cmdtimeout",  "30m", "--retry", "1", "--warningLength", "20"]
