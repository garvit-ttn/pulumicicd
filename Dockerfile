FROM python:3.10-slim

# Install build-time requirements
RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y curl awscli &&  \
    pip install -U setuptools pip &&  \
    pip install pipenv &&  \
    curl -fsSL https://get.pulumi.com | sh

# Add script and install dependencies
ADD ./ /pipelines
WORKDIR /pipelines

# Set up environment variables
ENV PIPENV_VENV_IN_PROJECT=1
ENV PATH /root/.pulumi/bin:$PATH

RUN pipenv install -vvvv

# If this command do not work please comment and run later in container
RUN pipenv run pulumi login 's3://cvm-pulumi-backend?region=eu-west-1&awssdk=v2&profile=maf-dev' --cwd=./lake

# run terminal
ENTRYPOINT (curl -fsSL https://get.pulumi.com | sh) && sh
