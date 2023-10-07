# CVM-SM-pipelines

# Structure

## Lake

## Jobs

# CI/CD

CI/CD approach is described in a separate doc section: [Branching flow](./doc/cicd/branching.md)

# Deployment

Local environment setup

```commandline
pip install pyenv --user
pip install pipenv --user
pyenv install 3.10
curl -fsSL https://get.pulumi.com | sh

cp .env.sample .env
vim .env # edit your password
source .env
```

All below pulumi command must be run within virtualenv created by pipenv.
We can switch to it with `pipenv shell` command.

Use local state

```commandline
cd ./lake
pulumi login file://./../
```

Deploy Jobs to DEV

```commandline
cd ./jobs
pulumi state select cvm-jobs-dev
pulumi up 
```

Deploy Jobs to PROD

```commandline
cd ./jobs
pulumi state select cvm-jobs-prod
pulumi up 
```

Deploy Lake to DEV

```commandline
cd ./lake
pulumi state select cvm-lake-dev
pulumi up 
```

Deploy Lake to PROD

```commandline
cd ./lake
pulumi state select cvm-lake-prod
pulumi up 
```
