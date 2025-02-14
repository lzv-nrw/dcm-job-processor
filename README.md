# Digital Curation Manager - Job Processor

The 'DCM Job Processor'-API provides functionality to process jobs, i.e. a sequence of processing steps, in the DCM.
This repository contains the corresponding Flask app definition.
For the associated OpenAPI-document, please refer to the sibling package [`dcm-job-processor-api`](https://github.com/lzv-nrw/dcm-job-processor-api).

The contents of this repository are part of the [`Digital Curation Manager`](https://github.com/lzv-nrw/digital-curation-manager).

## Local install
Make sure to include the extra-index-url `https://zivgitlab.uni-muenster.de/api/v4/projects/9020/packages/pypi/simple` in your [pip-configuration](https://pip.pypa.io/en/stable/cli/pip_install/#finding-packages) to enable an automated install of all dependencies.
Using a virtual environment is recommended.

1. Install with
   ```
   pip install .
   ```
1. Configure service environment to fit your needs ([see here](#environmentconfiguration)).
1. Run app as
   ```
   flask run --port=8080
   ```
1. To manually use the API, either run command line tools like `curl` as, e.g.,
   ```
   curl -X 'POST' \
     'http://localhost:8080/process' \
     -H 'accept: application/json' \
     -H 'Content-Type: application/json' \
     -d '{
     "process": {
       "from": "import_ies",
       "to": "import_ies",
       "args": {
         "import_ies": { ... },
         "import_ips": { ... },
         "build_ip": { ... },
         "validation_metadata": { ... },
         "validation_payload": { ... },
         "build_sip": { ... },
         "transfer": { ... },
         "ingest": { ... },
       }
     },
     "id": "dab3e1bf-f655-4e57-938d-d6953612552b"
   }'
   ```
   or run a gui-application, like Swagger UI, based on the OpenAPI-document provided in the sibling package [`dcm-job-processor-api`](https://github.com/lzv-nrw/dcm-job-processor-api).

## Docker
Build an image using, for example,
```
docker build -t dcm/job-processor:dev .
```
Then run with
```
docker run --rm --name=job-processor -p 8080:80 dcm/job-processor:dev
```
and test by making a GET-http://localhost:8080/identify request.

For additional information, refer to the documentation [here](https://github.com/lzv-nrw/digital-curation-manager).

## Tests
Install additional dev-dependencies with
```
pip install -r dev-requirements.txt
```
Run unit-tests with
```
pytest -v -s
```

## Environment/Configuration
Service-specific environment variables are
* `PROCESS_TIMEOUT` [DEFAULT 30] service timeout duration in seconds
* `IMPORT_MODULE_HOST` [DEFAULT http://localhost:8080] Import Module host address
* `IP_BUILDER_HOST` [DEFAULT http://localhost:8081] IP Builder host address
* `OBJECT_VALIDATOR_HOST` [DEFAULT http://localhost:8082] Object Validator host address
* `SIP_BUILDER_HOST` [DEFAULT http://localhost:8083] SIP Builder host address
* `TRANSFER_MODULE_HOST` [DEFAULT http://localhost:8084] Transfer Module host address
* `BACKEND_HOST` [DEFAULT http://localhost:8085] Backend host address

Additionally this service provides environment options for
* `BaseConfig` and
* `OrchestratedAppConfig`

as listed [here](https://github.com/lzv-nrw/dcm-common#app-configuration).

# Contributors
* Sven Haubold
* Orestis Kazasidis
* Stephan Lenartz
* Kayhan Ogan
* Michael Rahier
* Steffen Richters-Finger
* Malte Windrath
* Roman Kudinov