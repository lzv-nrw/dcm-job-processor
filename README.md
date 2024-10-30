# dcm-job-processor

The 'DCM Job Processor'-service provides functionality to execute the entire DCM-processing pipeline by orchestrating the individual stages.

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

as listed [here](https://github.com/lzv-nrw/dcm-common/-/tree/dev?ref_type=heads#app-configuration).

# Contributors
* Sven Haubold
* Orestis Kazasidis
* Stephan Lenartz
* Kayhan Ogan
* Michael Rahier
* Steffen Richters-Finger
* Malte Windrath
