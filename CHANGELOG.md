# Changelog

## [3.0.1] - 2025-09-09

### Fixed

- fixed issue with child abort-callbacks causing jobs to crash (in certain environments)

## [3.0.0] - 2025-09-09

### Changed

- migrated to dcm-common v4
- migrated to import-module API v7
- migrated to ip-builder API v6
- migrated to object-validator API v6
- migrated to preparation-module API v1
- migrated to sip-builder API v3
- migrated to transfer-module API v3
- migrated to backend API v3
- **Breaking:** migrated to API v2

## [2.3.0] - 2025-08-20

### Changed

- changed to running task-submission with retries and submission token

### Added

- added support for submission token

## [2.1.2] - 2025-08-18

### Fixed

- fixed `DELETE-/process` not updating final result in database
- fixed `on_abort`-handler not writing final result to database

## [2.1.1] - 2025-08-14

### Fixed

- finalized uncompleted parts for migrated to new extension system

## [2.1.0] - 2025-08-08

### Added

- added validation of schema version to database initialization extension

## [2.0.1] - 2025-08-06

### Fixed

- fixed problematic re-use of existing database connections in child-processes

## [2.0.0] - 2025-07-25

### Changed

- **Breaking:** migrated to Backend API v2
- **Breaking:** migrated to IP Builder API v5
- **Breaking:** changed default ports for other dcm-services

### Added

- added `REQUEST_TIMEOUT` environment variable and use it in the service adapters
- **Breaking:** added execution context to `POST-/process`-endpoint
- added `prepare_ip` Stage
- **Breaking:** replaced key-value store-database with sql-database

### Fixed

- fix orchestrator initialization (missing `nprocesses`-arg)

## [1.0.2] - 2025-04-30

### Fixed

- fixed errors due to premature attempt at record-export during job-processing in bootstrap-phase

## [1.0.1] - 2025-02-17

### Added

- increased supported range of `dcm-backend-sdk` versions (now supporting v0 and v1)

## [1.0.0] - 2025-02-14

### Changed

- **Breaking:** migrate to latest Import Module API (v6)
- **Breaking:** migrate to latest Object Validator API (v5)
- **Breaking:** migrate to latest IP Builder API (v4)

## [0.1.2] - 2024-11-21

### Changed

- updated package metadata, Dockerfiles, and README

### Removed

- removed unused volume in `compose.yml`

## [0.1.0] - 2024-10-16

### Changed

- initial release of dcm-job-processor
