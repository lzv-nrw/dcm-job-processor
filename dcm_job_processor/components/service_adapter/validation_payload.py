"""
This module defines the `VALIDATION_PAYLOAD-ServiceAdapter`.
"""

import dcm_object_validator_sdk

from dcm_job_processor.models.job_config import Stage
from .validation_metadata import ValidationMetadataAdapter


class ValidationPayloadAdapter(ValidationMetadataAdapter):
    """`ServiceAdapter` for the `VALIDATION_PAYLOAD`-`Stage`."""
    _STAGE = Stage.VALIDATION_PAYLOAD
    _SERVICE_NAME = "Object Validator"
    _SDK = dcm_object_validator_sdk
