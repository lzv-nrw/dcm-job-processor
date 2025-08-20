from setuptools import setup


setup(
    version="2.3.0",
    name="dcm-job-processor",
    description="flask app implementing the DCM Job Processor API",
    author="LZV.nrw",
    license="MIT",
    python_requires=">=3.10",
    install_requires=[
        "flask==3.*",
        "PyYAML==6.*",
        "data-plumber-http>=1.0.0,<2",
        "dcm-common[services, db, orchestration]>=3.28.0,<4",
        "dcm-database>=1.0.0,<2",
        "dcm-import-module-sdk>=6.2.0,<7",
        "dcm-ip-builder-sdk>=5.1.0,<6",
        "dcm-object-validator-sdk>=5.1.0,<6",
        "dcm-preparation-module-sdk>=0.2.0,<1",
        "dcm-sip-builder-sdk>=2.2.0,<3",
        "dcm-transfer-module-sdk>=2.2.0,<3",
        "dcm-backend-sdk>=2.5.0,<3",
        "dcm-job-processor-api>=1.0.0,<2",
    ],
    packages=[
        "dcm_job_processor",
        "dcm_job_processor.components",
        "dcm_job_processor.components.processor",
        "dcm_job_processor.components.service_adapter",
        "dcm_job_processor.extensions",
        "dcm_job_processor.models",
        "dcm_job_processor.views",
    ],
    extras_require={
        "cors": ["Flask-CORS==4"],
    },
    setuptools_git_versioning={
          "enabled": True,
          "version_file": "VERSION",
          "count_commits_from_version_file": True,
          "dev_template": "{tag}.dev{ccount}",
    },
)
