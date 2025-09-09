from setuptools import setup


setup(
    version="3.0.0",
    name="dcm-job-processor",
    description="flask app implementing the DCM Job Processor API",
    author="LZV.nrw",
    license="MIT",
    python_requires=">=3.10",
    install_requires=[
        "flask==3.*",
        "PyYAML==6.*",
        "data-plumber-http>=1.0.0,<2",
        "dcm-common[services, db, orchestra]>=4.0.0,<5",
        "dcm-database>=1.0.0,<2",
        "dcm-import-module-sdk>=7.0.0,<8",
        "dcm-ip-builder-sdk>=6.0.0,<7",
        "dcm-object-validator-sdk>=6.0.0,<7",
        "dcm-preparation-module-sdk>=1.0.0,<2",
        "dcm-sip-builder-sdk>=3.0.0,<4",
        "dcm-transfer-module-sdk>=3.0.0,<4",
        "dcm-backend-sdk>=3.0.0,<4",
        "dcm-job-processor-api>=2.0.0,<3",
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
