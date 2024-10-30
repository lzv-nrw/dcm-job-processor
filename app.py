from dcm_job_processor import app_factory, config

app = app_factory(config.AppConfig())
