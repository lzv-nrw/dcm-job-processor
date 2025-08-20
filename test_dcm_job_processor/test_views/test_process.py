"""Test-module for process-endpoint."""

from uuid import uuid4
from time import sleep, time
from pathlib import Path

from flask import Response
from dcm_common import util

from dcm_job_processor import app_factory


def test_process_minimal(
    client, minimal_request_body, import_report, run_service, wait_for_report
):
    """Test basic functionality of /process-POST endpoint."""
    run_service(
        routes=[
            ("/import/external", lambda: (import_report["token"], 201), ["POST"]),
            ("/report", lambda: (import_report, 200), ["GET"]),
        ],
        port=8080
    )
    # submit job
    response = client.post(
        "/process",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    assert response.status_code == 201
    assert response.mimetype == "application/json"
    token = response.json["value"]

    # wait until job is completed
    json = wait_for_report(client, token)

    assert json["data"]["success"]
    assert len(json["children"]) == 1
    assert all(
        id_.startswith(import_report["token"]["value"])
        for id_ in json["children"]
    )


def test_process_database(
    testing_config,
    temp_folder,
    minimal_request_body,
    import_report,
    run_service,
    wait_for_report,
):
    """Test writing results to database in /process-POST endpoint."""
    # setup test
    # * persistent SQLite to support multiprocessing-based jobs
    testing_config.SQLITE_DB_FILE = Path(temp_folder / str(uuid4()))
    # * allow for many simultaneous jobs (second part of this test)
    testing_config.ORCHESTRATION_PROCESSES = 10
    # * initialize config/app (with extensions that initialize database)
    config = testing_config()
    app = app_factory(config, block=True)
    # * write minimal data into database
    template_id = config.db.insert("templates", {}).eval()
    job_config_id = config.db.insert(
        "job_configs", {"template_id": template_id}
    ).eval()
    user_config_id = config.db.insert(
        "user_configs", {"username": "a", "email": "b"}
    ).eval()
    datetime_triggered = util.now().isoformat()
    trigger_type = "manual"
    # * create client for tests
    client = app.test_client()

    run_service(
        routes=[
            ("/import/external", lambda: (import_report["token"], 201), ["POST"]),
            ("/report", lambda: (import_report, 200), ["GET"]),
        ],
        port=8080
    )
    # submit job
    token = client.post(
        "/process",
        json=minimal_request_body
        | {
            "context": {
                "jobConfigId": job_config_id,
                "userTriggered": user_config_id,
                "datetimeTriggered": datetime_triggered,
                "triggerType": trigger_type,
            }
        },
    ).json["value"]
    jobs = config.db.get_rows("jobs").eval()
    assert len(jobs) == 1
    assert jobs[0]["token"] == token
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until job is completed
    json = wait_for_report(client, token)

    assert json["data"]["success"]

    jobs = config.db.get_rows("jobs").eval()
    assert len(jobs) == 1
    assert all(
        col in jobs[0] and jobs[0][col] is not None
        for col in [
            "token",
            "status",
            "job_config_id",
            "user_triggered",
            "datetime_triggered",
            "trigger_type",
            "success",
            "datetime_started",
            "datetime_ended",
            "report",
        ]
    )
    assert jobs[0]["token"] == token
    assert jobs[0]["job_config_id"] == job_config_id
    assert jobs[0]["user_triggered"] == user_config_id
    assert jobs[0]["datetime_triggered"] == datetime_triggered
    assert jobs[0]["trigger_type"] == trigger_type
    assert jobs[0]["status"] == "completed"
    assert jobs[0]["success"]

    records = config.db.get_rows("records").eval()
    assert len(records) == 1
    assert all(
        col in records[0] and records[0][col] is not None
        for col in [
            "id",
            "job_token",
            "success",
            "report_id",
        ]
    )
    assert records[0]["job_token"] == token
    assert records[0]["success"]
    assert records[0]["report_id"] in jobs[0]["report"]["data"]["records"]

    # run many jobs at once and check for correct creation of records in
    # the database
    # this is only part of the test due to strange behavior related to the
    # combination of the SQLite-adapter and multiprocessing
    tokens = []
    for _ in range(10):
        tokens.append(
            client.post("/process", json=minimal_request_body).json["value"]
        )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until all jobs are completed
    for token in tokens:
        assert wait_for_report(client, token)["data"]["success"]
    assert len(config.db.get_rows("jobs").eval()) == 11
    assert len(config.db.get_rows("records").eval()) == 11

    # run job without writing records-data (triggerType "test")
    token = client.post(
        "/process",
        json=minimal_request_body | {"context": {"triggerType": "test"}},
    ).json["value"]
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until all jobs are completed
    assert wait_for_report(client, token)["data"]["success"]
    assert len(config.db.get_rows("jobs").eval()) == 12
    assert len(config.db.get_rows("records").eval()) == 11


def test_process_multiple_records(
    client, minimal_request_body, import_report, run_service, wait_for_report
):
    """
    Test basic functionality of /process-POST endpoint with two records.
    """
    import_report["data"]["IEs"]["ie1"] = import_report["data"]["IEs"]["ie0"].copy()
    import_report["data"]["IEs"]["ie1"]["path"] = "ie/69c8f178-f6dc-4453-a34a-6e95cb175810"
    import_report["data"]["IEs"]["ie1"]["sourceIdentifier"] = "test:oai_dc:ca940b40-6f89"
    run_service(
        routes=[
            ("/import/external", lambda: (import_report["token"], 201), ["POST"]),
            ("/report", lambda: (import_report, 200), ["GET"]),
        ],
        port=8080
    )
    # submit job
    response = client.post(
        "/process",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until job is completed
    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert len(json["children"]) == 2
    assert all(
        id_.startswith(import_report["token"]["value"])
        for id_ in json["children"]
    )


def test_process_multiple_stages(
    client, minimal_request_body, import_report, ip_builder_report,
    run_service, wait_for_report
):
    """
    Test basic functionality of /process-POST endpoint with two stages.
    """
    run_service(
        routes=[
            ("/import/external", lambda: (import_report["token"], 201), ["POST"]),
            ("/report", lambda: (import_report, 200), ["GET"]),
        ],
        port=8080
    )
    run_service(
        routes=[
            ("/build", lambda: (ip_builder_report["token"], 201), ["POST"]),
            ("/report", lambda: (ip_builder_report, 200), ["GET"]),
        ],
        port=8081
    )
    minimal_request_body["process"]["to"] = "build_ip"

    # submit job
    response = client.post(
        "/process",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until job is completed
    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert len(json["children"]) == 2
    assert all(
        id_.startswith(import_report["token"]["value"])
        or id_.startswith(ip_builder_report["token"]["value"])
        for id_ in json["children"]
    )


def test_process_no_success(
    client, minimal_request_body, import_report, run_service, wait_for_report
):
    """
    Test behavior of /process-POST endpoint with no success for one of
    two records.
    """
    import_report["data"]["IEs"]["ie1"] = import_report["data"]["IEs"]["ie0"].copy()
    import_report["data"]["IEs"]["ie1"]["path"] = "ie/69c8f178-f6dc-4453-a34a-6e95cb175810"
    import_report["data"]["IEs"]["ie1"]["sourceIdentifier"] = "test:oai_dc:ca940b40-6f89"
    import_report["data"]["IEs"]["ie1"]["fetchedPayload"] = False
    import_report["data"]["success"] = False
    run_service(
        routes=[
            ("/import/external", lambda: (import_report["token"], 201), ["POST"]),
            ("/report", lambda: (import_report, 200), ["GET"]),
        ],
        port=8080
    )
    # submit job
    response = client.post(
        "/process",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until job is completed
    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert len(json["children"]) == 2


def test_process_connection_error_first_stage(
    testing_config, client, minimal_request_body, wait_for_report
):
    """
    Test behavior of /process-POST endpoint with no connection on first
    stage.
    """
    # submit job
    response = client.post(
        "/process",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until job is completed
    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert (
        f"Cannot connect to service at '{testing_config.IMPORT_MODULE_HOST}'"
        in str(json)
    )


def test_process_connection_error_second_stage(
    testing_config, client, minimal_request_body, import_report, run_service,
    wait_for_report
):
    """
    Test behavior of /process-POST endpoint with no connection on second
    stage.
    """
    run_service(
        routes=[
            ("/import/external", lambda: (import_report["token"], 201), ["POST"]),
            ("/report", lambda: (import_report, 200), ["GET"]),
        ],
        port=8080
    )
    minimal_request_body["process"]["to"] = "build_ip"

    # submit job
    response = client.post(
        "/process",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until job is completed
    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert (
        f"Cannot connect to service at '{testing_config.IP_BUILDER_HOST}'"
        in str(json)
    )


def test_process_request_timeout(
    testing_config, minimal_request_body, run_service, wait_for_report
):
    """
    Test behavior of /process-POST endpoint with service timeout
    during submission.
    """

    testing_config.REQUEST_TIMEOUT = 0.01

    msg = "some message"
    def _import():
        sleep(2*testing_config.REQUEST_TIMEOUT)
        return msg, 400
    run_service(routes=[("/import/external", _import, ["POST"])], port=8080)

    client = app_factory(testing_config(), block=True).test_client()

    # submit job
    response = client.post(
        "/process",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until job is completed
    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert msg not in str(json)
    assert (
        f"Cannot connect to service at '{testing_config.IMPORT_MODULE_HOST}'"
        in str(json)
    )


def test_process_timeout(
    testing_config, minimal_request_body, import_report, run_service,
    wait_for_report
):
    """
    Test behavior of /process-POST endpoint with service timeout.
    """
    run_service(
        routes=[
            ("/import/external", lambda: (import_report["token"], 201), ["POST"]),
            ("/report", lambda: ({"token": import_report["token"]}, 503), ["GET"]),
        ],
        port=8080
    )

    testing_config.PROCESS_TIMEOUT = 0.1
    client = app_factory(testing_config(), block=True).test_client()

    # submit job
    response = client.post(
        "/process",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until job is completed
    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert (
        f"Service at '{testing_config.IMPORT_MODULE_HOST}' has timed out"
        in str(json)
    )


def test_process_abort(
    testing_config,
    minimal_request_body,
    import_report,
    run_service,
    temp_folder,
):
    """
    Test abort mechanism for jobs submitted to /process-POST endpoint.
    """

    file = temp_folder / str(uuid4())

    import_report_aborted = import_report.copy()
    import_report_aborted["progress"]["status"] = "aborted"

    def _get_report():
        if file.exists():
            return import_report_aborted, 200
        return import_report, 503

    def _delete():
        file.touch()
        return Response("OK", mimetype="text/plain", status=200)

    # setup test
    # * persistent SQLite to support multiprocessing-based jobs
    testing_config.SQLITE_DB_FILE = Path(temp_folder / str(uuid4()))
    # * initialize config/app (with extensions that initialize database)
    config = testing_config()
    client = app_factory(config, block=True).test_client()

    run_service(
        routes=[
            ("/import/external", lambda: (import_report["token"], 201), ["POST"]),
            ("/import", _delete, ["DELETE"]),
            ("/report", _get_report, ["GET"]),
        ],
        port=8080
    )
    # submit job
    token = client.post("/process", json=minimal_request_body).json["value"]
    assert not file.exists()
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until job is completed
    sleep(0.5)
    assert client.delete(
        f"/process?token={token}",
        json={"origin": "pytest-runner", "reason": "test abort"}
    ).status_code == 200

    # should be synchronous, so waiting can be skipped
    report = client.get(f"/report?token={token}").json
    assert report["progress"]["status"] == "aborted"
    assert "Aborting child" in str(report["log"])
    assert file.exists()

    # check result is written to database
    info = config.db.get_row("jobs", token).eval()
    assert info["status"] == "aborted"
    assert info.get("datetime_ended") is not None
    assert info["report"]["progress"]["status"] == "aborted"


def test_process_abort_non_running(testing_config, temp_folder):
    """
    Test abort written to database for jobs that did not run on request.
    """

    # setup test
    # * persistent SQLite to support multiprocessing-based jobs
    testing_config.SQLITE_DB_FILE = Path(temp_folder / str(uuid4()))
    # * initialize config/app (with extensions that initialize database)
    config = testing_config()
    client = app_factory(config, block=True).test_client()

    token = str(uuid4())

    # write info to database
    config.db.insert(
        "jobs",
        {
            "token": token,
            "status": "queued",
            "report": {
                "host": "",
                "token": {
                    "value": token,
                    "expires": False,
                },
                "progress": {
                    "status": "queued",
                    "verbose": "Job queued.",
                    "numeric": 0,
                },
            },
        },
    )

    # run abort
    assert client.delete(
        f"/process?token={token}",
        json={"origin": "pytest-runner", "reason": "test abort"}
    ).status_code == 200

    # check result is written to database
    info = config.db.get_row("jobs", token).eval()
    assert info["status"] == "aborted"
    assert info.get("datetime_ended") is not None


def test_process_multiple_stages_abort(
    client, minimal_request_body, import_report, ip_builder_report,
    run_service, temp_folder
):
    """Test basic abort of job with two stages."""
    run_service(
        routes=[
            ("/import/external", lambda: (import_report["token"], 201), ["POST"]),
            ("/report", lambda: (import_report, 200), ["GET"]),
        ],
        port=8080
    )

    file = temp_folder / str(uuid4())
    ip_builder_report_aborted = ip_builder_report.copy()
    ip_builder_report_aborted["progress"]["status"] = "aborted"

    def _get_report():
        if file.exists():
            return ip_builder_report_aborted, 200
        return ip_builder_report, 503

    def _delete():
        file.touch()
        return Response("OK", mimetype="text/plain", status=200)

    run_service(
        routes=[
            ("/build", lambda: (ip_builder_report["token"], 201), ["POST"]),
            ("/build", _delete, ["DELETE"]),
            ("/report", _get_report, ["GET"]),
        ],
        port=8081
    )
    minimal_request_body["process"]["to"] = "build_ip"

    # submit job
    token = client.post("/process", json=minimal_request_body).json["value"]
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until build_ip-stage reached
    report = {}
    time0 = time()
    while time() - time0 < 2:
        report = client.get(f"/report?token={token}").json
        if (
            "logId" in report.get(
                "data", {}
            ).get(
                "records", {}
            ).get(
                "test:oai_dc:f50036dd-b4ef", {}
            ).get(
                "stages", {}
            ).get(
                "build_ip", {}
            )
        ):
            break
    assert (
        "logId" in report.get(
            "data", {}
        ).get(
            "records", {}
        ).get(
            "test:oai_dc:f50036dd-b4ef", {}
        ).get(
            "stages", {}
        ).get(
            "build_ip", {}
        )
    )
    assert not report.get(
        "data", {}
    ).get(
        "records", {}
    ).get(
        "test:oai_dc:f50036dd-b4ef", {}
    ).get(
        "completed", False
    )

    # abort
    client.delete(
        f"/process?token={token}",
        json={"origin": "pytest-runner", "reason": "test abort"}
    )

    # should be synchronous, so waiting can be skipped
    report = client.get(f"/report?token={token}").json
    child_report = report["children"][
        report["data"]["records"]["test:oai_dc:f50036dd-b4ef"]["stages"]["build_ip"]["logId"]
    ]

    assert report["progress"]["status"] == "aborted"
    assert child_report["progress"]["status"] == "aborted"
    assert "Aborting child" in str(report["log"])


def test_process_submit_existing(
    testing_config, temp_folder, minimal_request_body
):
    """
    Test behavior for a previously submitted job.
    """

    # setup test
    # * persistent SQLite to support multiprocessing-based jobs
    testing_config.SQLITE_DB_FILE = Path(temp_folder / str(uuid4()))
    # * initialize config/app (with extensions that initialize database)
    config = testing_config()
    client = app_factory(config, block=True).test_client()

    token = str(uuid4())

    # write info to database
    config.db.insert(
        "jobs",
        {
            "token": token,
            "status": "queued",
            "report": {
                "host": "",
                "token": {
                    "value": token,
                    "expires": False,
                },
                "progress": {
                    "status": "queued",
                    "verbose": "Job queued.",
                    "numeric": 0,
                },
            },
        },
    )

    # submit
    response = client.post(
        "/process", json=minimal_request_body | {"token": token}
    )
    assert response.status_code == 201
    assert response.json.get("value") == token

    # orchestrator did not run any job
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    assert (
        client.get("/orchestration").json.get("registry", {}).get("size") == 0
    )

    # check no additional entry in database
    assert len(config.db.get_rows("jobs", token).eval()) == 1
