"""Test-module for process-endpoint."""

from uuid import uuid4
from time import sleep, time

from flask import Response

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
    client = app_factory(testing_config()).test_client()

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
    client, minimal_request_body, import_report, run_service, temp_folder
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
