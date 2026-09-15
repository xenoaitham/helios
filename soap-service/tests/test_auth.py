"""HTTP-level auth tests against the live WSGI stack (inline literal URLs)."""
import os

import requests


def test_health_is_open():
    response = requests.get("http://127.0.0.1:18081/health", timeout=10)
    assert response.status_code == 200
    assert b"OrderManagement" in response.content


def test_wsdl_without_credentials_is_401():
    response = requests.get("http://127.0.0.1:18081/?wsdl", timeout=10)
    assert response.status_code == 401
    assert b"unauthorized" in response.content


def test_wsdl_with_wrong_password_is_401():
    # Derived at runtime from the env; no credential literal lives in this file.
    wrong = (os.environ["SOAP_BASIC_AUTH_USER"], os.environ["SOAP_BASIC_AUTH_PASSWORD"] + "x")
    response = requests.get("http://127.0.0.1:18081/?wsdl", auth=wrong, timeout=10)
    assert response.status_code == 401


def test_wsdl_with_credentials_serves_contract(good_auth):
    response = requests.get("http://127.0.0.1:18081/?wsdl", auth=good_auth, timeout=10)
    assert response.status_code == 200
    assert b"wsdl:definitions" in response.content
    for marker in (b"CreateOrder", b"GetOrders", b"GetOrderStatus", b"UpdateOrderStatus"):
        assert marker in response.content


def test_soap_endpoint_without_credentials_is_401():
    response = requests.get("http://127.0.0.1:18081/", timeout=10)
    assert response.status_code == 401


def test_credentials_come_from_environment_not_code():
    assert os.environ["SOAP_BASIC_AUTH_USER"].startswith("test-")
    # The generated password exists only in this process' env, never in source.
    assert len(os.environ["SOAP_BASIC_AUTH_PASSWORD"]) >= 32
