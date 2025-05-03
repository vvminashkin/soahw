import os
import pytest
import time
import subprocess
import requests
import logging
import uuid

API_HOST = "localhost"
API_PORT = "5000"


@pytest.fixture(scope="session", autouse=True)
def docker_services():
    compose_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    try:
        subprocess.run(
            ["docker-compose", "up", "-d", "--build"], cwd=compose_dir, check=True
        )
    except subprocess.CalledProcessError as e:
        raise

    time.sleep(30)

    yield

    try:
        subprocess.run(["docker-compose", "down", "-v"], cwd=compose_dir, check=True)
    except subprocess.CalledProcessError as e:
        pass


@pytest.fixture
def api_url():
    return f"http://{API_HOST}:{API_PORT}"


@pytest.fixture
def auth_headers(api_url):
    username = f"testuser_{uuid.uuid4().hex[:8]}"
    email = f"{username}@gmail.com"
    password = "Password123!"

    register_data = {"login": username, "email": email, "password": password}

    register_response = requests.post(
        f"{api_url}/users/v1/register", json=register_data
    )
    assert register_response.status_code == 201

    login_data = {"login": username, "password": password}

    login_response = requests.post(f"{api_url}/users/v1/login", json=login_data)
    assert login_response.status_code == 200

    token = login_response.json().get("access_token")
    return {"Authorization": f"Bearer {token}"}


def test_health_endpoint(api_url):

    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            response = requests.get(f"{api_url}/health", timeout=5)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass

        time.sleep(3)

    assert False, "API Gateway недоступен после нескольких попыток"


def test_user_registration(api_url):

    username = f"testuser_{uuid.uuid4().hex[:8]}"
    email = f"{username}@gmail.com"

    register_data = {"login": username, "email": email, "password": "Password123!"}

    response = requests.post(f"{api_url}/users/v1/register", json=register_data)
    assert response.status_code == 201


def test_user_login(api_url):

    username = f"testuser_{uuid.uuid4().hex[:8]}"
    email = f"{username}@gmail.com"
    password = "Password123!"

    register_data = {"login": username, "email": email, "password": password}

    register_response = requests.post(
        f"{api_url}/users/v1/register", json=register_data
    )
    assert register_response.status_code == 201

    login_data = {"login": username, "password": password}

    login_response = requests.post(f"{api_url}/users/v1/login", json=login_data)
    assert login_response.status_code == 200

    data = login_response.json()
    assert "access_token" in data


def test_create_post(api_url, auth_headers):

    post_data = {
        "title": f"Test Post {uuid.uuid4().hex[:8]}",
        "description": "This is a test post",
        "is_private": False,
        "tags": ["test", "api"],
    }

    response = requests.post(
        f"{api_url}/posts/v1", json=post_data, headers=auth_headers
    )
    assert response.status_code == 201

    data = response.json()
    assert "id" in data
    assert data.get("title") == post_data["title"]
    assert data.get("description") == post_data["description"]


def test_get_post(api_url, auth_headers):

    post_data = {
        "title": f"Test Post {uuid.uuid4().hex[:8]}",
        "description": "This is a test post for get",
        "is_private": False,
        "tags": ["test", "get"],
    }

    create_response = requests.post(
        f"{api_url}/posts/v1", json=post_data, headers=auth_headers
    )
    assert create_response.status_code == 201
    post_id = create_response.json().get("id")

    get_response = requests.get(f"{api_url}/posts/v1/{post_id}", headers=auth_headers)
    assert get_response.status_code == 200

    data = get_response.json()
    assert data.get("id") == post_id
    assert data.get("title") == post_data["title"]
    assert data.get("description") == post_data["description"]


def test_update_post(api_url, auth_headers):
    post_data = {
        "title": f"Test Post {uuid.uuid4().hex[:8]}",
        "description": "This is a test post for update",
        "is_private": False,
        "tags": ["test", "update"],
    }

    create_response = requests.post(
        f"{api_url}/posts/v1", json=post_data, headers=auth_headers
    )
    assert create_response.status_code == 201
    post_id = create_response.json().get("id")

    update_data = {
        "title": f"Updated Post {uuid.uuid4().hex[:8]}",
        "description": "This post has been updated",
        "is_private": True,
        "tags": ["test", "updated"],
    }

    update_response = requests.put(
        f"{api_url}/posts/v1/{post_id}", json=update_data, headers=auth_headers
    )
    assert update_response.status_code == 200

    get_response = requests.get(f"{api_url}/posts/v1/{post_id}", headers=auth_headers)
    assert get_response.status_code == 200

    data = get_response.json()
    assert data.get("title") == update_data["title"]
    assert data.get("description") == update_data["description"]
    assert data.get("is_private") == update_data["is_private"]


def test_list_posts(api_url, auth_headers):
    for i in range(3):
        post_data = {
            "title": f"List Test Post {i} {uuid.uuid4().hex[:8]}",
            "description": f"Test post {i} for list endpoint",
            "is_private": False,
            "tags": ["test", "list"],
        }

        create_response = requests.post(
            f"{api_url}/posts/v1", json=post_data, headers=auth_headers
        )
        assert create_response.status_code == 201

    list_response = requests.get(f"{api_url}/posts/v1", headers=auth_headers)
    assert list_response.status_code == 200

    data = list_response.json()
    assert "posts" in data
    assert isinstance(data["posts"], list)
    assert len(data["posts"]) > 0
    assert "total_count" in data
    assert "page" in data
    assert "page_size" in data


def test_like_post(api_url, auth_headers):

    post_data = {
        "title": f"Like Test Post {uuid.uuid4().hex[:8]}",
        "description": "This post will be liked",
        "is_private": False,
        "tags": ["test", "like"],
    }

    create_response = requests.post(
        f"{api_url}/posts/v1", json=post_data, headers=auth_headers
    )
    assert create_response.status_code == 201
    post_id = create_response.json().get("id")

    like_response = requests.post(
        f"{api_url}/posts/v1/{post_id}/like", headers=auth_headers
    )
    assert like_response.status_code == 204


def test_comment_post(api_url, auth_headers):
    post_data = {
        "title": f"Comment Test Post {uuid.uuid4().hex[:8]}",
        "description": "This post will be commented",
        "is_private": False,
        "tags": ["test", "comment"],
    }

    create_response = requests.post(
        f"{api_url}/posts/v1", json=post_data, headers=auth_headers
    )
    assert create_response.status_code == 201
    post_id = create_response.json().get("id")

    comment_data = {"content": "This is a test comment"}

    comment_response = requests.post(
        f"{api_url}/posts/v1/{post_id}/comments",
        json=comment_data,
        headers=auth_headers,
    )
    assert comment_response.status_code == 201
    assert "comment_id" in comment_response.json()
