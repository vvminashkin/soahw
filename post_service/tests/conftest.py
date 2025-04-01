import os
import pytest
import time
import grpc
import subprocess
import signal
import psycopg2
from datetime import datetime

import sys
sys.path.append("..")
import post_service_pb2
import post_service_pb2_grpc

TEST_DB_HOST = "localhost"
TEST_DB_PORT = "5434"
TEST_DB_USER = "post_service"
TEST_DB_PASSWORD = "post_password"
TEST_DB_NAME = "posts_db"
TEST_GRPC_PORT = "50052"

@pytest.fixture(scope="session")
def docker_compose():
    compose_file = os.path.join(os.path.dirname(__file__), "docker-compose.test.yml")
    env = os.environ.copy()
    env["TEST_DB_PORT"] = TEST_DB_PORT
    env["TEST_GRPC_PORT"] = TEST_GRPC_PORT
    subprocess.run(
        ["docker-compose", "-f", compose_file, "up", "-d"],
        env=env,
        check=True
    )
    time.sleep(10)
    yield
    subprocess.run(
        ["docker-compose", "-f", compose_file, "down", "-v"],
        env=env,
        check=True
    )

@pytest.fixture(scope="session")
def wait_for_db(docker_compose):
    max_attempts = 300
    for attempt in range(max_attempts):
        try:
            conn = psycopg2.connect(
                host=TEST_DB_HOST,
                port=TEST_DB_PORT,
                user=TEST_DB_USER,
                password=TEST_DB_PASSWORD,
                dbname=TEST_DB_NAME
            )
            conn.close()
            return
        except psycopg2.OperationalError:
            pass

@pytest.fixture(scope="session")
def grpc_channel(docker_compose, wait_for_db):
    channel = grpc.insecure_channel(f"{TEST_DB_HOST}:{TEST_GRPC_PORT}")
    grpc.channel_ready_future(channel).result(timeout=10)
    return channel

@pytest.fixture(scope="session")
def post_service_client(grpc_channel):
    return post_service_pb2_grpc.PostServiceStub(grpc_channel)

@pytest.fixture
def test_user_id():
    return "test_user_1"

@pytest.fixture
def another_user_id():
    return "test_user_2"

@pytest.fixture
def create_test_post(post_service_client, test_user_id):
    def _create_post(title="Test Post", description="Test Description", is_private=False, tags=None):
        if tags is None:
            tags = ["test", "unit"]

        request = post_service_pb2.CreatePostRequest(
            title=title,
            description=description,
            creator_id=test_user_id,
            is_private=is_private,
            tags=tags
        )

        return post_service_client.CreatePost(request)

    return _create_post

@pytest.fixture
def cleanup_test_posts(docker_compose):
    yield

    conn = psycopg2.connect(
        host=TEST_DB_HOST,
        port=TEST_DB_PORT,
        user=TEST_DB_USER,
        password=TEST_DB_PASSWORD,
        dbname=TEST_DB_NAME
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM post_tag")
            cursor.execute("DELETE FROM posts")
            cursor.execute("DELETE FROM tags")
            conn.commit()
    finally:
        conn.close()
