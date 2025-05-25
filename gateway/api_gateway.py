import sys
import requests
import grpc
from flask import Flask, request, jsonify
from functools import wraps
import jwt
from jwt.exceptions import InvalidTokenError
import json
from google.protobuf.json_format import MessageToDict
from datetime import datetime
import statistics_service_pb2
import statistics_service_pb2_grpc
import os
from kafka import KafkaProducer

import post_service_pb2
import post_service_pb2_grpc

USER_SERVICE_URL = "http://user-service:5001"
POST_SERVICE_HOST = os.environ.get("POST_SERVICE_HOST", "post-service")
POST_SERVICE_PORT = os.environ.get("POST_SERVICE_PORT", "50051")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_REGISTRATION = "user-registration"
KAFKA_TOPIC_LIKES = "user-likes"
KAFKA_TOPIC_VIEWS = "user-content-views"
KAFKA_TOPIC_COMMENTS = "user-comments"
STATS_SERVICE_HOST = os.environ.get("STATS_SERVICE_HOST", "stats-service")
STATS_SERVICE_PORT = os.environ.get("STATS_SERVICE_PORT", "50052")

kafka_producer = None
import socket
import time
from kafka.errors import NoBrokersAvailable


def is_kafka_ready(host, port, timeout=1):
    """Проверка доступности порта Kafka"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.close()
        return True
    except:
        s.close()
        return False


def wait_for_kafka(app, host="kafka", port=9092, max_retries=30):
    """Ожидание готовности Kafka"""
    retries = 0
    while retries < max_retries:
        if is_kafka_ready(host, port):
            app.logger.info(f"Kafka доступна на {host}:{port}")
            return True
        retries += 1
        app.logger.warning(
            f"Kafka недоступна, повторная попытка {retries}/{max_retries}..."
        )
        time.sleep(2)
    app.logger.error(f"Kafka не стала доступной после {max_retries} попыток")
    return False


def get_kafka_producer():
    global kafka_producer
    if kafka_producer is None:
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return kafka_producer


def create_app():
    app = Flask(__name__)
    try:
        wait_for_kafka(app)
        producer = get_kafka_producer()
        if producer:
            app.logger.info("Подключение к Kafka успешно инициализировано")
    except Exception as e:
        app.logger.error(f"Ошибка при инициализации Kafka: {str(e)}")
    return app


app = create_app()


def send_event_to_kafka(topic, event_data):
    try:
        producer = get_kafka_producer()
        producer.send(topic, event_data)
        producer.flush()
        app.logger.info(f"Event sent to Kafka topic {topic}: {event_data}")
    except Exception as e:
        app.logger.error(f"Failed to send event to Kafka: {str(e)}")


def get_public_key():
    try:
        with open("public_key.pem", "r") as file:
            return file.read()
    except FileNotFoundError:
        raise Exception("public_key.pem file not found")


PUBLIC_KEY = get_public_key()


def get_post_service_stub():
    channel = grpc.insecure_channel(f"{POST_SERVICE_HOST}:{POST_SERVICE_PORT}")
    return post_service_pb2_grpc.PostServiceStub(channel)


def authorize(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get("Authorization", None)
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid Authorization header"}), 401

        token = auth_header.split()[1]
        try:
            decoded = jwt.decode(token, PUBLIC_KEY, algorithms=["RS256"])
            request.user = decoded
        except InvalidTokenError as e:
            return jsonify({"error": f"Token invalid or expired: {str(e)}"}), 401

        return f(*args, **kwargs)

    return wrapper


def timestamp_to_iso(timestamp):
    dt = datetime.fromtimestamp(timestamp.seconds + timestamp.nanos / 1e9)
    return dt.isoformat()


def process_post_response(post):
    result = MessageToDict(post, preserving_proto_field_name=True)

    if "created_at" in result:
        result["created_at"] = timestamp_to_iso(post.created_at)
    if "updated_at" in result:
        result["updated_at"] = timestamp_to_iso(post.updated_at)

    return result


def get_stats_service_stub():
    channel = grpc.insecure_channel(f"{STATS_SERVICE_HOST}:{STATS_SERVICE_PORT}")
    return statistics_service_pb2_grpc.StatsServiceStub(channel)


@app.route("/stats/v1/posts/<post_id>", methods=["GET"])
@authorize
def get_post_stats(post_id):
    try:
        stub = get_stats_service_stub()
        request = statistics_service_pb2.PostStatsRequest(post_id=int(post_id))

        response = stub.GetPostStats(request)

        result = {
            "post_id": post_id,
            "views_count": response.views_count,
            "likes_count": response.likes_count,
            "comments_count": response.comments_count,
        }

        return jsonify(result), 200
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/stats/v1/posts/<post_id>/views/dynamics", methods=["GET"])
@authorize
def get_post_views_dynamics(post_id):
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        stub = get_stats_service_stub()
        request_msg = statistics_service_pb2.PostDynamicsRequest(
            post_id=int(post_id), start_date=start_date, end_date=end_date
        )

        response = stub.GetPostViewsDynamics(request_msg)

        result = {
            "post_id": post_id,
            "daily_stats": [
                {"date": stat.date, "count": stat.count}
                for stat in response.daily_stats
            ],
        }

        return jsonify(result), 200
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/stats/v1/posts/<post_id>/likes/dynamics", methods=["GET"])
@authorize
def get_post_likes_dynamics(post_id):
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        stub = get_stats_service_stub()
        request_msg = statistics_service_pb2.PostDynamicsRequest(
            post_id=int(post_id), start_date=start_date, end_date=end_date
        )

        response = stub.GetPostLikesDynamics(request_msg)

        result = {
            "post_id": post_id,
            "daily_stats": [
                {"date": stat.date, "count": stat.count}
                for stat in response.daily_stats
            ],
        }

        return jsonify(result), 200
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/stats/v1/posts/<post_id>/comments/dynamics", methods=["GET"])
@authorize
def get_post_comments_dynamics(post_id):
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        stub = get_stats_service_stub()
        request_msg = statistics_service_pb2.PostDynamicsRequest(
            post_id=int(post_id), start_date=start_date, end_date=end_date
        )

        response = stub.GetPostCommentsDynamics(request_msg)

        result = {
            "post_id": post_id,
            "daily_stats": [
                {"date": stat.date, "count": stat.count}
                for stat in response.daily_stats
            ],
        }

        return jsonify(result), 200
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/stats/v1/posts/top", methods=["GET"])
@authorize
def get_top_posts():
    try:
        metric_type = request.args.get("metric_type", "VIEWS")
        limit = int(request.args.get("limit", 10))
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        metric_mapping = {
            "VIEWS": statistics_service_pb2.VIEWS,
            "LIKES": statistics_service_pb2.LIKES,
            "COMMENTS": statistics_service_pb2.COMMENTS,
        }

        if metric_type not in metric_mapping:
            return (
                jsonify(
                    {
                        "error": "Invalid metric_type. Must be one of: VIEWS, LIKES, COMMENTS"
                    }
                ),
                400,
            )

        stub = get_stats_service_stub()
        request_msg = statistics_service_pb2.TopPostsRequest(
            metric_type=metric_mapping[metric_type],
            limit=limit,
            start_date=start_date,
            end_date=end_date,
        )

        response = stub.GetTopPosts(request_msg)

        result = {
            "metric_type": metric_type,
            "posts": [
                {"post_id": post.post_id, "count": post.count}
                for post in response.posts
            ],
        }

        return jsonify(result), 200
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/stats/v1/users/top", methods=["GET"])
@authorize
def get_top_users():
    try:
        metric_type = request.args.get("metric_type", "VIEWS")
        limit = int(request.args.get("limit", 10))
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        metric_mapping = {
            "VIEWS": statistics_service_pb2.VIEWS,
            "LIKES": statistics_service_pb2.LIKES,
            "COMMENTS": statistics_service_pb2.COMMENTS,
        }

        if metric_type not in metric_mapping:
            return (
                jsonify(
                    {
                        "error": "Invalid metric_type. Must be one of: VIEWS, LIKES, COMMENTS"
                    }
                ),
                400,
            )

        stub = get_stats_service_stub()
        request_msg = statistics_service_pb2.TopUsersRequest(
            metric_type=metric_mapping[metric_type],
            limit=limit,
            start_date=start_date,
            end_date=end_date,
        )

        response = stub.GetTopUsers(request_msg)

        result = {
            "metric_type": metric_type,
            "users": [
                {"user_id": user.user_id, "count": user.count}
                for user in response.users
            ],
        }

        return jsonify(result), 200
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "API service is healthy"}), 200


@app.route("/users/v1/login", methods=["POST"])
def login():
    resp = requests.post(f"{USER_SERVICE_URL}/users/v1/login", json=request.json)
    return jsonify(resp.json()), resp.status_code


@app.route("/users/v1/register", methods=["POST"])
def register():
    resp = requests.post(f"{USER_SERVICE_URL}/users/v1/register", json=request.json)

    if resp.status_code == 201:
        response_data = resp.json()
        decoded = jwt.decode(
            response_data.get("access_token"), PUBLIC_KEY, algorithms=["RS256"]
        )
        user = decoded
        event_data = {
            "user_id": user["user_id"],
            "registration_date": datetime.now().isoformat(),
            "login": request.json.get("login", ""),
            "email": request.json.get("email", ""),
        }

        send_event_to_kafka(KAFKA_TOPIC_REGISTRATION, event_data)

    return jsonify(resp.json()), resp.status_code


@app.route("/users/v1", methods=["GET", "PUT"])
@authorize
def user_profile():
    headers = {"Authorization": request.headers["Authorization"]}
    if request.method == "GET":
        resp = requests.get(f"{USER_SERVICE_URL}/users/v1", headers=headers)
    elif request.method == "PUT":
        resp = requests.put(
            f"{USER_SERVICE_URL}/users/v1", headers=headers, json=request.json
        )

    return jsonify(resp.json()), resp.status_code


@app.route("/posts/v1", methods=["POST"])
@authorize
def create_post():
    try:
        user_id = str(request.user.get("user_id"))
        data = request.json

        stub = get_post_service_stub()
        post_request = post_service_pb2.CreatePostRequest(
            title=data.get("title", ""),
            description=data.get("description", ""),
            creator_id=user_id,
            is_private=data.get("is_private", False),
            tags=data.get("tags", []),
        )

        response = stub.CreatePost(post_request)

        result = process_post_response(response)

        return jsonify(result), 201
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.PERMISSION_DENIED: 403,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.ALREADY_EXISTS: 409,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/posts/v1/<post_id>", methods=["GET"])
@authorize
def get_post(post_id):
    try:
        user_id = str(request.user.get("user_id"))

        stub = get_post_service_stub()
        post_request = post_service_pb2.GetPostRequest(post_id=post_id, user_id=user_id)

        response = stub.GetPost(post_request)

        result = process_post_response(response)

        event_data = {
            "user_id": user_id,
            "post_id": post_id,
            "view_time": datetime.now().isoformat(),
        }
        send_event_to_kafka(KAFKA_TOPIC_VIEWS, event_data)

        return jsonify(result), 200
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.PERMISSION_DENIED: 403,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/posts/v1/<post_id>", methods=["PUT"])
@authorize
def update_post(post_id):
    try:
        user_id = str(request.user.get("user_id"))
        data = request.json

        stub = get_post_service_stub()
        post_request = post_service_pb2.UpdatePostRequest(
            post_id=post_id,
            user_id=user_id,
            title=data.get("title", ""),
            description=data.get("description", ""),
            is_private=data.get("is_private", False),
            tags=data.get("tags", []),
        )

        response = stub.UpdatePost(post_request)

        result = process_post_response(response)

        return jsonify(result), 200
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.PERMISSION_DENIED: 403,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/posts/v1/<post_id>", methods=["DELETE"])
@authorize
def delete_post(post_id):
    try:
        user_id = str(request.user.get("user_id"))

        stub = get_post_service_stub()
        post_request = post_service_pb2.DeletePostRequest(
            post_id=post_id, user_id=user_id
        )

        stub.DeletePost(post_request)

        return "", 204
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.PERMISSION_DENIED: 403,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/posts/v1", methods=["GET"])
@authorize
def list_posts():
    try:
        user_id = str(request.user.get("user_id"))

        page = int(request.args.get("page", 1))
        page_size = max(int(request.args.get("page_size", 10)), 1)
        include_private = request.args.get("include_private", "false").lower() == "true"
        tags = request.args.getlist("tags")

        stub = get_post_service_stub()
        post_request = post_service_pb2.ListPostsRequest(
            page=page,
            page_size=page_size,
            user_id=user_id,
            include_private=include_private,
            tags=tags,
        )

        response = stub.ListPosts(post_request)

        posts_json = []
        current_time = datetime.now().isoformat()

        for post in response.posts:
            post_dict = process_post_response(post)
            posts_json.append(post_dict)

            event_data = {
                "user_id": user_id,
                "post_id": post_dict.get("id"),
                "view_time": current_time,
                "view_type": "list",
            }
            send_event_to_kafka(KAFKA_TOPIC_VIEWS, event_data)

        result = {
            "posts": posts_json,
            "total_count": response.total_count,
            "page": response.page,
            "page_size": response.page_size,
            "total_pages": response.total_pages,
        }

        return jsonify(result), 200
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.PERMISSION_DENIED: 403,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/posts/v1/<post_id>/like", methods=["POST"])
@authorize
def like_post(post_id):
    try:
        user_id = str(request.user.get("user_id"))

        stub = get_post_service_stub()
        post_request = post_service_pb2.GetPostRequest(post_id=post_id, user_id=user_id)

        response = stub.GetPost(post_request)
        event_data = {
            "user_id": user_id,
            "post_id": post_id,
            "like_time": datetime.now().isoformat(),
        }
        send_event_to_kafka(KAFKA_TOPIC_LIKES, event_data)

        return "", 204
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.PERMISSION_DENIED: 403,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


@app.route("/posts/v1/<post_id>/comments", methods=["POST"])
@authorize
def create_comment(post_id):
    try:
        user_id = str(request.user.get("user_id"))
        data = request.json

        stub = get_post_service_stub()
        post_request = post_service_pb2.GetPostRequest(post_id=post_id, user_id=user_id)

        response = stub.GetPost(post_request)

        from uuid import uuid4

        comment_id = str(uuid4())

        event_data = {
            "user_id": user_id,
            "post_id": post_id,
            "comment_id": comment_id,
            "comment_time": datetime.now().isoformat(),
            "content": data.get("content", ""),
        }
        send_event_to_kafka(KAFKA_TOPIC_COMMENTS, event_data)

        return jsonify({"comment_id": comment_id}), 201
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.PERMISSION_DENIED: 403,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500,
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
