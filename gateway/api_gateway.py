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
import os

import post_service_pb2
import post_service_pb2_grpc

USER_SERVICE_URL = "http://user-service:5001"
POST_SERVICE_HOST = os.environ.get("POST_SERVICE_HOST", "post-service")
POST_SERVICE_PORT = os.environ.get("POST_SERVICE_PORT", "50051")

app = Flask(__name__)

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
        auth_header = request.headers.get('Authorization', None)
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
    
    if 'created_at' in result:
        result['created_at'] = timestamp_to_iso(post.created_at)
    if 'updated_at' in result:
        result['updated_at'] = timestamp_to_iso(post.updated_at)
        
    return result

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "API service is healthy"}), 200

@app.route('/users/v1/login', methods=['POST'])
def login():
    resp = requests.post(f"{USER_SERVICE_URL}/users/v1/login", json=request.json)
    return jsonify(resp.json()), resp.status_code

@app.route('/users/v1/register', methods=['POST'])
def register():
    resp = requests.post(f"{USER_SERVICE_URL}/users/v1/register", json=request.json)
    return jsonify(resp.json()), resp.status_code

@app.route('/users/v1', methods=['GET', 'PUT'])
@authorize
def user_profile():
    headers = {'Authorization': request.headers['Authorization']}
    if request.method == 'GET':
        resp = requests.get(f"{USER_SERVICE_URL}/users/v1", headers=headers)
    elif request.method == 'PUT':
        resp = requests.put(f"{USER_SERVICE_URL}/users/v1", headers=headers, json=request.json)

    return jsonify(resp.json()), resp.status_code

@app.route('/posts/v1', methods=['POST'])
@authorize
def create_post():
    try:
        user_id = request.user.get('sub')
        data = request.json
        
        stub = get_post_service_stub()
        post_request = post_service_pb2.CreatePostRequest(
            title=data.get('title', ''),
            description=data.get('description', ''),
            creator_id=user_id,
            is_private=data.get('is_private', False),
            tags=data.get('tags', [])
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
            grpc.StatusCode.INTERNAL: 500
        }.get(e.code(), 500)
        
        return jsonify({"error": e.details()}), status_code

@app.route('/posts/v1/<post_id>', methods=['GET'])
@authorize
def get_post(post_id):
    try:
        user_id = request.user.get('sub')
        
        stub = get_post_service_stub()
        post_request = post_service_pb2.GetPostRequest(
            post_id=post_id,
            user_id=user_id
        )
        
        response = stub.GetPost(post_request)
        
        result = process_post_response(response)
        
        return jsonify(result), 200
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.PERMISSION_DENIED: 403,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500
        }.get(e.code(), 500)
        
        return jsonify({"error": e.details()}), status_code

@app.route('/posts/v1/<post_id>', methods=['PUT'])
@authorize
def update_post(post_id):
    try:
        user_id = request.user.get('sub')
        data = request.json
        
        stub = get_post_service_stub()
        post_request = post_service_pb2.UpdatePostRequest(
            post_id=post_id,
            user_id=user_id,
            title=data.get('title', ''),
            description=data.get('description', ''),
            is_private=data.get('is_private', False),
            tags=data.get('tags', [])
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
            grpc.StatusCode.INTERNAL: 500
        }.get(e.code(), 500)
        
        return jsonify({"error": e.details()}), status_code

@app.route('/posts/v1/<post_id>', methods=['DELETE'])
@authorize
def delete_post(post_id):
    try:
        user_id = request.user.get('sub')
        
        stub = get_post_service_stub()
        post_request = post_service_pb2.DeletePostRequest(
            post_id=post_id,
            user_id=user_id
        )
        
        stub.DeletePost(post_request)
        
        return '', 204
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.PERMISSION_DENIED: 403,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500
        }.get(e.code(), 500)
        
        return jsonify({"error": e.details()}), status_code

@app.route('/posts/v1', methods=['GET'])
@authorize
def list_posts():
    try:
        user_id = request.user.get('sub')
        
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 10))
        include_private = request.args.get('include_private', 'false').lower() == 'true'
        tags = request.args.getlist('tags')

        stub = get_post_service_stub()
        post_request = post_service_pb2.ListPostsRequest(
            page=page,
            page_size=page_size,
            user_id=user_id,
            include_private=include_private,
            tags=tags
        )

        response = stub.ListPosts(post_request)

        posts_json = []
        for post in response.posts:
            posts_json.append(process_post_response(post))

        result = {
            "posts": posts_json,
            "total_count": response.total_count,
            "page": response.page,
            "page_size": response.page_size,
            "total_pages": response.total_pages
        }

        return jsonify(result), 200
    except grpc.RpcError as e:
        status_code = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.PERMISSION_DENIED: 403,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.INTERNAL: 500
        }.get(e.code(), 500)

        return jsonify({"error": e.details()}), status_code

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)