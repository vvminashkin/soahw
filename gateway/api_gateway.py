import sys
import requests
from flask import Flask, request, jsonify
from functools import wraps
import jwt
from jwt.exceptions import InvalidTokenError

USER_SERVICE_URL = "http://user-service:5001"

app = Flask(__name__)

def get_public_key():
    try:
        with open("public_key.pem", "r") as file:
            return file.read()
    except FileNotFoundError:
        raise Exception("public_key.pem file not found")

PUBLIC_KEY = get_public_key()

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

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)