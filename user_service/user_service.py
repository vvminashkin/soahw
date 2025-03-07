from flask import Flask, request, jsonify
import jwt
import datetime
from email_validator import validate_email, EmailNotValidError
import os
from DB.db import (
    get_user_by_login, create_user,
    verify_user_credentials, get_user_by_id,
    update_user, init_db
)

PRIVATE_KEY_PATH = os.getenv("PRIVATE_KEY_PATH", "./private_key.pem")
PUBLIC_KEY_PATH = os.getenv("PUBLIC_KEY_PATH", "./public_key.pem")
JWT_ALGORITHM = "RS256"

app = Flask(__name__)

# функции JWT
def create_jwt_token(user):
    with open(PRIVATE_KEY_PATH, "r") as key_file:
        private_key = key_file.read()
    payload = {
        "user_id": user.id,
        "login": user.login,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24)
    }
    token = jwt.encode(payload, private_key, algorithm=JWT_ALGORITHM)
    return token

def verify_jwt_token(token):
    with open(PUBLIC_KEY_PATH, "r") as key_file:
        public_key = key_file.read()
    return jwt.decode(token, public_key, algorithms=[JWT_ALGORITHM])

@app.route("/users/v1/register", methods=["POST"])
def register():
    data = request.json
    required_fields = ["login", "password", "email"]
    
    if not all(field in data for field in required_fields):
        return jsonify({"error": "missing required fields"}), 400
    
    if get_user_by_login(data["login"]):
        return jsonify({"error": "Login already exists"}), 400
    try:
        email_info = validate_email(data["email"])
    except EmailNotValidError as e:
        return jsonify({"error": str(e)}), 400
    
    create_user(
        login=data["login"],
        email=data["email"],
        password=data["password"]
    )
    
    return jsonify({"message": "User registered successfully"}), 201

@app.route("/users/v1/login", methods=["POST"])
def login():
    data = request.json
    if "login" not in data or "password" not in data:
        return jsonify({"error": "Login and password required"}), 400

    user = verify_user_credentials(data["login"], data["password"])

    if user is None:
        return jsonify({"error": "Invalid credentials"}), 401

    token = create_jwt_token(user)
    return jsonify({"access_token": token}), 200

@app.route("/users/v1", methods=["GET", "PUT"])
def user_profile():
    auth_header = request.headers.get("Authorization", "")
    token = auth_header.replace("Bearer ", "")
    try:
        token_data = verify_jwt_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401
    
    user_id = token_data["user_id"]

    if request.method == "GET":
        user = get_user_by_id(user_id)
        if not user:
            return jsonify({"error": "User not found"}), 404
        
        return jsonify({
            "id": user.id,
            "login": user.login,
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "birth_date": user.birth_date,
            "phone_number": user.phone_number,
            "created_at": user.created_at,
            "updated_at": user.updated_at
        }), 200

    elif request.method == "PUT":
        updates = request.json
        if "email" in updates:
            try:
                email_info = validate_email(updates["email"])
            except EmailNotValidError as e:
                return jsonify({"error": str(e)}), 400
        user = update_user(user_id, updates)
        if not user:
            return jsonify({"error": "User not found"}), 404
        
        return jsonify({"message": "Profile updated successfully"}), 200

if __name__ == "__main__":
    init_db()
    app.run(debug=False,host="0.0.0.0", port=5001)