from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash

class User:
    def __init__(self, **kwargs):
        self.id = 1
        self.first_name = None
        self.last_name = None
        self.birth_date = None
        self.phone_number = None
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.__dict__.update(kwargs)

users_db = []

def get_user_by_login(login):
    return next((u for u in users_db if u.login == login), None)

def get_user_by_id(user_id):
    return next((u for u in users_db if u.id == user_id), None)

def create_user(login, password, email):
    user = User(
        login=login,
        email=email,
        hashed_password=generate_password_hash(password)
    )
    user.id = len(users_db) + 1
    users_db.append(user)
    return user

def update_user(user_id, updates):
    user = get_user_by_id(user_id)
    if not user:
        return None
    
    allowed = {"first_name", "last_name", "birth_date", "email", "phone_number"}
    for key in updates:
        if key in allowed:
            setattr(user, key, updates[key])
    user.updated_at = datetime.now()
    return user

def verify_user_credentials(login, password):
    user = get_user_by_login(login)
    if user and check_password_hash(user.hashed_password, password):
        return user
    return None

def reset_database():
    global users_db
    users_db = []
    create_user("test_user", "test_pass", "test@gmail.com")

def init_db():
    pass