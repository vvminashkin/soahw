import os
from sqlalchemy import create_engine, Column, Integer, String, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base
from werkzeug.security import generate_password_hash, check_password_hash

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user_service:user_password@user-db:5432/users_db"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    login = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    birth_date = Column(String, nullable=True)
    phone_number = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())


def get_user_by_login(login):
    with SessionLocal() as db:
        return db.query(User).filter(User.login == login).first()

def get_user_by_id(user_id):
    with SessionLocal() as db:
        return db.query(User).filter(User.id == user_id).first()

def create_user(login, password, email):
    hashed_password = generate_password_hash(password)
    user = User(login=login, email=email, hashed_password=hashed_password)
    with SessionLocal() as db:
        db.add(user)
        db.commit()
        db.refresh(user)
    return user

def update_user(user_id, updates: dict):
    allowed_fields = ["first_name", "last_name", "birth_date", "email", "phone_number"]
    with SessionLocal() as db:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return None
        for field in allowed_fields:
            if field in updates:
                setattr(user, field, updates[field])
        db.commit()
        db.refresh(user)
    return user

def verify_user_credentials(login, password):
    user = get_user_by_login(login)
    if user and check_password_hash(user.hashed_password, password):
        return user
    return None

def init_db():
    Base.metadata.create_all(bind=engine)
