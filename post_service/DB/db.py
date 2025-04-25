import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Table, ForeignKey, func, Text
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, selectinload
from sqlalchemy.dialects.postgresql import ARRAY

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://post_service:post_password@post-db:5432/posts_db"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

post_tag = Table(
    'post_tag',
    Base.metadata,
    Column('post_id', Integer, ForeignKey('posts.id')),
    Column('tag_id', Integer, ForeignKey('tags.id'))
)

class Tag(Base):
    __tablename__ = "tags"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    
    posts = relationship("Post", secondary=post_tag, back_populates="tags")

class Post(Base):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    creator_id = Column(String, nullable=False, index=True)
    is_private = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    tags = relationship("Tag", secondary=post_tag, back_populates="posts", lazy="joined")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_post_by_id(post_id):
    with SessionLocal() as db:
        return db.query(Post).options(selectinload(Post.tags)).filter(Post.id == post_id).first()

def create_post(title, description, creator_id, is_private=False, tags=None):
    with SessionLocal() as db:
        post = Post(
            title=title,
            description=description,
            creator_id=creator_id,
            is_private=is_private
        )
        db.add(post)
        if tags:
            for tag_name in tags:
                tag = db.query(Tag).filter(Tag.name == tag_name).first()
                if not tag:
                    tag = Tag(name=tag_name)
                    db.add(tag)
                    db.flush()
                post.tags.append(tag)
        db.commit()
        return db.query(Post).options(selectinload(Post.tags)).filter(Post.id == post.id).first()

def update_post(post_id, user_id, updates: dict):
    with SessionLocal() as db:
        post = db.query(Post).options(selectinload(Post.tags)).filter(
            Post.id == post_id, 
            Post.creator_id == user_id
        ).first()
        if not post:
            return None
        allowed_fields = ["title", "description", "is_private"]
        for field in allowed_fields:
            if field in updates:
                setattr(post, field, updates[field])
        if "tags" in updates:
            post.tags = []
            for tag_name in updates["tags"]:
                tag = db.query(Tag).filter(Tag.name == tag_name).first()
                if not tag:
                    tag = Tag(name=tag_name)
                    db.add(tag)
                    db.flush()
                post.tags.append(tag)
        post.updated_at = datetime.now()
        db.commit()
        db.refresh(post)
        return post

def delete_post(post_id, user_id):
    with SessionLocal() as db:
        post = db.query(Post).filter(
            Post.id == post_id, 
            Post.creator_id == user_id
        ).first()
        if not post:
            return False
        db.delete(post)
        db.commit()
        return True

def list_posts(page=1, page_size=10, user_id=None, include_private=False, tags=None):
    with SessionLocal() as db:
        query = db.query(Post).options(selectinload(Post.tags))
        print(user_id, "fdafs")
        if not include_private:
            query = query.filter((Post.is_private == False) | 
                                ((Post.is_private == True) & (Post.creator_id == user_id)))
        else:
            query = query.filter(Post.creator_id == user_id)
        if tags and len(tags) > 0:
            for tag_name in tags:
                query = query.filter(Post.tags.any(Tag.name == tag_name))
        total_count = query.count()
        query = query.order_by(Post.created_at.desc())
        query = query.offset((page - 1) * page_size).limit(page_size)
        posts = query.all()
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        return posts, total_count, total_pages

def get_all_tags():
    with SessionLocal() as db:
        return db.query(Tag).all()

def init_db():
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db()
