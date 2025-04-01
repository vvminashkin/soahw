import os
import logging
import grpc
from concurrent import futures
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.empty_pb2 import Empty
from datetime import datetime

import post_service_pb2
import post_service_pb2_grpc

from DB.db import (
    get_post_by_id,
    create_post,
    update_post,
    delete_post,
    list_posts,
    get_all_tags,
    init_db
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def datetime_to_timestamp(dt):
    """Преобразует datetime в protobuf Timestamp"""
    timestamp = Timestamp()
    timestamp.FromDatetime(dt)
    return timestamp

class PostServiceServicer(post_service_pb2_grpc.PostServiceServicer):
    """Реализация gRPC-сервиса для работы с постами"""
    def CreatePost(self, request, context):
        """Создает новый пост"""
        try:
            post = create_post(
                title=request.title,
                description=request.description,
                creator_id=request.creator_id,
                is_private=request.is_private,
                tags=list(request.tags)
            )
            if not post:
                logger.error("Failed to create post")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Failed to create post")
                return post_service_pb2.PostResponse()
            response = post_service_pb2.PostResponse(
                id=str(post.id),
                title=post.title,
                description=post.description,
                creator_id=post.creator_id,
                is_private=post.is_private,
                created_at=datetime_to_timestamp(post.created_at),
                updated_at=datetime_to_timestamp(post.updated_at)
            )
            for tag in post.tags:
                response.tags.append(tag.name)
            return response
        except Exception as e:
            logger.error(f"Error creating post: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return post_service_pb2.PostResponse()

    def GetPost(self, request, context):
        try:
            post = get_post_by_id(int(request.post_id))
            if not post:
                logger.warning(f"Post with ID {request.post_id} not found")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Post with ID {request.post_id} not found")
                return post_service_pb2.PostResponse()
            if post.is_private and post.creator_id != request.user_id:
                logger.warning(f"User {request.user_id} denied access to private post {request.post_id}")
                context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                context.set_details("You don't have permission to view this post")
                return post_service_pb2.PostResponse()
            response = post_service_pb2.PostResponse(
                id=str(post.id),
                title=post.title,
                description=post.description,
                creator_id=post.creator_id,
                is_private=post.is_private,
                created_at=datetime_to_timestamp(post.created_at),
                updated_at=datetime_to_timestamp(post.updated_at)
            )
            for tag in post.tags:
                response.tags.append(tag.name)
            return response
        except Exception as e:
            logger.error(f"Error getting post: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return post_service_pb2.PostResponse()

    def UpdatePost(self, request, context):
        try:
            logger.info(f"Updating post with ID: {request.post_id}")
            updates = {
                "title": request.title,
                "description": request.description,
                "is_private": request.is_private,
                "tags": list(request.tags)
            }
            post = update_post(
                post_id=int(request.post_id),
                user_id=request.user_id,
                updates=updates
            )
            if not post:
                logger.warning(f"Post with ID {request.post_id} not found or access denied")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Post with ID {request.post_id} not found or access denied")
                return post_service_pb2.PostResponse()
            response = post_service_pb2.PostResponse(
                id=str(post.id),
                title=post.title,
                description=post.description,
                creator_id=post.creator_id,
                is_private=post.is_private,
                created_at=datetime_to_timestamp(post.created_at),
                updated_at=datetime_to_timestamp(post.updated_at)
            )
            for tag in post.tags:
                response.tags.append(tag.name)
            logger.info(f"Post updated with ID: {post.id}")
            return response
        except Exception as e:
            logger.error(f"Error updating post: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return post_service_pb2.PostResponse()

    def DeletePost(self, request, context):
        try:
            success = delete_post(
                post_id=int(request.post_id),
                user_id=request.user_id
            )
            if not success:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Post with ID {request.post_id} not found or access denied")
                return Empty()
            return Empty()
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return Empty()

    def ListPosts(self, request, context):
        try:
            posts, total_count, total_pages = list_posts(
                page=request.page,
                page_size=request.page_size,
                user_id=request.user_id,
                include_private=request.include_private,
                tags=list(request.tags) if request.tags else None
            )
            response = post_service_pb2.ListPostsResponse(
                total_count=total_count,
                page=request.page,
                page_size=request.page_size,
                total_pages=total_pages
            )
            for post in posts:
                post_response = post_service_pb2.PostResponse(
                    id=str(post.id),
                    title=post.title,
                    description=post.description,
                    creator_id=post.creator_id,
                    is_private=post.is_private,
                    created_at=datetime_to_timestamp(post.created_at),
                    updated_at=datetime_to_timestamp(post.updated_at)
                )
                for tag in post.tags:
                    post_response.tags.append(tag.name)
                response.posts.append(post_response)
            return response
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return post_service_pb2.ListPostsResponse()

def serve():
    port = os.getenv("GRPC_PORT", "50051")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    post_service_pb2_grpc.add_PostServiceServicer_to_server(
        PostServiceServicer(), server
    )
    server.add_insecure_port(f"[::]:{port}")
    init_db()
    server.start()

    server.wait_for_termination()

if __name__ == "__main__":
    serve()
