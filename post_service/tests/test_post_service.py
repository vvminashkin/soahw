import pytest
import grpc
from datetime import datetime
import post_service_pb2
pytestmark = pytest.mark.usefixtures("cleanup_test_posts")

class TestPostService:
    def test_create_post(self, post_service_client, test_user_id):
        request = post_service_pb2.CreatePostRequest(
            title="Test Post Title",
            description="Test Post Description",
            creator_id=test_user_id,
            is_private=False,
            tags=["test", "create"]
        )
        response = post_service_client.CreatePost(request)
        assert response.title == "Test Post Title"
        assert response.description == "Test Post Description"
        assert response.creator_id == test_user_id
        assert response.is_private is False
        assert len(response.tags) == 2
        assert "test" in response.tags
        assert "create" in response.tags
        assert response.id is not None
        assert response.created_at is not None
        assert response.updated_at is not None
    
    def test_create_private_post(self, post_service_client, test_user_id):
        request = post_service_pb2.CreatePostRequest(
            title="Private Post",
            description="This is a private post",
            creator_id=test_user_id,
            is_private=True,
            tags=["private", "test"]
        )
        response = post_service_client.CreatePost(request)
        assert response.is_private is True
        assert "private" in response.tags
    
    def test_get_post(self, post_service_client, create_test_post, test_user_id):
        post = create_test_post(title="Get Test Post", description="Post to retrieve")
        request = post_service_pb2.GetPostRequest(
            post_id=post.id,
            user_id=test_user_id
        )
        response = post_service_client.GetPost(request)
        assert response.id == post.id
        assert response.title == "Get Test Post"
        assert response.description == "Post to retrieve"
        assert response.creator_id == test_user_id
    
    def test_get_nonexistent_post(self, post_service_client, test_user_id):
        """Тест получения несуществующего поста."""
        request = post_service_pb2.GetPostRequest(
            post_id="999999",  # Несуществующий ID
            user_id=test_user_id
        )
        with pytest.raises(grpc.RpcError) as excinfo:
            post_service_client.GetPost(request)

        assert excinfo.value.code() == grpc.StatusCode.NOT_FOUND
    
    def test_get_private_post_denied(self, post_service_client, create_test_post, test_user_id, another_user_id):
        """Тест проверки доступа к приватному посту."""
        post = create_test_post(title="Private Post", description="Secret post", is_private=True)
        request = post_service_pb2.GetPostRequest(
            post_id=post.id,
            user_id=another_user_id  # Другой пользователь
        )
        with pytest.raises(grpc.RpcError) as excinfo:
            post_service_client.GetPost(request)
        assert excinfo.value.code() == grpc.StatusCode.PERMISSION_DENIED
    
    def test_update_post(self, post_service_client, create_test_post, test_user_id):
        """Тест обновления поста."""
        post = create_test_post()
        update_request = post_service_pb2.UpdatePostRequest(
            post_id=post.id,
            user_id=test_user_id,
            title="Updated Title",
            description="Updated Description",
            is_private=True,
            tags=["updated", "test"]
        )
        updated_post = post_service_client.UpdatePost(update_request)
        assert updated_post.id == post.id
        assert updated_post.title == "Updated Title"
        assert updated_post.description == "Updated Description"
        assert updated_post.is_private is True
        assert "updated" in updated_post.tags
        get_request = post_service_pb2.GetPostRequest(
            post_id=post.id,
            user_id=test_user_id
        )
        get_response = post_service_client.GetPost(get_request)
        assert get_response.title == "Updated Title"

    def test_update_nonexistent_post(self, post_service_client, test_user_id):
        update_request = post_service_pb2.UpdatePostRequest(
            post_id="999999",
            user_id=test_user_id,
            title="This Won't Work",
            description="This post doesn't exist",
            is_private=False,
            tags=["test"]
        )
        with pytest.raises(grpc.RpcError) as excinfo:
            post_service_client.UpdatePost(update_request)
        assert excinfo.value.code() == grpc.StatusCode.NOT_FOUND

    def test_delete_post(self, post_service_client, create_test_post, test_user_id):
        post = create_test_post()
        delete_request = post_service_pb2.DeletePostRequest(
            post_id=post.id,
            user_id=test_user_id
        )
        post_service_client.DeletePost(delete_request)
        get_request = post_service_pb2.GetPostRequest(
            post_id=post.id,
            user_id=test_user_id
        )
        with pytest.raises(grpc.RpcError) as excinfo:
            post_service_client.GetPost(get_request)
        assert excinfo.value.code() == grpc.StatusCode.NOT_FOUND

    def test_list_posts(self, post_service_client, create_test_post, test_user_id):
        create_test_post(title="Post 1", tags=["list", "test", "one"])
        create_test_post(title="Post 2", tags=["list", "test", "two"])
        create_test_post(title="Post 3", tags=["list", "test", "three"])
        request = post_service_pb2.ListPostsRequest(
            page=1,
            page_size=10,
            user_id=test_user_id,
            include_private=False,
            tags=["list"]
        )
        response = post_service_client.ListPosts(request)
        assert response.total_count >= 3
        assert len(response.posts) >= 3
        assert response.page == 1
        assert response.page_size == 10
        tag_request = post_service_pb2.ListPostsRequest(
            page=1,
            page_size=10,
            user_id=test_user_id,
            include_private=False,
            tags=["one"]
        )
        tag_response = post_service_client.ListPosts(tag_request)
        assert len(tag_response.posts) == 1
        assert tag_response.posts[0].title == "Post 1"

    def test_list_posts_pagination(self, post_service_client, create_test_post, test_user_id):
        for i in range(5):
            create_test_post(title=f"Pagination Post {i+1}", tags=["pagination", "test"])
        page1_request = post_service_pb2.ListPostsRequest(
            page=1,
            page_size=2,
            user_id=test_user_id,
            include_private=False,
            tags=["pagination"]
        )
        page1_response = post_service_client.ListPosts(page1_request)
        assert page1_response.total_count >= 5
        assert len(page1_response.posts) == 2
        assert page1_response.page == 1
        assert page1_response.page_size == 2
        page2_request = post_service_pb2.ListPostsRequest(
            page=2,
            page_size=2,
            user_id=test_user_id,
            include_private=False,
            tags=["pagination"]
        )
        page2_response = post_service_client.ListPosts(page2_request)
        assert page2_response.total_count >= 5
        assert len(page2_response.posts) == 2
        assert page2_response.page == 2
        assert page2_response.page_size == 2
        page1_ids = [post.id for post in page1_response.posts]
        page2_ids = [post.id for post in page2_response.posts]
        assert not set(page1_ids).intersection(set(page2_ids))

    def test_list_private_posts(self, post_service_client, create_test_post, test_user_id):
        create_test_post(title="Public Post", is_private=False, tags=["privacy", "public"])
        create_test_post(title="Private Post 1", is_private=True, tags=["privacy", "private"])
        create_test_post(title="Private Post 2", is_private=True, tags=["privacy", "private"])
        private_request = post_service_pb2.ListPostsRequest(
            page=1,
            page_size=10,
            user_id=test_user_id,
            include_private=True,
            tags=["privacy"]
        )
        private_response = post_service_client.ListPosts(private_request)
        assert private_response.total_count >= 3
        public_request = post_service_pb2.ListPostsRequest(
            page=1,
            page_size=10,
            user_id=test_user_id,
            include_private=False,
            tags=["privacy"]
        )
        public_response = post_service_client.ListPosts(public_request)
        public_post_count = sum(1 for post in public_response.posts if post.is_private is False)
        private_post_count = sum(1 for post in public_response.posts if post.is_private is True)
        assert public_post_count >= 1
        assert private_post_count >= 2
