import os
import time
import json
import grpc
import logging
from concurrent import futures
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import clickhouse_driver
from threading import Thread

import statistics_service_pb2
import statistics_service_pb2_grpc

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "default")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "stats_db")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_VIEWS = "user-content-views"
KAFKA_TOPIC_LIKES = "user-likes"
KAFKA_TOPIC_COMMENTS = "user-comments"

GRPC_PORT = int(os.environ.get("GRPC_PORT", 50052))


class ClickHouseClient:
    def __init__(self):
        self.client = None
        self.connect()
        self.create_tables()

    def connect(self):
        retries = 5
        delay = 5
        for attempt in range(retries):
            try:
                self.client = clickhouse_driver.Client(
                    host=CLICKHOUSE_HOST,
                    port=CLICKHOUSE_PORT,
                    user=CLICKHOUSE_USER,
                    password=CLICKHOUSE_PASSWORD,
                    database=CLICKHOUSE_DB,
                )

                self.client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
                self.client.execute(f"USE {CLICKHOUSE_DB}")
                logger.info("Successfully connected to ClickHouse")
                return
            except Exception as e:
                logger.error(f"Failed to connect to ClickHouse: {e}")
                if attempt < retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise

    def create_tables(self):
        self.client.execute(
            """
        CREATE TABLE IF NOT EXISTS views (
            user_id String,
            post_id String,
            view_time DateTime,
            view_type String DEFAULT 'detail'
        ) ENGINE = MergeTree()
        ORDER BY (post_id, view_time)
        """
        )

        self.client.execute(
            """
        CREATE TABLE IF NOT EXISTS likes (
            user_id String,
            post_id String,
            like_time DateTime
        ) ENGINE = MergeTree()
        ORDER BY (post_id, like_time)
        """
        )

        self.client.execute(
            """
        CREATE TABLE IF NOT EXISTS comments (
            user_id String,
            post_id String,
            comment_id String,
            comment_time DateTime,
            content String
        ) ENGINE = MergeTree()
        ORDER BY (post_id, comment_time)
        """
        )

        logger.info("ClickHouse tables created successfully")

    def insert_view(self, user_id, post_id, view_time, view_type="detail"):
        try:
            self.client.execute(
                "INSERT INTO views (user_id, post_id, view_time, view_type) VALUES",
                [(user_id, post_id, view_time, view_type)],
            )
        except Exception as e:
            logger.error(f"Error inserting view: {e}")

    def insert_like(self, user_id, post_id, like_time):
        try:
            self.client.execute(
                "INSERT INTO likes (user_id, post_id, like_time) VALUES",
                [(user_id, post_id, like_time)],
            )
        except Exception as e:
            logger.error(f"Error inserting like: {e}")

    def insert_comment(self, user_id, post_id, comment_id, comment_time, content):
        try:
            self.client.execute(
                "INSERT INTO comments (user_id, post_id, comment_id, comment_time, content) VALUES",
                [(user_id, post_id, comment_id, comment_time, content)],
            )
        except Exception as e:
            logger.error(f"Error inserting comment: {e}")

    def get_post_stats(self, post_id):
        try:
            views_count = self.client.execute(
                f"SELECT COUNT(*) FROM views WHERE post_id = '{post_id}'"
            )[0][0]

            likes_count = self.client.execute(
                f"SELECT COUNT(DISTINCT user_id) FROM likes WHERE post_id = '{post_id}'"
            )[0][0]

            comments_count = self.client.execute(
                f"SELECT COUNT(*) FROM comments WHERE post_id = '{post_id}'"
            )[0][0]

            return views_count, likes_count, comments_count
        except Exception as e:
            logger.error(f"Error getting post stats: {e}")
            return 0, 0, 0

    def get_post_views_dynamics(self, post_id, start_date=None, end_date=None):
        query = f"""
        SELECT 
            toDate(view_time) as date,
            COUNT(*) as count
        FROM views
        WHERE post_id = '{post_id}'
        """

        if start_date:
            query += f" AND toDate(view_time) >= toDate('{start_date}')"
        if end_date:
            query += f" AND toDate(view_time) <= toDate('{end_date}')"

        query += " GROUP BY date ORDER BY date"

        try:
            return self.client.execute(query)
        except Exception as e:
            logger.error(f"Error getting post views dynamics: {e}")
            return []

    def get_post_likes_dynamics(self, post_id, start_date=None, end_date=None):
        query = f"""
        SELECT 
            toDate(like_time) as date,
            COUNT(*) as count
        FROM likes
        WHERE post_id = '{post_id}'
        """

        if start_date:
            query += f" AND toDate(like_time) >= toDate('{start_date}')"
        if end_date:
            query += f" AND toDate(like_time) <= toDate('{end_date}')"

        query += " GROUP BY date ORDER BY date"

        try:
            return self.client.execute(query)
        except Exception as e:
            logger.error(f"Error getting post likes dynamics: {e}")
            return []

    def get_post_comments_dynamics(self, post_id, start_date=None, end_date=None):
        query = f"""
        SELECT 
            toDate(comment_time) as date,
            COUNT(*) as count
        FROM comments
        WHERE post_id = '{post_id}'
        """

        if start_date:
            query += f" AND toDate(comment_time) >= toDate('{start_date}')"
        if end_date:
            query += f" AND toDate(comment_time) <= toDate('{end_date}')"

        query += " GROUP BY date ORDER BY date"

        try:
            return self.client.execute(query)
        except Exception as e:
            logger.error(f"Error getting post comments dynamics: {e}")
            return []

    def get_top_posts(self, metric_type, limit=10, start_date=None, end_date=None):
        table_name = ""
        time_field = ""

        if metric_type == statistics_service_pb2.VIEWS:
            table_name = "views"
            time_field = "view_time"
        elif metric_type == statistics_service_pb2.LIKES:
            table_name = "likes"
            time_field = "like_time"
        elif metric_type == statistics_service_pb2.COMMENTS:
            table_name = "comments"
            time_field = "comment_time"
        else:
            return []

        query = f"""
        SELECT 
            post_id,
            COUNT(*) as count
        FROM {table_name}
        WHERE 1=1
        """

        if start_date:
            query += f" AND toDate({time_field}) >= toDate('{start_date}')"
        if end_date:
            query += f" AND toDate({time_field}) <= toDate('{end_date}')"

        query += f" GROUP BY post_id ORDER BY count DESC LIMIT {limit}"

        try:
            return self.client.execute(query)
        except Exception as e:
            logger.error(f"Error getting top posts: {e}")
            return []

    def get_top_users(self, metric_type, limit=10, start_date=None, end_date=None):
        table_name = ""
        time_field = ""

        if metric_type == statistics_service_pb2.VIEWS:
            table_name = "views"
            time_field = "view_time"
        elif metric_type == statistics_service_pb2.LIKES:
            table_name = "likes"
            time_field = "like_time"
        elif metric_type == statistics_service_pb2.COMMENTS:
            table_name = "comments"
            time_field = "comment_time"
        else:
            return []

        query = f"""
        SELECT 
            user_id,
            COUNT(*) as count
        FROM {table_name}
        WHERE 1=1
        """

        if start_date:
            query += f" AND toDate({time_field}) >= toDate('{start_date}')"
        if end_date:
            query += f" AND toDate({time_field}) <= toDate('{end_date}')"

        query += f" GROUP BY user_id ORDER BY count DESC LIMIT {limit}"

        try:
            return self.client.execute(query)
        except Exception as e:
            logger.error(f"Error getting top users: {e}")
            return []


class StatsServicer(statistics_service_pb2_grpc.StatsServiceServicer):
    def __init__(self, clickhouse_client):
        self.ch_client = clickhouse_client

    def GetPostStats(self, request, context):
        try:
            views_count, likes_count, comments_count = self.ch_client.get_post_stats(
                str(request.post_id)
            )

            return statistics_service_pb2.PostStatsResponse(
                views_count=views_count,
                likes_count=likes_count,
                comments_count=comments_count,
            )
        except Exception as e:
            logger.error(f"Error in GetPostStats: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return statistics_service_pb2.PostStatsResponse()

    def GetPostViewsDynamics(self, request, context):
        try:
            dynamics = self.ch_client.get_post_views_dynamics(
                str(request.post_id), request.start_date, request.end_date
            )

            response = statistics_service_pb2.PostDynamicsResponse()
            for date, count in dynamics:
                daily_stat = response.daily_stats.add()
                daily_stat.date = date.strftime("%Y-%m-%d")
                daily_stat.count = count

            return response
        except Exception as e:
            logger.error(f"Error in GetPostViewsDynamics: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return statistics_service_pb2.PostDynamicsResponse()

    def GetPostLikesDynamics(self, request, context):
        try:
            dynamics = self.ch_client.get_post_likes_dynamics(
                str(request.post_id), request.start_date, request.end_date
            )

            response = statistics_service_pb2.PostDynamicsResponse()
            for date, count in dynamics:
                daily_stat = response.daily_stats.add()
                daily_stat.date = date.strftime("%Y-%m-%d")
                daily_stat.count = count

            return response
        except Exception as e:
            logger.error(f"Error in GetPostLikesDynamics: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return statistics_service_pb2.PostDynamicsResponse()

    def GetPostCommentsDynamics(self, request, context):
        try:
            dynamics = self.ch_client.get_post_comments_dynamics(
                str(request.post_id), request.start_date, request.end_date
            )

            response = statistics_service_pb2.PostDynamicsResponse()
            for date, count in dynamics:
                daily_stat = response.daily_stats.add()
                daily_stat.date = date.strftime("%Y-%m-%d")
                daily_stat.count = count

            return response
        except Exception as e:
            logger.error(f"Error in GetPostCommentsDynamics: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return statistics_service_pb2.PostDynamicsResponse()

    def GetTopPosts(self, request, context):
        try:
            top_posts = self.ch_client.get_top_posts(
                request.metric_type,
                request.limit or 10,
                request.start_date,
                request.end_date,
            )

            response = statistics_service_pb2.TopPostsResponse()
            for post_id, count in top_posts:
                post_stat = response.posts.add()
                post_stat.post_id = int(post_id)
                post_stat.count = count

            return response
        except Exception as e:
            logger.error(f"Error in GetTopPosts: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return statistics_service_pb2.TopPostsResponse()

    def GetTopUsers(self, request, context):
        try:
            top_users = self.ch_client.get_top_users(
                request.metric_type,
                request.limit or 10,
                request.start_date,
                request.end_date,
            )

            response = statistics_service_pb2.TopUsersResponse()
            for user_id, count in top_users:
                user_stat = response.users.add()
                user_stat.user_id = int(user_id)
                user_stat.count = count

            return response
        except Exception as e:
            logger.error(f"Error in GetTopUsers: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return statistics_service_pb2.TopUsersResponse()


class KafkaConsumerWorker:
    def __init__(self, clickhouse_client):
        self.ch_client = clickhouse_client
        self.consumers = {}
        self.running = True

        topics = [
            (KAFKA_TOPIC_VIEWS, self.process_view),
            (KAFKA_TOPIC_LIKES, self.process_like),
            (KAFKA_TOPIC_COMMENTS, self.process_comment),
        ]

        for topic, processor in topics:
            thread = Thread(target=self.consume_topic, args=(topic, processor))
            thread.daemon = True
            thread.start()

    def consume_topic(self, topic, processor):
        retries = 10
        delay = 5
        consumer = None

        for attempt in range(retries):
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    auto_offset_reset="earliest",
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                    group_id=f"stats_service_{topic}_group",
                )
                logger.info(f"Successfully connected to Kafka topic: {topic}")
                break
            except Exception as e:
                logger.error(f"Failed to connect to Kafka topic {topic}: {e}")
                if attempt < retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to connect to Kafka after {retries} attempts")
                    return

        self.consumers[topic] = consumer

        try:
            for message in consumer:
                if not self.running:
                    break
                try:
                    processor(message.value)
                except Exception as e:
                    logger.error(f"Error processing message from topic {topic}: {e}")
        except Exception as e:
            logger.error(f"Error consuming from Kafka topic {topic}: {e}")

    def process_view(self, data):
        user_id = str(data.get("user_id"))
        post_id = str(data.get("post_id"))
        view_time = datetime.fromisoformat(data.get("view_time"))
        view_type = data.get("view_type", "detail")

        self.ch_client.insert_view(user_id, post_id, view_time, view_type)
        logger.debug(f"Processed view: user_id={user_id}, post_id={post_id}")

    def process_like(self, data):
        user_id = str(data.get("user_id"))
        post_id = str(data.get("post_id"))
        like_time = datetime.fromisoformat(data.get("like_time"))

        self.ch_client.insert_like(user_id, post_id, like_time)
        logger.debug(f"Processed like: user_id={user_id}, post_id={post_id}")

    def process_comment(self, data):
        user_id = str(data.get("user_id"))
        post_id = str(data.get("post_id"))
        comment_id = str(data.get("comment_id"))
        comment_time = datetime.fromisoformat(data.get("comment_time"))
        content = data.get("content", "")

        self.ch_client.insert_comment(
            user_id, post_id, comment_id, comment_time, content
        )
        logger.debug(
            f"Processed comment: user_id={user_id}, post_id={post_id}, comment_id={comment_id}"
        )

    def stop(self):
        self.running = False
        for consumer in self.consumers.values():
            consumer.close()


def serve():
    clickhouse_client = ClickHouseClient()

    kafka_worker = KafkaConsumerWorker(clickhouse_client)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    statistics_service_pb2_grpc.add_StatsServiceServicer_to_server(
        StatsServicer(clickhouse_client), server
    )
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()

    logger.info(f"Stats service started on port {GRPC_PORT}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)
        kafka_worker.stop()
        logger.info("Stats service stopped")


if __name__ == "__main__":
    serve()
