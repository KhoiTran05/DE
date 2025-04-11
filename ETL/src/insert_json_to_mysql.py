from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

from config.database_config import get_database_config
from config.spark_config import create_spark_session

MYSQL_CONNECTOR_PATH = '..//lib//mysql-connector-j-9.2.0.jar'
DATA_PATH = '..//data//2015-03-01-17.json'

def get_json_dataframe(spark: SparkSession, path: str, schema):
    try:
        dataframe = spark.read.json(path, schema=schema)
        return dataframe
    except Exception as e:
        print(f"Error reading JSON file at {path}: {e}")
        return None

def insert_json_to_mysql(
        db_config: Dict[str, str],
        table_name: str,
        write_mode:str,
        json_dataframe
):
    try:
        json_dataframe.write \
            .format("jdbc") \
            .option("url", db_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode(write_mode) \
            .save()
        print(f"Successfully inserted {table_name} to MySQL")
    except Exception as e:
        print(f"Error inserting JSON file at {table_name}: {e}")

schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ]), True),
    StructField("repo", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True)
    ]), True),
    StructField("payload", StructType([
        StructField("forkee", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("owner", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True)
            ]), True),
            StructField("private", BooleanType(), True),
            StructField("html_url", BooleanType(), True),
            StructField("description", StringType(), True),
            StructField("fork", BooleanType(), True),
            StructField("url", StringType(), True),
            StructField("forks_url", StringType(), True),
            StructField("keys_url", StringType(), True),
            StructField("collaborators_url", StringType(), True),
            StructField("teams_url", StringType(), True),
            StructField("hooks_url", StringType(), True),
            StructField("issue_events_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("assignees_url", StringType(), True),
            StructField("branches_url", StringType(), True),
            StructField("tags_url", StringType(), True),
            StructField("blobs_url", StringType(), True),
            StructField("git_tags_url", StringType(), True),
            StructField("git_refs_url", StringType(), True),
            StructField("trees_url", StringType(), True),
            StructField("statuses_url", StringType(), True),
            StructField("languages_url", StringType(), True),
            StructField("stargazers_url", StringType(), True),
            StructField("contributors_url", StringType(), True),
            StructField("subscribers_url", StringType(), True),
            StructField("subscription_url", StringType(), True),
            StructField("commits_url", StringType(), True),
            StructField("git_commits_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("issue_comment_url", StringType(), True),
            StructField("contents_url", StringType(), True),
            StructField("compare_url", StringType(), True),
            StructField("merges_url", StringType(), True),
            StructField("archive_url", StringType(), True),
            StructField("downloads_url", StringType(), True),
            StructField("issues_url", StringType(), True),
            StructField("pulls_url", StringType(), True),
            StructField("milestones_url", StringType(), True),
            StructField("notifications_url", StringType(), True),
            StructField("labels_url", StringType(), True),
            StructField("releases_url", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("pushed_at", TimestampType(), True),
            StructField("git_url", StringType(), True),
            StructField("ssh_url", StringType(), True),
            StructField("clone_url", StringType(), True),
            StructField("svn_url", StringType(), True),
            StructField("homepage", StringType(), True),
            StructField("size", IntegerType(), True),
            StructField("stargazers_count", IntegerType(), True),
            StructField("watchers_count", IntegerType(), True),
            StructField("language", StringType(), True),
            StructField("has_issues", BooleanType(), True),
            StructField("has_downloads", BooleanType(), True),
            StructField("has_wiki", BooleanType(), True),
            StructField("has_pages", BooleanType(), True),
            StructField("forks_count", IntegerType(), True),
            StructField("mirror_url", StringType(), True),
            StructField("open_issues_count", IntegerType(), True),
            StructField("forks", IntegerType(), True),
            StructField("open_issues", IntegerType(), True),
            StructField("watchers", IntegerType(), True),
            StructField("default_branch", StringType(), True),
            StructField("public", BooleanType(), True)
        ]), True)
    ]), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("org", StructType([
        StructField("id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ]), True)
])

spark = create_spark_session(
    app_name ="insert-json-to-mysql",
    master="local[*]",
    spark_conf={"spark.sql.shuffle.partition":"10"},
    jars=[MYSQL_CONNECTOR_PATH],
    log_level="ERROR"
)
json_df = get_json_dataframe(spark, DATA_PATH, schema)
db_config = get_database_config()

events_df = json_df.select(
    col("id"),
    col("type"),
    col("public"),
    col("created_at")
)

actor_df = json_df.select(
    col("actor.id").alias("actor_id"),
    col("id").alias("event_id"),
    col("actor.login").alias("login"),
    col("actor.gravatar_id").alias("gravatar_id"),
    col("actor.avatar_url").alias("avatar_url"),
    col("actor.url").alias("url"),
)

repositories_df = json_df.select(
    col("repo.id").alias("repo_id"),
    col("id").alias("event_id"),
    col("repo.name").alias("name"),
    col("repo.url").alias("url"),
)

payload_forkee_df = json_df.select(
    col("payload.forkee.id").alias("forkee_id"),
    col("id").alias("event_id"),
    col("payload.forkee.name").alias("name"),
    col("payload.forkee.full_name").alias("full_name"),
    col("payload.forkee.private").alias("private"),
    col("payload.forkee.html_url").alias("html_url"),
    col("payload.forkee.description").alias("description"),
    col("payload.forkee.fork").alias("fork"),
    col("payload.forkee.url").alias("url"),
    col("payload.forkee.forks_url").alias("forks_url"),
    col("payload.forkee.keys_url").alias("keys_url"),
    col("payload.forkee.collaborators_url").alias("collaborators_url"),
    col("payload.forkee.teams_url").alias("teams_url"),
    col("payload.forkee.hooks_url").alias("hooks_url"),
    col("payload.forkee.issue_events_url").alias("issue_events_url"),
    col("payload.forkee.events_url").alias("events_url"),
    col("payload.forkee.assignees_url").alias("assignees_url"),
    col("payload.forkee.branches_url").alias("branches_url"),
    col("payload.forkee.tags_url").alias("tags_url"),
    col("payload.forkee.blobs_url").alias("blobs_url"),
    col("payload.forkee.git_tags_url").alias("git_tags_url"),
    col("payload.forkee.git_refs_url").alias("git_refs_url"),
    col("payload.forkee.trees_url").alias("trees_url"),
    col("payload.forkee.statuses_url").alias("statuses_url"),
    col("payload.forkee.languages_url").alias("languages_url"),
    col("payload.forkee.stargazers_url").alias("stargazers_url"),
    col("payload.forkee.contributors_url").alias("contributors_url"),
    col("payload.forkee.subscribers_url").alias("subscribers_url"),
    col("payload.forkee.subscription_url").alias("subscription_url"),
    col("payload.forkee.commits_url").alias("commits_url"),
    col("payload.forkee.git_commits_url").alias("git_commits_url"),
    col("payload.forkee.comments_url").alias("comments_url"),
    col("payload.forkee.issue_comment_url").alias("issue_comment_url"),
    col("payload.forkee.contents_url").alias("contents_url"),
    col("payload.forkee.compare_url").alias("compare_url"),
    col("payload.forkee.merges_url").alias("merges_url"),
    col("payload.forkee.archive_url").alias("archive_url"),
    col("payload.forkee.downloads_url").alias("downloads_url"),
    col("payload.forkee.issues_url").alias("issues_url"),
    col("payload.forkee.pulls_url").alias("pulls_url"),
    col("payload.forkee.milestones_url").alias("milestones_url"),
    col("payload.forkee.notifications_url").alias("notifications_url"),
    col("payload.forkee.labels_url").alias("labels_url"),
    col("payload.forkee.releases_url").alias("releases_url"),
    col("payload.forkee.created_at").alias("created_at"),
    col("payload.forkee.updated_at").alias("updated_at"),
    col("payload.forkee.pushed_at").alias("pushed_at"),
    col("payload.forkee.git_url").alias("git_url"),
    col("payload.forkee.ssh_url").alias("ssh_url"),
    col("payload.forkee.clone_url").alias("clone_url"),
    col("payload.forkee.svn_url").alias("svn_url"),
    col("payload.forkee.homepage").alias("homepage"),
    col("payload.forkee.size").alias("size"),
    col("payload.forkee.stargazers_count").alias("stargazers_count"),
    col("payload.forkee.watchers_count").alias("watchers_count"),
    col("payload.forkee.language").alias("language"),
    col("payload.forkee.has_issues").alias("has_issues"),
    col("payload.forkee.has_downloads").alias("has_downloads"),
    col("payload.forkee.has_wiki").alias("has_wiki"),
    col("payload.forkee.has_pages").alias("has_pages"),
    col("payload.forkee.forks_count").alias("forks_count"),
    col("payload.forkee.mirror_url").alias("mirror_url"),
    col("payload.forkee.open_issues_count").alias("open_issues_count"),
    col("payload.forkee.forks").alias("forks"),
    col("payload.forkee.open_issues").alias("open_issues"),
    col("payload.forkee.watchers").alias("watchers"),
    col("payload.forkee.default_branch").alias("default_branch"),
    col("payload.forkee.public").alias("public")
)

forkee_owner_df = json_df.select(
    col("payload.forkee.owner.id").alias("owner_id"),
    col("payload.forkee.id").alias("forkee_id"),
    col("payload.forkee.owner.login").alias("login"),
    col("payload.forkee.owner.avatar_url").alias("avatar_url"),
    col("payload.forkee.owner.gravatar_id").alias("gravatar_id"),
    col("payload.forkee.owner.url").alias("url"),
    col("payload.forkee.owner.html_url").alias("html_url"),
    col("payload.forkee.owner.followers_url").alias("followers_url"),
    col("payload.forkee.owner.following_url").alias("following_url"),
    col("payload.forkee.owner.gists_url").alias("gists_url"),
    col("payload.forkee.owner.starred_url").alias("starred_url"),
    col("payload.forkee.owner.subscriptions_url").alias("subscriptions_url"),
    col("payload.forkee.owner.organizations_url").alias("organizations_url"),
    col("payload.forkee.owner.repos_url").alias("repos_url"),
    col("payload.forkee.owner.events_url").alias("events_url"),
    col("payload.forkee.owner.received_events_url").alias("received_events_url"),
    col("payload.forkee.owner.type").alias("type"),
    col("payload.forkee.owner.site_admin").alias("site_admin")
)

org_df = json_df.select(
    col("org.id").alias("org_id"),
    col("id").alias("event_id"),
    col("org.login").alias("login"),
    col("org.gravatar_id").alias("gravatar_id"),
    col("org.url").alias("url"),
    col("org.avatar_url").alias("avatar_url"),
)

# org_df.show()

insert_json_to_mysql(db_config, "Events", "append", events_df)
insert_json_to_mysql(db_config, "Actors", "append", actor_df)
insert_json_to_mysql(db_config, "Repositories", "append", repositories_df)
insert_json_to_mysql(db_config, "Payload_Forkee", "append", payload_forkee_df)
insert_json_to_mysql(db_config, "Forkee_Owner", "append", forkee_owner_df)
insert_json_to_mysql(db_config, "org", "append", org_df)


