import heapq
import json
import re
import sys
import time
from itertools import chain
from statistics import mean

import requests
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

TOP10 = "top10"
TOTAL_COUNT_IN_60S = "total_count_in_60s"
TOTAL_ACOUNT = "total_count"
AVG_STARS = "avg_stars"


def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def merge_list(new_values, total_sum):
    return list(chain(*new_values)) + (total_sum or [])


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']


def debug(time, rdd):
    print(rdd.collect()[:1])


def to_words(lang_s):
    lang, s = lang_s
    if not s:
        return []
    s = re.sub('[^a-zA-Z ]', '', s)
    for w in s.split():
        yield lang, w


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']


def send_df_to_dashboard(df, key, time):
    url = 'http://webapp:5000/updateData'
    data = df.rdd.map(lambda x: x.asDict()).collect()
    package = {'key': key, 'time': time.strftime('%Y-%m-%d %H:%M:%S'), 'data': data}
    rsp = requests.post(url, json=package)
    print(f"status {rsp.status_code}, {rsp.content}")


def save_top_10(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        rdd = rdd.flatMap(
            lambda x: [{'language': x[0], 'word': word_cnt[0], 'count': word_cnt[1]} for word_cnt in x[1]])
        sql_context = get_sql_context_instance(rdd.context)
        results_df = sql_context.createDataFrame(rdd)
        results_df.createOrReplaceTempView("results")
        results_df = sql_context.sql("select language, word, count from results order by language, count desc")
        results_df.show(30)
        send_df_to_dashboard(results_df, TOP10, time)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def save_result(time, rdd, key):
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        results_df = sql_context.createDataFrame(rdd)
        results_df.createOrReplaceTempView("results")
        results_df = sql_context.sql("select * from results")
        results_df.show()
        send_df_to_dashboard(results_df, key, time)
    except ValueError:
        print("Waiting for data...")
    except Exception as e:
        print("Error: %s" % e)


def get_top10(a, b):
    return list(heapq.nlargest(10, a + b, key=lambda x: x[1]))


if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName="NineMultiples")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 15)
    ssc.checkpoint("checkpoint_NineMultiples")
    """
    # keys 
    ['id', 'node_id', 'name', 'full_name', 'private', 'owner', 'html_url', 'description', 'fork', 'url', 'forks_url',
     'keys_url', 'collaborators_url', 'teams_url', 'hooks_url', 'issue_events_url', 'events_url', 'assignees_url',
     'branches_url', 'tags_url', 'blobs_url', 'git_tags_url', 'git_refs_url', 'trees_url', 'statuses_url',
     'languages_url', 'stargazers_url', 'contributors_url', 'subscribers_url', 'subscription_url', 'commits_url',
     'git_commits_url', 'comments_url', 'issue_comment_url', 'contents_url', 'compare_url', 'merges_url', 'archive_url',
     'downloads_url', 'issues_url', 'pulls_url', 'milestones_url', 'notifications_url', 'labels_url', 'releases_url',
     'deployments_url', 'created_at', 'updated_at', 'pushed_at', 'git_url', 'ssh_url', 'clone_url', 'svn_url',
     'homepage', 'size', 'stargazers_count', 'watchers_count', 'language', 'has_issues', 'has_projects',
     'has_downloads', 'has_wiki', 'has_pages', 'has_discussions', 'forks_count', 'mirror_url', 'archived', 'disabled',
     'open_issues_count', 'license', 'allow_forking', 'is_template', 'web_commit_signoff_required', 'topics',
     'visibility', 'forks', 'open_issues', 'watchers', 'default_branch', 'score']
    """
    start_time = time.time()
    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    # Compute the total number of the collected repositories since the start of the streaming application
    # for each of the three programming languages. Each repository should be counted only once.
    repo_infos = data.map(json.loads).flatMap(lambda x: x['items']).filter(
        lambda x: x['language'] in ['Python', 'JavaScript', 'Java'])
    counts = repo_infos.map(lambda x: (x['language'], 1)).reduceByKey(lambda a, b: a + b)
    aggregated_counts = counts.updateStateByKey(aggregate_count).map(lambda x: {'language': x[0], 'count': x[1]})
    aggregated_counts.foreachRDD(lambda t, rdd: save_result(t, rdd, TOTAL_ACOUNT))

    # Compute the number of the collected repositories with changes pushed during the last 60 seconds.
    # Each repository should be counted only once during a batch interval (60 seconds).

    windowedCounts = repo_infos.map(lambda x: (x['language'], 1)).reduceByKeyAndWindow(
        lambda x, y: x + y,
        lambda x, y: x - y,
        60, 60).map(lambda x: {'language': x[0], 'count': x[1]})
    windowedCounts.foreachRDD(lambda t, rdd: save_result(t, rdd, TOTAL_COUNT_IN_60S))

    # Compute the average number of stars of all the collected repositories since the start of the streaming application
    # for each of the three programming languages. Each repository counts towards the result only once.
    avg_star = repo_infos.map(lambda x: (x['language'], x['stargazers_count'])).groupByKey().mapValues(list)
    aggregated_counts = avg_star.updateStateByKey(merge_list).map(
        lambda x: {"language": x[0], "count": len(x[1]), "avg_star": mean(x[1])})
    aggregated_counts.pprint()
    aggregated_counts.foreachRDD(lambda t, rdd: save_result(t, rdd, AVG_STARS))

    # Find the top 10 most frequent words in the description of all the collected repositories
    # since the start of the streaming application for each of the three programming languages.
    # Each repository counts towards the result only once. You don't need to process the project description
    # if it is empty (null). You should use the statement re.sub('[^a-zA-Z ]', '', DESCRIPTION_STRING)
    # to strip the description before extracting words.
    words = repo_infos.map(lambda x: (x['language'], x.get('description', ''))).flatMap(to_words).map(lambda x: (x, 1))
    word_cnt = words.reduceByKey(lambda a, b: a + b)
    word_cnt = word_cnt.updateStateByKey(aggregate_count)
    # get top 10 by language
    word_cnt = word_cnt.map(lambda x: (x[0][0], [(x[0][1], x[1])])).reduceByKey(get_top10)
    word_cnt.pprint()
    # top 10
    word_cnt.map(lambda x: [x]).reduce(lambda a, b: a + b).flatMap(lambda x: x).foreachRDD(save_top_10)

    end_time = time.time()
    ssc.start()
    ssc.awaitTermination()
