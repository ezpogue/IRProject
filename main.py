import praw
import pandas
import json

reddit = praw.Reddit("IRProject")
reddit.read_only = True

with open('seed.json') as json_file:
    crawl_list=json.load(json_file)

for crawl in crawl_list:
    for submission in reddit.subreddit(crawl).hot(limit=10):
        print (submission.title)


