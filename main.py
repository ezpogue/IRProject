import praw
import pandas
import json

reddit = praw.Reddit("IRProject")
reddit.read_only = True

with open('seed.json') as json_file:
    data =json.load(json_file)


for submission in reddit.subreddit("test").hot(limit=10):
    print(submission.title)

