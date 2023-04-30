import praw

reddit = praw.Reddit("IRProject")
reddit.read_only = True
print(reddit.read_only)
for submission in reddit.subreddit("test").hot(limit=10):
    print(submission.title)
