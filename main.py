import praw
import pandas as pd
import praw.models

reddit = praw.Reddit("IRProject")
reddit.read_only = True

sub = reddit.subreddit("HobbyDrama")
posts = sub.top(time_filter="month",limit=100)

dict= {"Title": [], "Body": [], "ID": [], "Score": [], "URL": [], "Permalink": [], "Number of comments": [], "Comments": [], "Hyperlinks": []}


def extract_hyperlinks(post):
    post_text = post.selftext
    words = post_text.split()
    links = []
    for word in words:
        if word.startswith("http") or word.startswith("https"):
            links.append(word)
    return links

for post in posts:
        dict["Title"].append(post.title)
        dict["Body"].append(post.selftext)
        dict["ID"].append(post.id)
        dict["Score"].append(post.score)
        dict["URL"].append(post.url)
        dict["Permalink"].append(post.permalink)
        dict["Number of comments"].append(post.num_comments)
        submission = reddit.submission(post.id)
        submission.comment_sort = "best"
        submission.comments.replace_more(limit=0)
        c = []
        for comment in submission.comments:
                c.append(comment.body)
        dict["Comments"].append(c)
        dict["Hyperlinks"].append(extract_hyperlinks(post))

df = pd.DataFrame(dict)
print(df)
f = open("test.json", "w")
f.write(df.to_json(orient='records',lines=True))
