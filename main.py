import praw
import pandas as pd
import praw.models
import concurrent.futures

reddit = praw.Reddit("IRProject")
reddit.read_only = True

sub = reddit.subreddit("HobbyDrama")

def scrape_posts(posts, file_name, seen_ids):
    dict= {"Title": [], "Body": [], "ID": [], "Score": [], "URL": [], "Permalink": [], "Number of comments": [], "Comments": []}
    for post in posts:
        if post.id in seen_ids:
            continue

        seen_ids.add(post.id)
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

    df = pd.DataFrame(dict)
    df.drop_duplicates(subset="ID", keep="first", inplace=True)
    df.to_json(file_name, orient='records', lines=True)

seen_ids = set()

with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
    future_new = executor.submit(scrape_posts, sub.new(limit=100), "new_posts.json", seen_ids)
    future_hot = executor.submit(scrape_posts, sub.hot(limit=100), "hot_posts.json", seen_ids)
    future_top_day = executor.submit(scrape_posts, sub.top(time_filter="day",limit=100), "top_posts_day.json", seen_ids)
    future_top_week = executor.submit(scrape_posts, sub.top(time_filter="week",limit=100), "top_posts_week.json", seen_ids)
    future_top_month = executor.submit(scrape_posts, sub.top(time_filter="month",limit=100), "top_posts_month.json", seen_ids)
    future_top_year = executor.submit(scrape_posts, sub.top(time_filter="year",limit=100), "top_posts_year.json", seen_ids)
    future_top_all = executor.submit(scrape_posts, sub.top(time_filter="all",limit=100), "top_posts_all.json", seen_ids)
