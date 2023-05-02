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

#print execution time for threads for testing
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    ##start_new = time.perf_counter()
    future_new = executor.submit(scrape_posts, sub.new(limit=100), "new_posts.json", seen_ids)
    ##end_new = time.perf_counter()

    ##start_hot = time.perf_counter()
    future_hot = executor.submit(scrape_posts, sub.hot(limit=100), "hot_posts.json", seen_ids)
    ##end_hot = time.perf_counter()

    ##start_top = time.perf_counter()
    future_top = executor.submit(scrape_posts, sub.top(time_filter="month",limit=100), "top_posts.json", seen_ids)
    ##end_top = time.perf_counter()

    """print(f"New posts execution time: {end_new - start_new:0.4f} seconds")
    print(f"Hot posts execution time: {end_hot - start_hot:0.4f} seconds")
    print(f"Top posts execution time: {end_top - start_top:0.4f} seconds")"""
