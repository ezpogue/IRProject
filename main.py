import praw
import pandas as pd
import concurrent.futures

reddit = praw.Reddit("IRProject")
reddit.read_only = True

sub = reddit.subreddit("HobbyDrama")

def scrape_posts(posts, file_name, seen_ids):
    dict= {"Title": [], "Body": [], "ID": [], "Score": [], "URL": [], "Permalink": [], "Number of comments": [], "Comments": []}
    for post in posts:
        if post.id in seen_ids or post.id in dict["ID"]:
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
        dict["Comments"].append([comment.body for comment in submission.comments])

    df = pd.DataFrame(dict).drop_duplicates(subset="ID", keep="first")
    df.to_json(file_name, orient='records', lines=True)

def scrape_author_posts(author_name, seen_ids):
    author = reddit.redditor(author_name)
    author_upvotes = [submission.score for submission in author.submissions.new()]
    if len(author_upvotes) > 0 and sum(author_upvotes) / len(author_upvotes) >= 100:
        author_posts = [submission for submission in author.submissions.new()]
        scrape_posts(author_posts, f"{author_name}.json", seen_ids)
        print(f"Saved data for {author_name} to {author_name}.json")
    else:
        print(f"Not scraping feed for {author_name}, average upvotes < 100")

seen_ids = set()
thread_count = 0

with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
    futures = []
    thread_count +=1
    futures.append(executor.submit(scrape_posts, sub.new(limit=100), "new_posts.json", seen_ids))
    thread_count +=1
    futures.append(executor.submit(scrape_posts, sub.hot(limit=100), "hot_posts.json", seen_ids))
    thread_count +=1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="day",limit=100), "top_posts_day.json", seen_ids))
    thread_count +=1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="week",limit=100), "top_posts_week.json", seen_ids))
    thread_count +=1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="month",limit=100), "top_posts_month.json", seen_ids))
    thread_count +=1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="year",limit=100), "top_posts_year.json", seen_ids))
    thread_count +=1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="all",limit=100), "top_posts_all.json", seen_ids))
    
    # Collect a list of authors to scrape
    authors = set()
    for post in sub.top(time_filter="year",limit=100):
        if post.author is not None and post.author.name not in authors:
            authors.add(post.author.name)
    
    # Scrape author feeds
    for author_name in authors:
        thread_count +=1
        futures.append(executor.submit(scrape_author_posts, author_name, seen_ids))
        
    print(f"{thread_count} threads running")
    
    for future in concurrent.futures.as_completed(futures):
        thread_count -=1
        print(f"{thread_count} threads running") ##checking if multi-threading works with author posts