import praw
import pandas as pd
from urllib.parse import urlparse, parse_qs
import re
import requests
from bs4 import BeautifulSoup
import concurrent.futures

futures = []

reddit = praw.Reddit("IRProject")
reddit.read_only = True

dict = {"Title": [], "Author": [], "Subreddit": [], "Body": [], "ID": [], "Score": [], "Ratio": [], "URL": [], "Permalink": [], "Number of comments": [], "Comments": [], "Text URL": []}
subreddit_frequency = {}
scrape_subreddit = []

seen_ids = set()

url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
subreddit_pattern = re.compile(r'^https?://(www\.)?reddit\.com/r/[\w-]+/?$')
post_pattern = re.compile(r'^https?://(www\.)?reddit\.com/r/[\w-]+/comments/[\w-]+/[\w-]+/?$')
comment_pattern = re.compile(r'^https?://(www\.)?reddit\.com/r/[\w-]+/comments/[\w-]+/[\w-]+/#\w+$')
    
def extract_link_title(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content,'html.parser')
        title = soup.title.string
    except:
        title = "No title found"
    return title
def extract_text_url(self_text):
    global url_pattern
    urls = []
    reddit_url = []
    url_list = []
    urls.extend(url_pattern.findall(self_text))
    for u in urls:
        if re.search(r'reddit', u):
            if re.match(subreddit_pattern, u):
                update_frequency(reddit.subreddit(u.split('/r/')[1].split('/')[0]))
            if re.match(post_pattern, u):
                sub = reddit.submission(url=u)
                scrape(sub)
                update_frequency(sub.subreddit.display_name)
            if re.match(comment_pattern,u):
                update_frequency(reddit.comment(url=u).subreddit.display_name)
        parsed_url = urlparse(u)
        query = parse_qs(parsed_url.query)
        if "title" in query:
            new_pair = (query["title"][0],u)
        else:
            new_pair = (extract_link_title(u),u)
        url_list.append(new_pair)        
    return url_list

def update_frequency(subreddit):
    global subreddit_frequency
    global futures
    global thread
    if subreddit in subreddit_frequency:
        subreddit_frequency[subreddit] += 1
    else:
        subreddit_frequency[subreddit] = 1
    if (not subreddit in scrape_subreddit and len(scrape_subreddit) <= 15 and subreddit_frequency[subreddit] >= 50):
        sub = reddit.subreddit(subreddit)
        scrape_subreddit.append(subreddit)
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            thread_count += 1
            futures.append(executor.submit(scrape_posts, sub.new(limit=50)))
            thread_count += 1
            futures.append(executor.submit(scrape_posts, sub.hot(limit=50)))
            thread_count += 1
            futures.append(executor.submit(scrape_posts, sub.top(time_filter="day", limit=50)))
            thread_count += 1
            futures.append(executor.submit(scrape_posts, sub.top(time_filter="week", limit=50)))
            thread_count += 1
            futures.append(executor.submit(scrape_posts, sub.top(time_filter="month", limit=50)))
            thread_count += 1
            futures.append(executor.submit(scrape_posts, sub.top(time_filter="year", limit=50)))
            thread_count += 1
            futures.append(executor.submit(scrape_posts, sub.top(time_filter="all", limit=50)))
def get_comments(c):
    com = {}
    if c.author is None:
        com['Author'] = "Deleted"
    else:
        com['Author'] = c.author.name
    com['Parent ID'] = c.parent_id
    com['Body'] = c.body
    com['Text Url'] = extract_text_url(c.body)
    com['Ups'] = c.ups
    com['Downs'] = c.downs
    com['Permalink'] = c.permalink
    return com
        
def scrape_posts(posts):
    global dict
    global seen_ids
    for post in posts:
        if post.id in seen_ids or post.id in dict["ID"]:
            continue
        scrape(post)
def scrape(post):
    seen_ids.add(post.id)
    dict["Title"].append(post.title)
    dict["Author"].append(post.author.name)
    dict["Subreddit"].append(post.subreddit.display_name)
    dict["Body"].append(post.selftext)
    dict["ID"].append(post.id)
    dict["Score"].append(post.score)
    dict["Ratio"].append(post.upvote_ratio)
    dict["URL"].append(post.url)
    dict["Permalink"].append(post.permalink)
    dict["Number of comments"].append(post.num_comments)
    submission = reddit.submission(post.id)
    submission.comment_sort = "best"

    submission.comments.replace_more(limit=5)
    
    com_dict = {}
    for comment in submission.comments.list():
        if comment is None:
            continue
        com_dict[comment.id] = get_comments(comment)
    dict["Comments"].append(com_dict)      
    dict["Text URL"].append(extract_text_url(post.selftext))

def scrape_author_posts(author_name):
    author = reddit.redditor(author_name)
    author_upvotes = [submission.score for submission in author.submissions.new()]
    if len(author_upvotes) > 0 and sum(author_upvotes) / len(author_upvotes) >= 100:
        author_posts = [submission for submission in author.submissions.new()]
        scrape_posts(author_posts)
    else:
        print(f"Not scraping feed for {author_name}, average upvotes < 100")


thread_count = 0
sub = reddit.subreddit("HobbyDrama")
scrape_subreddit.append("HobbyDrama")

##Small Test Case
scrape_posts(sub.top(time_filter="all",limit=5))


def task_priority(future):
    #Calculate the priority of a task based on its execution time
    return 1 / future.result()


with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.new(limit=50)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.hot(limit=50)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="day", limit=50)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="week", limit=50)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="month", limit=50)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="year", limit=50)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="all", limit=50)))
    
    # Collect a list of authors to scrape
    authors = set()
    for post in sub.top(time_filter="year", limit=100):
        if post.author is not None and post.author.name not in authors:
            authors.add(post.author.name)
    
    # Scrape author feeds
    for author_name in authors:
        thread_count += 1
        futures.append(executor.submit(scrape_author_posts, author_name, seen_ids))
        
    print(f"{thread_count} threads running")
    

    #for future in concurrent.futures.as_completed(futures):
        #thread_count -=1
        #print(f"{thread_count} threads running") ##checking if multi-threading works with author posts

    for future in sorted(concurrent.futures.as_completed(futures), key=task_priority):
        thread_count -= 1
        print(f"{thread_count} threads running")

file_name = "data.json"
df = pd.DataFrame(dict).drop_duplicates(subset="ID", keep="first")
print(f"Writing data to {file_name}")
df.to_json(file_name, orient='records', lines=True)
print(f"Finished writing data to {file_name}")