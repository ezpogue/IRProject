import praw
from urllib.parse import urlparse, parse_qs
import re
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import json
import os

futures = []

reddit = praw.Reddit("IRProject")
reddit.read_only = True

payload = []
cwd = os.getcwd()
chunk = 0
file_name = "data"
file_ext = ".json"

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
        html = response.read().decode(encoding="utf-8")
        soup = BeautifulSoup(response.content,'html.parser')
        title = soup.title.string
    except:
        title = "No title found"
    return title
def extract_text_url(self_text):
    global url_pattern
    urls = []
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
    if subreddit in subreddit_frequency:
        subreddit_frequency[subreddit] += 1
    else:
        subreddit_frequency[subreddit] = 1
    if (not subreddit in scrape_subreddit and len(scrape_subreddit) <= 15 and subreddit_frequency[subreddit] >= 50):
        scrape_subreddit.append(subreddit)
        
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
    for post in posts:
        if post.id in seen_ids:
            continue
        scrape(post)
def scrape(post):
    global payload
    global chunk
    global seen_ids
    
    dict = {}
    seen_ids.add(post.id)
    dict["Title"] = post.title
    dict["Author"] = post.author.name
    dict["Subreddit"] = post.subreddit.display_name
    dict["Body"] = post.selftext
    dict["ID"] = post.id
    dict["Score"] = post.score
    dict["Ratio"] = post.upvote_ratio
    dict["URL"] = post.url
    dict["Permalink"] = post.permalink
    dict["Number of comments"] = post.num_comments
    
    submission = reddit.submission(post.id)
    submission.comment_sort = "best"
    submission.comments.replace_more(limit=5)
    com_dict = {}
    for comment in submission.comments.list()[0:500]:
        if comment is None:
            continue
        com_dict[comment.id] = get_comments(comment)
    dict["Comments"] = com_dict      
    dict["Text URL"] = extract_text_url(post.selftext)
    
    print("finished parsing " + post.title)
    
    payload.append(dict)
    if len(payload) >= 5:
        path = os.path.join(cwd,"data",file_name + str(chunk) + file_ext)
        with open(path,'w') as file:
            json.dump(payload,file)
        chunk += 1
        payload.clear()
        print("5 Submissions have been recorded")
        

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
scrape_posts(sub.top(time_filter="all",limit=1000))

def task_priority(future):
    #Calculate the priority of a task based on its execution time
    #return 1 / future.result()
    result = future.result()
    if result is not None:
        return 1 / result
    else:
        return 0

'''
with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.new(limit=2)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.hot(limit=2)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="day", limit=2)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="week", limit=2)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="month", limit=2)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="year", limit=2)))
    thread_count += 1
    futures.append(executor.submit(scrape_posts, sub.top(time_filter="all", limit=2)))
    
    # Collect a list of authors to scrape
    authors = set()
    for post in sub.top(time_filter="year", limit=100):
        if post.author is not None and post.author.name not in authors:
            authors.add(post.author.name)
    
    # Scrape author feeds
    for author_name in authors:
        thread_count += 1
        futures.append(executor.submit(scrape_author_posts, author_name))
        
    print(f"{thread_count} threads running")
    

    #for future in concurrent.futures.as_completed(futures):
        #thread_count -=1
        #print(f"{thread_count} threads running") ##checking if multi-threading works with author posts

    for future in sorted(concurrent.futures.as_completed(futures), key=task_priority):
        thread_count -= 1
        #print(f"{thread_count} threads running")
'''


path = os.path.join(cwd,"data",file_name + str(chunk) + file_ext)
chunk += 1
with open(path,'w') as file:
    json.dump(payload,file)
payload.clear()
print("Remainder Data saved")

