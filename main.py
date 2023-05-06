import praw
from urllib.parse import urlparse, parse_qs
import re
import requests
from bs4 import BeautifulSoup
import json
import os
import queue
import sys

reddit = praw.Reddit("IRProject")
reddit.read_only = True

payload = []
cwd = os.getcwd()
chunk = 0
file_name = "data"
file_ext = ".json"

subreddit_frequency = {}
scrape_subreddit = []
scrape_queue = queue.Queue()

seen_ids = set()

url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
subreddit_pattern = re.compile(r'^https?://(www\.)?reddit\.com/r/[\w-]+/?$')
post_pattern = re.compile(r'^https?://(www\.)?reddit\.com/r/[\w-]+/comments/[\w-]+/[\w-]+/?$')
comment_pattern = re.compile(r'^https?://(www\.)?reddit\.com/r/[\w-]+/comments/[\w-]+/[\w-]+/#\w+$')
    
def extract_link_title(url):
    try:
        response = requests.get(url, timeout=10)
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
        if re.match(subreddit_pattern, u):
            update_frequency(reddit.subreddit(u.split('/r/')[1].split('/')[0]))
        if re.match(post_pattern, u):
            sub = reddit.submission(url=u)
            if(sub.id not in seen_ids):
                print("link to new post occured, begin scraping")
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
    if (subreddit not in scrape_subreddit and len(scrape_subreddit) <= 100 and subreddit_frequency[subreddit] >= 1):
        print("subreddit: " + str(subreddit) + " added to scrape queue")
        scrape_subreddit.append(subreddit)
        scrape_queue.put(subreddit)
    
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
    if post.author is None:
        dict["Author"] = "Deleted"
    else:
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
    
    print("finished parsing " + post.id + " from " + post.subreddit.display_name)
    payload.append(dict)
    
    json_size = json.dumps(payload)
    json_size_bytes = sys.getsizeof(json_size)
    if json_size_bytes >= 1000000:
        path = os.path.join(cwd,"data",file_name + str(chunk) + file_ext)
        with open(path,'w',encoding='utf-8') as file:
            json.dump(payload,file, ensure_ascii=False)
            file.write('\n')
        chunk += 1
        payload.clear()
        print("10 mb saved to data" + str(chunk))
        
def scrape_author_posts(author_name):
    author = reddit.redditor(author_name)
    author_upvotes = [submission.score for submission in author.submissions.new()]
    if len(author_upvotes) > 0 and sum(author_upvotes) / len(author_upvotes) >= 100:
        author_posts = [submission for submission in author.submissions.new()]
        scrape_posts(author_posts)
    else:
        print(f"Not scraping feed for {author_name}, average upvotes < 100")

with open('seed.json', 'r') as file:
    seed = json.load(file)

for s in seed:
    scrape_queue.put(s)
    scrape_subreddit.append(s)
    
print("Crawling " + str(scrape_queue.qsize()) + " from seed")
##Test Case
while(scrape_queue.qsize() > 0):
    sub = reddit.subreddit(scrape_queue.get())
    scrape_posts(sub.top(time_filter="all",limit=1000))

path = os.path.join(cwd,"data",file_name + str(chunk) + file_ext)
chunk += 1
with open(path,'w',encoding='utf-8') as file:
    json.dump(payload,file, ensure_ascii=False)
    file.write('\n')
payload.clear()
print("Remainder Data saved")

