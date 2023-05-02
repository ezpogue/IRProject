import praw
import pandas as pd
from urllib.parse import urlparse, parse_qs
import requests
from bs4 import BeautifulSoup

reddit = praw.Reddit("IRProject")
reddit.read_only = True

def extract_link_title(url):
    try:
        response = requests.get(url)            # send a HTTP GET request to the URL
        soup = BeautifulSoup(response.content,'html.parser')    # parse HTML documents & extract the title of the webpage
        title = soup.title.string
    except:
        title = "No title found"
    return title

def extract_hyperlink_titles(post):
    post_text = post.selftext
    words = post_text.split()   #splits selftext into a list of words
    hyperlink_titles = []
    for word in words:
        if word.startswith("http") or word.startswith("https"):
            parsed_url = urlparse(word)         # Parse the URL to extract the query parameters
            query = parse_qs(parsed_url.query)  # qs: extract the values of the query 
            if "title" in query:
                hyperlink_titles.append(query["title"][0])     # If exists, append the value of the "title" to hyperlink_titles 
            else:
                hyperlink_titles.append(extract_link_title(word))
    return hyperlink_titles

sub = reddit.subreddit("HobbyDrama")
posts = sub.top(time_filter="month",limit=100)

dict= {"Title": [], "Body": [], "ID": [], "Score": [], "URL": [], "Permalink": [], "Number of comments": [], "Comments": [], "Hyperlink Titles": []}

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
        dict["Hyperlink Titles"].append(extract_hyperlink_titles(post))

df = pd.DataFrame(dict)
print(df)
f = open("test.json", "w")
f.write(df.to_json(orient='records',lines=True))
