import praw
import json
import re
import requests
from bs4 import BeautifulSoup

url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
def find_url(self_text):
    global url_pattern
    urls = []
    dict_url = {}
    urls.extend(url_pattern.findall(self_text))
    for u in urls:
        response = requests.get(u)
        soup = BeautifulSoup(response.content,'html.parser')
        dict_url[soup.title.string] = u
    return dict_url
def convert_comment(comment):
    com_dict = {}
    for c in comment:
        com = {}
        com['c_author'] = c.author.name
        com['c_parent_id'] = c.parent_id
        com['c_score'] = c.score
        com['c_created_utc'] = c.created_utc
        com['c_permalink'] = c.permalink
        com['c_self_text'] = c.body
        com['c_self_text_url'] = find_url(c.body)
        com_dict[c.id] = com
    return com_dict
def convert_data(post,submission):
    post["id"] = submission.id
    post["title"] = submission.title
    post["created_utc"] = submission.created_utc
    ##ToDo
    ##Tracking Frequency of this Author, incase they are a major contributor. Consider threshhold for this 
    post["author"] = submission.author.name
    post["score"] = submission.score
    post["ratio"] = submission.upvote_ratio
    post["url"] = submission.url
    post["permalink"] = submission.permalink        
    post["self_text"] = submission.selftext
    post["self_text_urls"] = find_url(submission.selftext)
    post["num_comments"] = submission.num_comments
    submission.comment_sort = "best"
    ## No limit to nested, setting to 0 gives you only first layer
    submission.comments.replace_more(limit=None)
    post["comments"] = convert_comment(submission.comments.list())
    
    ##Download or just have the link to a seperate subreddit with link
    ##Crawl the comments to other reddit
    ##think about duplication or if cyclical.

reddit = praw.Reddit("IRProject")
reddit.read_only = True

with open('seed.json') as json_file:
    crawl_list=json.load(json_file)

post = {}

##Empty dictionary to track frequency in which an author appears
##Ratio of likes
author = {}


##ToDo
##Find limit on which i can multithread
##Split crawlers to subreddits
for crawl in crawl_list:
    post[crawl] = dict()
    for submission in reddit.subreddit(crawl).hot(limit = 1):
        convert_data(post[crawl],submission)

print(post)

##ToDo
##Dump post into a json file
with open('data.json','w') as f:
    json.dump(post,f)        
         
