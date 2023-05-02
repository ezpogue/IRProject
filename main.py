import praw
import pandas
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
        com_dict['id'] = c.id
        com_dict['author'] = c.author
        com_dict['parent_id'] = c.parent_id
        com_dict['score'] = c.score
        com_dict['created_utc'] = c.created_utc
        com_dict['permalink'] = c.permalink
        com_dict['subreddit'] = c.subreddit
        com_dict['self_text'] = c.body
        com_dict['self_text_url'] = find_url(c.body)
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
    ## submission.comments.replace_more(limit=0)
    post["comments"] = convert_comment(submission.comment)
    
    
    
    ##Todo
    ##Search for URLs in this post, and assign them a name + link for the json   
    ##Access HTML Header, Grab the title of the website.
        
    ##ToDo
    ##Search through comments and seperate title of links and url for each
    
        ##Downlaod or just have the link to a seperate subreddit with link

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
    ##ToDo
    
    ##Split this into crawlers for New, Hot, Top(day,week,month,year)
    
    ##Check for new comments or updates, include this for comments
    
    for submission in reddit.subreddit(crawl).hot(limit = 5):
        post[crawl]["title"] = submission.title
        ##ToDo
        ##Tracking Frequency of this Author, incase they are a major contributor. Consider threshhold for this 
        post[crawl]["author"] = submission.author.name
        post[crawl]["self_url"] = submission.url        
        post[crawl]["self_text"] = submission.selftext
        
        ##Todo
        ##Search for URLs in this post, and assign them a name + link for the json   
        ##Access HTML Header, Grab the title of the website.
         
        ##ToDo
        ##Search through comments and seperate title of links and url for each
        
        ##Downlaod or just have the link to a seperate subreddit with link
        
    print(post)

##ToDo
##Dump post into a json file
with open('data.json','w') as f:
    json.dump(post,f)        
         
