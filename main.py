import praw
import pandas
import json

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
    ##Check for new comments or updates and dupes
    for submission in reddit.subreddit(crawl).hot(limit = 5):
        post[crawl]["title"] = submission.title
        
        ##ToDo
        ##Tracking Frequency of this Author, incase they are a major contributor. Consider threshhold for this 
        post[crawl]["author"] = submission.author.name
        post[crawl]["self_url"] = submission.url        
        post[crawl]["self_text"] = submission.selftext
        
        ##Todo
        ##Search for URLs in this post, and assign them a name + link for the json   
        
        ##ToDo
        ##Search through comments and seperate title of links and url for each
        
        ##Downlaod or just have the link to a seperate subreddit with link?
        
        
    print(post)

##ToDo
##Dump post into a json file
with open('data.json','w') as f:
    json.dump(post,f)        
         
