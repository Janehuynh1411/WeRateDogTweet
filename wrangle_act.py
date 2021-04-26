#!/usr/bin/env python
# coding: utf-8

# # Gather

# In[2]:


import pandas as pd
df_1 = pd.read_csv('twitter-archive-enhanced.csv')
df_1.head()


# In[137]:


import tweepy
from tweepy import OAuthHandler
import json
from timeit import default_timer as timer

# Query Twitter API for each tweet in the Twitter archive and save JSON in a text file
# These are hidden to comply with Twitter's API terms and conditions
consumer_key = 'HIDDEN'
consumer_secret = 'HIDDEN'
access_token = 'HIDDEN'
access_secret = 'HIDDEN'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

tweet_ids = df_1.tweet_id.values
len(tweet_ids)

# Query Twitter's API for JSON data for each tweet ID in the Twitter archive
count = 0
fails_dict = {}
start = timer()
# Save each tweet's returned JSON as a new line in a .txt file
with open('tweet_json.txt', 'w') as outfile:
    # This loop will likely take 20-30 minutes to run because of Twitter's rate limit
    for tweet_id in tweet_ids:
        count += 1
        print(str(count) + ": " + str(tweet_id))
        try:
            tweet = api.get_status(tweet_id, tweet_mode='extended')
            print("Success")
            json.dump(tweet._json, outfile)
            outfile.write('\n')
        except tweepy.TweepError as e:
            print("Fail")
            fails_dict[tweet_id] = e
            pass
end = timer()
print(end - start)
print(fails_dict)


# In[3]:


df_image=pd.read_csv('image-predictions.tsv',sep='\t')


# In[4]:


with open('tweet_json.txt', 'r') as file:
    tweet_json = pd.read_json('tweet_json.txt', lines=True)


# In[5]:


tweet_json


# # Assess

# Assess df_1

# In[141]:


df_1.head()


# In[142]:


df_1.info()
# tweet id should be object
# retweeted_status_id  should be object
# retweeted_status_user_id should be object
# timestamp should be date time
# retweeted_status_timestamp should be date time
# expanded_urls should contain only the link not html command


# In[143]:


list(df_1)


# In[144]:


df_1.describe()


# In[145]:


# Check quality issue on the name column
df_1['name'].value_counts()
# Remove "a", "an", "the" in df_1['name'] 
# Quality issue


# In[146]:


list(df_1['name'])
# "his", "one" are not names
# Quality issue


# In[147]:


# Check to see if there is any quality issue in the doggo column
# doggo == "None" or "doggo"
df_1['doggo'].value_counts()
# No quality issue here


# In[148]:


# Check to see if there is any quality issue in the floofer column
# floofer == "None" or "floofer"
df_1['floofer'].value_counts()
# No quality issue here


# In[149]:


# Check to see if there is any quality issue in the pupper column
# pupper == "None" or "pupper"
df_1['pupper'].value_counts()
# No quality issue here


# In[150]:


# Check to see if there is any quality issue in the puppo column
# puppo == "None" or "puppo"
df_1['puppo'].value_counts()
# No quality issue here


# In[151]:


# Check to see if there is any quality issue in the rating_denominator column
# Rating_denominator == 10
df_1['rating_denominator'].value_counts()
# There are other values in this column rather than 10
# Quality issue


# In[152]:


# Check if there is any duplicate tweet
sum(df_1['tweet_id'].duplicated())
# All clear


# Assess df image

# In[153]:


df_image.info()


# In[154]:


df_image.describe()


# In[155]:


df_image['p1'].value_counts()
# Some values are not a legit dog breed
# Quality issue


# In[156]:


df_image['p2'].value_counts()
# Some values are not a legit dog breed
# Quality issue


# In[157]:


df_image['p3'].value_counts()
# Some values are not a legit dog breed
# Quality issue


# In[158]:


# Check if there is any duplicate tweet
sum(df_image['tweet_id'].duplicated())
# All clear


# In[159]:


# p1_conf <= 1
df_image['p1_conf'].max()
# No quality issue here


# In[160]:


# p2_conf <= 1
df_image['p2_conf'].max()
# No quality issue here


# In[161]:


# p3_conf <= 1
df_image['p3_conf'].max()
# No quality issue here


# In[162]:


# p1_dog == "True" or "False"
df_image['p1_dog'].value_counts()
# No quality issue here


# In[163]:


# p2_dog == "True" or "False"
df_image['p2_dog'].value_counts()
# No quality issue here


# In[164]:


# p3_dog == "True" or "False"
df_image['p3_dog'].value_counts()
# No quality issue here


# Assess df_tweet

# In[170]:


tweet_json.head()


# In[166]:


tweet_json.info()
# contributors does not have any data
# coordinates does not have any data
# geo does not have any data
# place has only one row
# favorite_count should be int
# id should be object


# In[168]:


tweet_json['quoted_status_id_str']


# In[169]:


tweet_json['extended_entities'].sample(10)
# tidiness issue
# seperate tweet_json['extended_entities'] into columns by comma


# In[ ]:


tweet_json['user'].sample(10)
# tidiness issue
# seperate tweet_json['user'] into columns by comma


# In[ ]:


tweet_json['quoted_status'].sample(10)
# tidiness issue
# value should not be a dictionary
# seperate values that contain a dictionary in tweet_json['quoted_status']


# In[ ]:


tweet_json['retweeted_status'].sample(10)
# tidiness issue
# value should not be a dictionary
# seperate values that contain a dictionary in tweet_json['retweeted_status']


# In[ ]:


tweet_json['source'].sample(10)
# quality issue
# Remove html tags in tweet_json['source']


# In[ ]:


tweet_json.describe()


# # Quality issues

# df_1

# 1) Change tweet id to object

# 2) Change retweeted_status_id to object

# 3) Change timestamp to date time

# 4) Change retweeted_status_timestamp to date time

# 5) Remove "a", "an", "the", "his", "her", "one" in df_1['name'] 

# 6) Change all rows whose values != 10 in df_1['rating_denominator'] to 10

#      

# tweet_json

# 7) Drop columns that do not contain values: contributors, coordinates, geo, place, possibly_sensitive, possibly_sensitive_id, possibly_sensitive_id_string

# 8) favorite_count should be int

# 9) id should be object

#     

# df_image

# 8) Remove values that are not dog breeds in p1, p2 and p3

# # Tidiness issues

# df_1

# 12) Remove html command <href> in the expanded_urls

# tweet_json.txt

# 13) Remove html tags in tweet_json['source']

# # Clean

# df_1

# In[ ]:


# 1) Change tweet id to object
df_1['tweet_id'] = df_1['tweet_id'].astype(object)


# In[ ]:


# 2) Change retweeted_status_id to object
df_1['retweeted_status_id'] = df_1['retweeted_status_id'].astype(object)


# In[ ]:


# 3) Change timestamp to date time
df_1['timestamp'] = df_1['timestamp'].astype('datetime64[ns]')


# In[ ]:


# 4) Change retweeted_status_timestamp to date time
df_1['retweeted_status_timestamp'] = df_1['retweeted_status_timestamp'].astype('datetime64[ns]')


# In[ ]:


# 6) Remove "a", "an", "the", "his", "her", "one" in df_1['name']
df_1['name'] = df_1['name'].replace(['a','an','the','his','her','one','not'],'None')
list(df_1['name'])


# In[ ]:


# 7) Change all rows whose values != 10 in df_1['rating_denominator'] to 10
df_1['rating_denominator'] = df_1['rating_denominator'].replace([11,50,80,20,2,16,40,70,15,90,110,120,130,150,170,7,0],10)
df_1['rating_denominator'].value_counts()


# In[ ]:


# 12) Remove html command in the expanded_urls
df_1['source'] = df_1['source'].str.strip("<a href=")
df_1['source'] = df_1['source'].str.strip("</a>")
df_1['source'] = df_1['source'].str.split(pat="rel=", expand=True)
df_1['source'].sample(5)


# df_image

# tweet_json

# In[ ]:


# 9) Remove html tags in tweet_json['source']
tweet_json['source'] = tweet_json['source'].str.strip("<a href=")
tweet_json['source'] = tweet_json['source'].str.strip("</a>")
tweet_json['source'] = tweet_json['source'].str.split(pat="rel=", expand=True)
tweet_json['source'].sample(5)


# In[ ]:


# 10) Drop columns that do not contain values: contributors, coordinates, geo, place, possibly_sensitive, possibly_sensitive_appealable,lang, entities, extend_entities
tweet_json = tweet_json.drop(columns=['contributors','coordinates','geo','place','possibly_sensitive','lang','possibly_sensitive_appealable', 'id_str','retweeted_status','in_reply_to_user_id_str','quoted_status_id_str','entities','extended_entities', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'in_reply_to_status_id_str','in_reply_to_user_id','quoted_status','quoted_status_id','user'])
tweet_json.info()


# In[ ]:


tweet_json.head()


# In[ ]:


tweet_json.rename(columns={'id': 'tweet_id', 'full_text': 'text', 'created_at': 'timestamp'})


# In[ ]:


df_clean = pd.merge(df_1.reset_index(), tweet_json.reset_index(), on=['tweet_id', 'text', 'timestamp', 'source'])


# In[ ]:




