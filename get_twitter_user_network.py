#!/usr/bin/env python
# -*- coding: utf-8 -*-

#Using api.followers_ids,api.friends_ids fetching followers and friends of specific user and api.lookup_users TWITTER APIs
import tweepy
import itertools
from datetime import datetime
from py2neo import Graph,neo4j,Relationship,Node,PropertyContainer

#open Neo4j graph database to store the data
graph = Graph()

#result pagination
def paginate(iterable, page_size):
    while True:
        i1, i2 = itertools.tee(iterable)
        iterable, page = (itertools.islice(i1, page_size, None),
                list(itertools.islice(i2, page_size)))
        if len(page) == 0:
            break
        yield page


#twitter application credentials
ckey = ''
csecret = ''
akey = ''
asecret = ''
#authorization
auth = tweepy.OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
#wait for rate limits
api = tweepy.API(auth, wait_on_rate_limit=True,
                       wait_on_rate_limit_notify=True)

while(1):
 #find Followers for Tweet User
 #check if we've already explored this user's network (DB)
 users = graph.cypher.execute("MATCH (x:User)-[:POSTS]->(t) WHERE x.Exploration='' RETURN DISTINCT x.Screen_Name")
 for r in users:
    scrname=r[0]
    x=graph.merge_one("User","Screen_Name",scrname)
    print scrname

    try:
        #find Followers for Tweet User
        followers = api.followers_ids(screen_name=scrname)
        for page in paginate(followers, 100):
            results = api.lookup_users(user_ids=page)
            for result in results:
                #Only add relationships between users that already exist in the network because of their tweets (get_tweets.py, get_live_tweets.py)
                mynode = list(graph.find('User',property_key='Screen_Name',
                               property_value=result.screen_name))
                if len(mynode) > 0:
                    # use of merge_one in order to avoid duplicates
                    y=graph.merge_one("User","Screen_Name",result.screen_name.encode('utf8'))
                    y.properties.update({"Name": result.name, "Description": result.description.encode('utf8'),"Location":result.location
                                     ,"Followers": result.followers_count,"Friends": result.friends_count, "Tweets": result.statuses_count
                                     ,"Image":result.profile_image_url })
                    y.push()
                    follows=Relationship(y, "FOLLOWS", x)
                    graph.create_unique(follows)
    except Exception, e:
             #error handler
             print 'Exception occurred for followers'
             pass


    try:
        #find Friends for Tweet User
        friends = api.friends_ids(screen_name=scrname)
        for page in paginate(friends, 100):
            results = api.lookup_users(user_ids=page)
            for result in results:
                #Only add relationships between users that already exist in the network by their tweets
                mynode = list(graph.find('User',property_key='Screen_Name',
                               property_value=result.screen_name))
                if len(mynode) > 0:
                    y=graph.merge_one("User","Screen_Name",result.screen_name.encode('utf8'))
                    y.properties.update({"Name": result.name, "Description": result.description.encode('utf8'),"Location":result.location
                                     ,"Followers": result.followers_count,"Friends": result.friends_count, "Tweets": result.statuses_count
                                     ,"Image":result.profile_image_url  })
                    y.push()
                    follows=Relationship(x, "FOLLOWS", y)
                    graph.create_unique(follows)
    except Exception, e:
             #error handler
             print 'Exception occurred for friends'
             pass

    #Explored on specific date (in case we want to renew it on future
    x.properties.update({"Exploration":datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
    x.push()


   