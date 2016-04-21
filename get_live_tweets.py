#!/usr/bin/env python
# -*- coding: utf-8 -*-

#Using api.get_user,api.get_status TWITTER APIs
import tweepy
from tweepy.streaming import StreamListener
from py2neo import Graph,Relationship,Node
import sys
from httplib import IncompleteRead

#open Neo4j graph database to store the data
graph = Graph()

#twitter application credentials
ckey = ''
csecret = ''
akey = ''
asecret = ''
#authorization
auth = tweepy.OAuthHandler(ckey, csecret)
auth.set_access_token(akey, asecret)
#wait on rate limits
api = tweepy.API(auth, wait_on_rate_limit=True,
                       wait_on_rate_limit_notify=True)


class listener(tweepy.StreamListener):
    def on_status(self, status):
              tweet=status
              #creating (unique) user and tweet along with attributes
              #Connection between user and tweet
              mynode = list(graph.find('User',property_key='Screen_Name',
                                       property_value=tweet.user.screen_name.encode('utf8')))
              x=graph.merge_one("User","Screen_Name",tweet.user.screen_name.encode('utf8'))
              x.properties.update({"Name": tweet.user.name, "Description":tweet.user.description,"Location": tweet.user.location
                                   ,"Followers": tweet.user.followers_count,"Friends": tweet.user.friends_count,"Tweets": tweet.user.statuses_count
                                   ,"Image":tweet.user.profile_image_url})
              if tweet.user.description:
                  x.properties.update({"Description":tweet.user.description.encode('utf8')})

              if len(mynode)==0:
                x.properties.update({"Exploration":''})
              x.push()

              t=graph.merge_one("Tweet","ID",tweet.id)

              if hasattr(tweet, 'retweeted_status'):
                rtcount=0
              else:
                rtcount=tweet.retweet_count

              t.properties.update({"Date": tweet.created_at.strftime('%Y-%m-%d %H:%M:%S'),"Text": tweet.text.encode('utf8')
                                  ,"Favourites": tweet.favorite_count,"Retweets":rtcount})
              t.push()
              posts=Relationship(x, "POSTS", t)
              graph.create_unique(posts)

              # Find Tweet MENTIONS
              for m in (tweet.entities.get('user_mentions')):
                      try:

                        muser = api.get_user(m['screen_name'])
                        y=graph.merge_one("User","Screen_Name",m['screen_name'].encode('utf8'))
                        y.properties.update({"Name": muser.name, "Description": muser.description.encode('utf8'),"Location": muser.location
                                            ,"Followers": muser.followers_count,"Friends": muser.friends_count, "Tweets": muser.statuses_count
                                            ,"Image":muser.profile_image_url})

                        y.push()

                        mentions=Relationship(t, "MENTIONS", y)
                        graph.create_unique(mentions)
                      except Exception, e:
                        #error handler
                        print 'Exception Mention'
                        pass

              #Find Tweet HASHTAGS

              for h in (tweet.entities.get('hashtags')) :
                      y=graph.merge_one("Hashtag","Word",h['text'].encode('utf8'))
                      uses_hashtag=Relationship(t, "TAGS", y)
                      graph.create_unique(uses_hashtag)

              #Find Tweet URLs

              for u in (tweet.entities.get('urls')) :
                      y=graph.merge_one("Link","Url",u['expanded_url'])
                      uses_hashtag=Relationship(t, "CONTAINS", y)
                      graph.create_unique(uses_hashtag)


              # Find RETWEET source

              if tweet.retweet_count>0 :
                  #hasattr solves problems with users having null retweeted_status
                  if hasattr(tweet, 'retweeted_status'):

                      try:
                        # initu is the initial user node which is found on initusr based on retweeted_status
                        initusr = api.get_user(id=tweet.retweeted_status.user.screen_name)

                        initu=graph.merge_one("User","Screen_Name",initusr.screen_name)
                        initu.properties.update({"Name": initusr.name, "Description": initusr.description.encode('utf8'),"Location": initusr.location
                                                ,"Followers": initusr.followers_count,"Friends": initusr.friends_count, "Tweets": initusr.statuses_count
                                                ,"Image":initusr.profile_image_url})
                        initu.push()

                        # initt is the initial tweet
                        initt=graph.merge_one("Tweet","ID",tweet.retweeted_status.id)

                        initt.properties.update({"Date": tweet.retweeted_status.created_at.strftime('%Y-%m-%d %H:%M:%S'), "Text": tweet.retweeted_status.text.encode('utf8'),
                                               "Favourites": tweet.retweeted_status.favorite_count, "Retweets":tweet.retweet_count})

                        initt.push()

                        # RETWEET relationship between tweets
                        retweetof=Relationship(t, "RETWEET OF", initt)
                        graph.create_unique(retweetof)

                        # POST relationship between user and tweet
                        posts=Relationship(initu, "POSTS", initt)
                        graph.create_unique(posts)
                      except Exception, e:
                        #error handler
                        print 'Exception Retweet'
                        pass

              # find REPLY source
              # Check if we have a Reply
              if tweet.in_reply_to_status_id!=None:
                  # Get Tweet attributes on trpl based on TWEET ID
                  try:
                    trpl = api.get_status(id=tweet.in_reply_to_status_id)

                    # rpl is the Tweet node in which we Reply to
                    rpl = graph.merge_one('Tweet', 'ID',trpl.id)

                    if hasattr(trpl, 'retweeted_status'):
                        rtcount=0
                    else:
                        rtcount=trpl.retweet_count

                    rpl.properties.update({"Date": trpl.created_at.strftime('%Y-%m-%d %H:%M:%S'),"Text": trpl.text.encode('utf8')
                                          ,"Favourites": trpl.favorite_count,"Retweets":rtcount})
                    rpl.push()


                    replyto=Relationship(t, "REPLY TO", rpl)
                    graph.create_unique(replyto)

                    # y is the User who Posts the tweet in which we Reply to
                    ry=graph.merge_one("User","Screen_Name",trpl.user.screen_name.encode('utf8'))
                    ry.properties.update({"Name": trpl.user.name, "Description": trpl.user.description.encode('utf8'),"Location": trpl.user.location
                                         ,"Followers": trpl.user.followers_count,"Friends": trpl.user.friends_count, "Tweets": trpl.user.statuses_count
                                         ,"Image":trpl.user.profile_image_url})

                    ry.push()
                    posts=Relationship(ry, "POSTS", rpl)
                    graph.create_unique(posts)

                  except Exception, e:
                     #error handler
                     print 'Exception Reply'
                     pass
                  
              return True

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

        
        
#Main part
#Geo-Location coordinates for Athens and Thessaloniki cities in case we want to make more geo-specific search
#Athens = [21.58,36.46,24.97,39.3]
#Thessaloniki=[23.1495,40.0054,24.3788,40.9289]

while(1):
    try:
        # Connect/reconnect the stream
        twitterStream = tweepy.streaming.Stream(auth, listener())
        #twitterStream.filter(locations=Athens)
        twitterStream.filter(track=["#Oscars"])
    except IncompleteRead, e:
        pass
    except KeyboardInterrupt:
        # Or however you want to exit this loop
        twitterStream.disconnect()
        pass
    except:
        pass
