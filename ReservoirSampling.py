import sys
import json
from time import time
import math
import random
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener



cons_key = 'HTOhfvsh65MsQY0TCFICRNpEj'
cons_Sec_Key = '8AXBEpGogYiMBbcLtamrvtoQA7fxxgF3IpqilcXJT7z1EQm9cD'
aces_token = '1213856383126265856-P48KKzGX6soIaJkCIsRSnpsFp8HufJ'
aces_sec_Key = 'Yq3rFjJmiY0FIt3l30ZhIlk9nPXgr6aKLDmpIMhPWDZGs'

class Stream_listener(StreamListener):
    def on_error(self, status):
        print("Error: ", str(status))
        
    
    def on_status(self, status):
        global numTweets
        global allTweets
        global NUMTWEETS 
        
        Hash_tags=status.entities['hashtags']
        tag_dict={}
        if len(Hash_tags)>0:
            numTweets += 1
 
            if numTweets < NUMTWEETS:
                allTweets.append(status)
                hashtags=status.entities.get("hashtags")
                for hashtag in hashtags:
                    content=hashtag['text']
                    if content in tag_dict:
                        tag_dict[content]=tag_dict[content]+1
                    else:
                        tag_dict[content]=1
                        
                sortedTweets = sorted(tag_dict.items(), key=lambda x: (-x[1], x[0]))
                M=[]
                for items in sortedTweets:
                    M.append(items[1])
                set_M=list(set(M))
                M=sorted(set_M,key=lambda x:-x)
                n=M[:3]
                print(n)
                final_list=[]
                for items in sortedTweets:
                    if items[1] in n:
                        final_list.append(items)
                    
                out="The number of tweets with tags from the beginning: " + str(numTweets)+"\n"
                for tweets in final_list:
                    out=out+(tweets[0]+" : "+str(tweets[1])+"\n")
                outputFile.write(out)
                outputFile.write('\n')
                outputFile.flush()
                print(out)
                
                        
                        
            else:
                randomIdx = random.randint(0, numTweets)

                if randomIdx < NUMTWEETS-1:
                    allTweets[randomIdx]=status

                for tweet in allTweets:
                    hashtags = tweet.entities.get("hashtags")

                    for hashtag in hashtags:
                        content = hashtag["text"]
                        
                        if content in tag_dict.keys():
                            tag_dict[content]=tag_dict[content]+1
                        else:
                            tag_dict[content]=1

                sortedTweets = sorted(tag_dict.items(), key=lambda x: (-x[1], x[0]))
                M=[]
                for items in sortedTweets:
                    M.append(items[1])
                set_M=list(set(M))
                M=sorted(set_M,key=lambda x:-x)
                n=M[:3]
                print(n)
                final_list=[]
                for items in sortedTweets:
                    if items[1] in n:
                        final_list.append(items)
                        
                

                out="The number of tweets with tags from the beginning: " + str(numTweets)+"\n"
                for tweets in final_list:
                    out += (tweets[0]+" : "+str(tweets[1])+"\n")
                outputFile.write(out)
                outputFile.write('\n')
                outputFile.flush()
                print(out)



'''
MAIN
'''
if __name__ == "__main__":
    
    NUMTWEETS = 100
    output_file_path=sys.argv[2]
    auth = tweepy.OAuthHandler(consumer_key=cons_key, consumer_secret=cons_Sec_Key)
    auth.set_access_token(key=aces_token, secret=aces_sec_Key)
    tweetAPI = tweepy.API(auth)
    
    numTweets = 0
    allTweets = []
    outputFile = open(output_file_path, "w")
    tweetStream = Stream(auth=auth, listener=Stream_listener())

    tweetStream.filter(track=["Trump"])
