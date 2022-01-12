# For sending GET requests from the API
import random
import math
import requests
# For saving access tokens and for file management when creating and adding to the dataset
import os
# For dealing with json responses we receive from the API
import json
# For displaying the data after
import pandas as pd
# For saving the response data in CSV format
import csv
# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata
#To add wait time between requests
import time


#(Temporary) Add bearer token
os.environ['TOKEN'] = 'AAAAAAAAAAAAAAAAAAAAAIJKWQEAAAAA%2FLVYs34tr2%2FYx9bIR%2FgIx5zrsro%3DpqtOGtiX9PLzZBbiUG7mpDrdQtqKTV7LqnWKlCaODMXQprDCGR'


def auth():
    return os.getenv('TOKEN')


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_url(keyword, start_date, end_date, max_results = 10):
    search_url = "https://api.twitter.com/2/tweets/search/recent"

    # change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'tweet.fields':'created_at,id,text,public_metrics',
                    #'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    #'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    #'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    #'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)


def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def main():
    # Inputs for the request
    bearer_token = auth()
    headers = create_headers(bearer_token)
    keyword = "(COVID OR \"COVID-19\" OR vaccine OR vaccination OR Pfizer OR BioNTech OR Moderna OR \"Johnson & Johnson\" OR \"Johnson and Johnson\" OR Janssen OR J&J) lang:en -is:reply -is:retweet -is:links"
    max_results = 10

    count = 0
    for amount in [(330,-5),(330,-4),(340,-3)]:
        count += 10
        for i in range(0,int(amount[0]/max_results)):
            s = math.floor(random.random() * 60)
            m = math.floor(random.random() * 60)
            h = math.floor(random.random() * 23)
            d = 8 + amount[1]
            end_time = "2021-12-{}T{}:{}:{}Z".format(d,h+1,m,s)
            start_time = "2021-12-{}T{}:{}:{}Z".format(d,h,m,s)

            #TODO check for duplicate time

            print("{} - {}".format(start_time,end_time))

            url = create_url(keyword, start_time, end_time, max_results)
            json_response = connect_to_endpoint(url[0], headers, url[1])

            json_dict = json_response["data"]
            for tweet in json_dict:
                with open("data/tweets_linkless.json","a") as f:
                    json.dump(tweet, f, sort_keys=True)
                    f.write("\n")
        #if count == 100:
        #    time.sleep(60*15)
        #    count = 0


if __name__ == "__main__":
    main()



