#!/usr/bin/python
# -*- coding:utf-8
#
import pytz
import pymongo
from TwitterAPI import TwitterAPI #https://github.com/geduldig/TwitterAPI
from TwitterAPI import TwitterRestPager

searches = ['andalucia','andalucismo']
accounts = ['IsidoroRopero','kormenika']

def createIndexes(db):
	db.releases.create_index([('UPC', ASCENDING), ('account', ASCENDING)])
	db.tracks.create_index([('ISRC', ASCENDING), ('title', ASCENDING), ('artist', ASCENDING), ('account', ASCENDING)])
	db.raw.create_index([('ISRC', ASCENDING), ('UPC', ASCENDING), ('post-processed', ASCENDING), ('account', ASCENDING)])

def getMongoDB():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client.andalucia
    return db

def auth():
    api = TwitterAPI('eBlhSxCEO4RHd4H7l0gMZc4Uj', '25HYJuPaBpE6SexjufpPdwvx2GkwMoQlqdvlEpZzp4n3I2uFdK', '223941017-GxaWpCkriCATD6rTb24B0aovhVFU9JuboyQYPb2J', 'NuxVUJr4u7nUU5przGKbneQ6bT70MbbhSBIhymsmdolN9')
    return api

#Store on MongoDB
def storeData(data, requestType, requestKey):
    print requestType
    print requestKey

    for d in data:
        d['type'] = requestType
        d['key'] = requestKey

    db = getMongoDB()
    db.twitter.insert(data)

#Map-reduce function
def reduceTweet(tweet):
    red = {}
    user = {}

    u = tweet['user']

    red['datetime'] = tweet['created_at']
    red['originalId'] = tweet['id_str']
    red['content'] = tweet['text']
    red['hashtag'] = []
    red['media'] = []
    red['urls'] = []
    red['mentionedUsers'] = []
    red['origin'] = 'twitter'

    user['id'] = u['id_str']
    user['name'] = u['name']
    user['username'] = u['screen_name']
    user['avatar'] = u['profile_image_url']
    red['user'] = user

    if 'hashtags' in tweet['entities']:
        for hashtag in tweet['entities']['hashtags']:
            red['hashtag'].append(hashtag['text'])

    if 'media' in tweet['entities']:
        for media in tweet['entities']['media']:
            red['media'].append(media['url'])

    if 'urls' in tweet['entities']:
        for url in tweet['entities']['urls']:
            red['urls'].append(url['expanded_url'])

    if 'user_mentions' in tweet['entities']:
        for u in tweet['entities']['user_mentions']:
            u2 = {}
            u2['username'] = u['screen_name']
            u2['name'] = u['name']
            u2['id'] = u['id_str']
            red['mentionedUsers'].append(u2)

    return red



def userTimeline(user, api):
    pager = TwitterRestPager(api, 'statuses/user_timeline', {'count':200, 'screen_name': user})
    for item in pager.get_iterator(wait=60):
        if 'text' in item:
            storeData(item)
            print item['text']


def main():
    api = auth()

    for user in accounts:
        userTimeline(user, api)

    for s in searches:
        req = api.request('search/tweets', {'q':s})
        reducedData = map(reduceTweet, req)
        storeData(reducedData, 'search', s)

if __name__ == "__main__":
    print 'Tartes.us twitter wrapper'
    main()
