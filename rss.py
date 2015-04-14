#!/usr/bin/python
# -*- coding:utf-8
#
import pytz
import pymongo
import feedparser
# import os
# import pprint
# import sys

feeds = ['http://www.ideal.es/granada/rss/atom/portada']

def createIndexes(db):
	db.releases.create_index([('UPC', ASCENDING), ('account', ASCENDING)])
	db.tracks.create_index([('ISRC', ASCENDING), ('title', ASCENDING), ('artist', ASCENDING), ('account', ASCENDING)])
	db.raw.create_index([('ISRC', ASCENDING), ('UPC', ASCENDING), ('post-processed', ASCENDING), ('account', ASCENDING)])

def getMongoDB():
	from pymongo import MongoClient
	client = MongoClient('localhost:27017')
	db = client.tartesus
	return db

#Map-reduce function
def reduceFeed(feed):
	result = {}
	result['origin'] = 'rss'
	result['content'] = feed['title']
	result['originalId'] = feed['id']
	result['updated'] = feed['updated']
	result['urls'] = [feed['link']]
	return result

def main():
	db = getMongoDB()
	for feed in feeds:
		data = feedparser.parse(feed)
		reducedData = map(reduceFeed, data['entries'])
		db.social.insert(reducedData)

if __name__ == "__main__":
	print 'Tartes.us rss wrapper'
	main()
