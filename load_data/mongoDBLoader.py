import pymongo
import ast
import json

PATH_TO_DATA = "../dataset/"

CHECKINS = "yelp_academic_dataset_checkin.json"
REVIEWS = "yelp_academic_dataset_review.json"
TIPS = "yelp_academic_dataset_tip.json"
USERS = "yelp_academic_dataset_user.json"
BUSINESSES = "yelp_academic_dataset_business.json"

listOfFiles = [BUSINESSES]

def getDb():
	return pymongo.MongoClient().yelp_challenge

def load_into_mongo():
	for filename in listOfFiles:
		db = getDb()
		with open(PATH_TO_DATA + filename) as filepath:
			for line in filepath:
				collection = 0
				if filename == REVIEWS:
					collection = db.reviews
				elif filename == CHECKINS:
					collection = db.checkins
				elif filename == TIPS:
					collection = db.tips
				elif filename == USERS:
					collection = db.users
				else:
					collection = db.businesses
				collection.insert_one(json.loads(line))

if __name__ == "__main__":
	load_into_mongo()


