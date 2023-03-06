import pymongo
from pymongo import MongoClient
client=MongoClient("mongodb://hermannjo:24Fe1988@blxmdp.j13e4n0.mongodb.net/?retryWrites=true&w=majority")

db=client.get_database('blxmdp')
db
