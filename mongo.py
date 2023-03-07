# %%
import pymongo
import time
from bson.objectid import ObjectId

class MongoService():
    def __init__(self, connection="mongodb://root:Asdf1234@144.126.156.183:8095/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false", 
                db_name='metrogas',
                collection='clientsPipeline'):
        self.client = pymongo.MongoClient(connection)
        self.db = self.client[db_name]
        self.collection = collection
    
    def get_doc(self, key):
        # return self.db[collection].find_one({'_id': ObjectId(id)})
        return self.db[self.collection].find_one({key: {'$exists': True}})
    
    def update_status(self, id, status):
        self.db[self.collection].update_one({'_id': ObjectId(id)}, {'$set': {'status': status}})
    
    def update_array_needs(self, doc, key, value):
        self.db[self.collection].update_one({'_id': ObjectId(doc['_id'])}, {'$set': {key: value}})
