from flask import Flask
from flask_restful import Resource, Api
from pymongo import MongoClient
import pprint
import os
import json
from bson import ObjectId


app = Flask(__name__)
api = Api(app)

uri = 'mongodb://' + os.environ['MONGODB_USERNAME'] + ':'\
                   + os.environ['MONGODB_PASSWORD'] + '@'\
                   + os.environ['MONGODB_HOSTNAME'] + ':27017/'
mongo = MongoClient(uri)
db = mongo.index


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)


class NameServer(Resource):
    def get(self, msg):  # msg argument is temporary
        return pprint.pformat([element for element in db.my_collection.find()])

    def put(self, msg):
        item = {
            "text": msg
        }
        db.my_collection.insert_one(item)
        return f"{item}"


api.add_resource(NameServer, "/<string:msg>")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
