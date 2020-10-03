from flask import Flask
# import json
from flask_restful import Resource, Api
from pymongo import MongoClient


client = MongoClient(port=27017)
db = client.index

app = Flask(__name__)
api = Api(app)


class NameServer(Resource):
    def get(self, msg):  # msg argument is temporary
        print(f"{msg}")

    def put(self, msg):
        print(f"{msg}")


api.add_resource(NameServer, "/<string:msg>")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
