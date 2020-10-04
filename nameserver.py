from flask import Flask
from flask_restful import Resource, Api
from pymongo import MongoClient
import pprint
import os
import socket


AVAILABLE_SIZE = 0
BUFFER_SIZE = 1024
PORT = "8080"
STORAGE_1 = "10.0.15.13"
STORAGE_2 = "10.0.15.14"
STORAGE_3 = "10.0.15.15"
STORAGES = [STORAGE_1, STORAGE_2, STORAGE_3]

app = Flask(__name__)
api = Api(app)

uri = 'mongodb://' + os.environ['MONGODB_USERNAME'] + ':'\
                   + os.environ['MONGODB_PASSWORD'] + '@'\
                   + os.environ['MONGODB_HOSTNAME'] + ':27017/'
mongo = MongoClient(uri)
db = mongo.index


class Initialize(Resource):
    def get(self):
        global AVAILABLE_SIZE
        AVAILABLE_SIZE = 0
        for storage in STORAGES:
            # connect to server with socket
            # send to socket command
            # receive from socket output
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((storage, PORT))
                s.send('initialize'.encode())
                size = s.recv(BUFFER_SIZE).decode()
                AVAILABLE_SIZE += int(size)
        return "available size"


class File(Resource):
    def get(self, msg):
        return pprint.pformat([element for element in db.my_collection.find()])

    def put(self, msg):
        item = {
            "text": msg
        }
        db.my_collection.insert_one(item)
        return f"{item}"


class Directory(Resource):
    def get(self, msg):
        return "dir"

    def put(self, msg):
        return "dir"


api.add_resource(Initialize, "/init")
api.add_resource(File, "/file")
api.add_resource(Directory, "/dir")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
