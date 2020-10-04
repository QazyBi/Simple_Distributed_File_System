from flask import Flask
from flask_restful import Resource, Api, reqparse
from pymongo import MongoClient
# import pprint
import os
import socket


AVAILABLE_SIZE = 0
BUFFER_SIZE = 1024
PORT = "8080"
STORAGE_1 = "10.0.15.13"
STORAGE_2 = "10.0.15.14"
STORAGE_3 = "10.0.15.15"
STORAGES = [STORAGE_1, STORAGE_2, STORAGE_3]
URI = 'mongodb://' + os.environ['MONGODB_USERNAME'] + ':'\
                   + os.environ['MONGODB_PASSWORD'] + '@'\
                   + os.environ['MONGODB_HOSTNAME'] + ':27017/'

app = Flask(__name__)
api = Api(app)
mongo = MongoClient(URI)
db = mongo.index
# pprint.pformat([element for element in db.my_collection.find()])
# db.my_collection.insert_one(item)


class Initialize(Resource):
    def get(self):
        global AVAILABLE_SIZE
        AVAILABLE_SIZE = 0
        for storage in STORAGES:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # connect to server with socket
                s.connect((storage, PORT))
                # send command to the socket
                s.send('initialize'.encode())
                # receive from socket output
                size = s.recv(BUFFER_SIZE).decode()
                AVAILABLE_SIZE += int(size)
        return "available size"


parser = reqparse.RequestParser()
parser.add_argument('filename')
parser.add_argument('path')


class File(Resource):
    def put(self):
        args = parser.parse_args()
        command = args['command']
        path = args['path']
        filename = args['filename']

        if command == "create":
            pass
        elif command == "read":
            pass
        elif command == "info":
            pass
        elif command == "write":
            pass
        elif command == "copy":
            pass
        elif command == "move":
            pass

    def delete(self):
        pass


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
