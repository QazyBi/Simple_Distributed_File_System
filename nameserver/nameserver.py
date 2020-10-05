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

parser = reqparse.RequestParser()
parser.add_argument('command')
parser.add_argument('filename')
parser.add_argument('path')

parser_dir = reqparse.RequestParser()
parser_dir.add_argument('command')
parser_dir.add_argument('current directory')
parser_dir.add_argument('target directory')


def send_n_recv_message(ip, port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((ip, port))
        # send message to the socket
        s.send(message.encode())
        # receive message from server via socket
        answer_message = s.recv(BUFFER_SIZE).decode()
    return answer_message


class Initialize(Resource):
    def get(self):
        global AVAILABLE_SIZE
        AVAILABLE_SIZE = 0
        for storage in STORAGES:
            size = send_n_recv_message(storage, PORT, "initialize")
            AVAILABLE_SIZE += int(size)
        return "available size"


def select_storage_servers(storages, amount):
    storage_dict = {}
    for storage in storages:
        size = send_n_recv_message(storage, PORT, "memtest")
        storage_dict[storage] = int(size)

    sorted_dict = {k: v for k, v in sorted(storage_dict.items(),
                                           key=lambda item: item[1])}
    return [sorted_dict[i] for i in range(amount)]


class File(Resource):
    def post(self):
        args = parser.parse_args()
        command = args['command']
        path = args['path']
        filename = args['filename']

        if command == "create":
            selected_storages = select_storage_servers(STORAGES)
            for storage in selected_storages:
                send_n_recv_message(storage, PORT, "create")  # handle output
            return {"answer": "success"}

        elif command == "write":
            selected_storages = select_storage_servers(STORAGES)
            # selected_storages = [STORAGE_1, STORAGE_2]  # used for debugging
            item = {
                "path": path,
                "filename": filename,
                "storages": selected_storages
            }
            db.my_collection.insert_one(item)
        elif command == "read":
            pass
        elif command == "info":
            pass
        elif command == "copy":
            pass
        elif command == "move":
            pass
        else:
            return {"answer": "invalid command"}

    def delete(self):
        pass


class Directory(Resource):
    def post(self):
        args = parser.parse_args()
        command = args['command']
        path = args['current directory']
        filename = args['target directory']

        if command == "open":
            pass
        elif command == "read":
            pass
            # db.my_collection.find({"path":{ $regex: /^\/fol/, $options: 'm'}})
            # pprint.pformat([element for element in db.my_collection.find()])
        elif command == "make":
            pass
        else:
            return {"answer": "incorrect command"}

    def delete(self):
        return "deleted."


api.add_resource(Initialize, "/init")
api.add_resource(File, "/file")
api.add_resource(Directory, "/dir")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
