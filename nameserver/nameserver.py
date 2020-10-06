from flask import Flask
from flask_restful import Resource, Api, reqparse
from pymongo import MongoClient
import pprint
import os
import socket
import datetime


AVAILABLE_SIZE = 0
REPLICA_NUM = 2
BUFFER_SIZE = 1024
SEPARATOR = "<SEPARATOR>"

CURRENT_DIR = "/var/storage/"

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
parser.add_argument('size')
parser.add_argument('new_directory')

parser_dir = reqparse.RequestParser()
parser_dir.add_argument('command')
parser_dir.add_argument('current_directory')
parser_dir.add_argument('target_directory')


def send_n_recv_message(ip, port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((ip, port))
        # send message to the socket
        s.send(message.encode())
        # receive message from server via socket
        answer_message = s.recv(BUFFER_SIZE).decode()
    return answer_message


def select_storage_servers(storages, amount):
    storage_dict = {}
    for storage in storages:
        size = send_n_recv_message(storage, PORT, "disk_size")
        storage_dict[storage] = int(size)

    sorted_dict = {k: v for k, v in sorted(storage_dict.items(),
                                           key=lambda item: item[1])}
    return [sorted_dict[i] for i in range(amount)]


def request_server(command, path, filename, new_directory):
    query = {
        "path": path,
        "filename": filename
    }
    for element in db.my_collection.find(query):
        for storage in element['storages']:
            message = SEPARATOR.join([command, filename, new_directory])
            server_response = send_n_recv_message(storage, PORT, message)
    return server_response


class Initialize(Resource):
    def get(self):
        global AVAILABLE_SIZE
        AVAILABLE_SIZE = 0
        for storage in STORAGES:
            size = send_n_recv_message(storage, PORT, "initialize")
            AVAILABLE_SIZE += int(size)
        return "available size"


class File(Resource):
    def post(self):
        args = parser.parse_args()
        command = args['command']
        filename = args['filename']
        path = args['path']
        size = args['size']
        new_directory = args['new_directory']

        if command == "create":
            selected_storages = select_storage_servers(STORAGES, REPLICA_NUM)
            for storage in selected_storages:
                send_n_recv_message(storage, PORT, "create")  # handle output
            return {"response": "success"}
        elif command == "write":
            selected_storages = select_storage_servers(STORAGES, REPLICA_NUM)
            # selected_storages = [STORAGE_1, STORAGE_2]  # used for debugging
            request_datetime = datetime.datetime.now()
            item = {
                "path": path,
                "filename": filename,
                "storages": selected_storages,
                "datetime": request_datetime,
                "size": size
            }
            db.my_collection.insert_one(item)
            response = {
                "ip": selected_storages,
                "port": PORT,
                "new_filename": filename
            }
            return response
        elif command == "read":
            query = {
                "path": path,
                "filename": filename
            }
            storages = []
            for element in db.my_collection.find(query):
                try:
                    storages.append(element['storages'])
                except Exception:
                    pass

            return {"ip": storages, "port": PORT}
        elif command == "info":
            query = {
                "path": path,
                "filename": filename
            }
            for element in db.my_collection.find(query):
                try:
                    response = {
                        "ip": element['storages'],
                        "size": element['size'],
                        "datetime": element['datetime']
                    }
                except Exception:
                    response = {
                        "response": "file not found"
                    }

            return response
        elif command == "copy":
            server_response = request_server("copy_file",
                                             path, filename,
                                             new_directory)
            return server_response
        elif command == "move":
            server_response = request_server("move_file",
                                             path,
                                             filename,
                                             new_directory)
            return server_response
        elif command == "delete":
            query = {
                "path": path,
                "filename": filename
            }
            for element in db.my_collection.find(query):
                for storage in element['storages']:
                    message = SEPARATOR.join(["delete_file", filename])
                    server_response = send_n_recv_message(storage,
                                                          PORT,
                                                          message)
            db.my_collection.delete_one(query)
            return server_response
        else:
            return {"answer": "invalid command"}


def format_dir(directory):
    return directory if directory[-1] == "/" else directory + "/"


class Directory(Resource):
    def post(self):
        global CURRENT_DIR
        args = parser_dir.parse_args()
        command = args['command']
        path = args['current_directory']
        target_directory = args['target_directory']

        if command == "open":
            CURRENT_DIR = format_dir(target_directory)
            return {"response": "done"}
        elif command == "read":
            if target_directory == "null":
                query = {
                    "path": {"$regex": f"^{format_dir(CURRENT_DIR)}$"}
                }
            else:
                query = {
                    "path": {"$regex": f"^{format_dir(target_directory)}$"},
                }
            return pprint.pformat([element for element in db.my_collection.find(query)])
        elif command == "make":
            item = {
                "path": format_dir(path),
                "datetime": datetime.datetime.now(),
            }
            db.my_collection.insert_one(item)
        elif command == "delete":
            query = {
                "path": {"$regex": f"^{format_dir(target_directory)}"}
            }
            for element in db.my_collection.find(query):
                for storage in element['storages']:
                    message = SEPARATOR.join(["delete_dir", path])
                    server_response = send_n_recv_message(storage,
                                                          PORT,
                                                          message)
            db.my_collection.delete_one(query)
            return server_response
        else:
            return {"answer": "incorrect command"}


api.add_resource(Initialize, "/init")
api.add_resource(File, "/file")
api.add_resource(Directory, "/dir")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
