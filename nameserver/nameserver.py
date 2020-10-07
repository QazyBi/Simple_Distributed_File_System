from flask import Flask
from flask_restful import Resource, Api, reqparse
from pymongo import MongoClient
import os
# import pprint
import socket
import datetime
from json import dumps


AVAILABLE_SIZE = 0
REPLICA_NUM = 2
BUFFER_SIZE = 1024
SEPARATOR = "<SEPARATOR>"

CURRENT_DIR = ""
ROOT_DIR = ""

PORT = 8080
STORAGE_1 = "10.0.15.13"
STORAGE_2 = "10.0.15.14"
STORAGE_3 = "10.0.15.15"
STORAGES = [STORAGE_1, STORAGE_2, STORAGE_3]
URI = 'mongodb://' + os.environ['MONGODB_USERNAME'] + ':'\
                   + os.environ['MONGODB_PASSWORD'] + '@'\
                   + os.environ['MONGODB_HOSTNAME'] + ':27017/'

mongo = MongoClient(URI)
db = mongo.index
app = Flask(__name__)
api = Api(app)

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
    answer_message = ""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ip, port))
            # send message to the socket
            s.sendall(message.encode())
            s.sendall("<DONE>".encode())
            # receive message from server via socket
            answer_message = s.recv(BUFFER_SIZE).decode()
    except:
        app.logger.info("connection from server refused")
    return answer_message


def select_storage_servers(storages, amount):
    storage_list = []
    for storage_index, storage in enumerate(storages):
        # handle cases when output contains an error
        server_response = send_n_recv_message(storage, PORT, "disk_size").split(SEPARATOR)
        size = server_response[0]
        if size.isdigit():
            storage_list.append((size, storage))
        else:
            return "-1"

    storage_list.sort(reverse=True)

    return [storage_list[i][1] for i in range(amount)]


def request_server(command, path, filename, new_directory):
    query = {
        "path": path,
        "filename": filename
    }
    r = db.my_collection.find_one(query)
    if r is not None:
        for storage in r['storages']:
            message = SEPARATOR.join([command, path + filename, new_directory])
            server_response = send_n_recv_message(storage, PORT, message)
    else:
        return "error" + SEPARATOR + "False"
    return server_response


def check_server_response(message):
    if message.split(SEPARATOR)[1] == "True":
        return True
    else:
        return False


def available_filename(filename, path):
    app.logger.info("in the function %s", filename)
    counter = 0
    query = {
        "filename": filename,
        "path": path
    }
    split = filename.split(".")
    basename = split[0]
    extension = ""
    if len(split) > 1:
        extension = split[1]
    new_filename = filename
    # CHECK CASE when file which same name exists
    while db.my_collection.find_one(query) is not None:
        new_filename = basename + f"({counter})" + "." + extension
        query = {
            "filename": new_filename,
            "path": path
        }
        counter += 1
    return new_filename


def format_dir(directory):
    if directory:
        return directory[:-1] if directory[-1] == "/" else directory
    else:
        return -1


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class Initialize(Resource):
    def get(self):
        """GET request to init distributed file system
        Route: nameserver:8080/init

        Returns:
        int: available disk size on file system
        """
        global AVAILABLE_SIZE
        global CURRENT_DIR
        AVAILABLE_SIZE = 0
        CURRENT_DIR = ""
        # delete all records inside collection
        db.my_collection.delete_many({})
        temp = {"path": CURRENT_DIR, "datetime": datetime.datetime.now()}
        db.my_collection.insert_one(temp)
        for storage in STORAGES:
            # send request to initialize to each storage server
            server_response = send_n_recv_message(storage, PORT, "initialize").split(SEPARATOR)
            if server_response[0].isdigit():
                AVAILABLE_SIZE += int(server_response[0])
            else:
                app.logger.info("Storage Server returned non numerical size %s", server_response[0])
                return {"status": "failed",
                        "response": "error in storage server"}

        return {"size": AVAILABLE_SIZE,
                "status": "success",
                "response": "sending available size of distributed file system"}


class File(Resource):
    def post(self):
        args = parser.parse_args()
        command = args['command']
        filename = args['filename']
        path = args['path']
        size = args['size']
        new_directory = args['new_directory']
        request_datetime = datetime.datetime.now()

        if command == "create":
            """POST request
            Route: nameserver:8080/file

            Args:
                filename
                path
                size

            Make request to storage servers
            """
            selected_storages = select_storage_servers(STORAGES, REPLICA_NUM)
            if selected_storages[0] == "-1":
                return {"response": "server error",
                        "status": "failed"}

            new_filename = available_filename(filename, CURRENT_DIR + path)
            item = {
                "path": CURRENT_DIR + path,
                "filename": new_filename,
                "storages": selected_storages,
                "datetime": request_datetime,
                "size": size
            }
            db.my_collection.insert_one(item)

            message = SEPARATOR.join(["create_file", CURRENT_DIR + path + "/" + new_filename, " ".join(selected_storages[1:])])
            response = send_n_recv_message(selected_storages[0], PORT, message)
            if check_server_response(response):
                return {"response": "created a file",
                        "status": "success"}
            else:
                return {"response": "server error",
                        "status": "failed"}
        elif command == "write":
            """POST request
            Route: nameserver:8080/file

            Args:
                filename
                path

            Returns:
                ips of storage servers
                ports of storage servers
                filename|new_filename(if such file exists)
            """
            try:
                new_filename = available_filename(filename, CURRENT_DIR + path)
                selected_storages = select_storage_servers(STORAGES, REPLICA_NUM)
                item = {
                    "path": CURRENT_DIR + path,
                    "filename": new_filename,
                    "storages": selected_storages,
                    "datetime": request_datetime,
                    "size": size
                }
                db.my_collection.insert_one(item)
                response = {
                    "storages": selected_storages,
                    "port": PORT,
                    "new_filename": new_filename,
                    "status": "success",
                    "response": "file will be stored in servers"
                }
            except:
                response = {
                    "response": "server error",
                    "status": "failed"
                }
            return response
        elif command == "read":
            """POST request

            Route: nameserver:8080/file

            Args:
                filename
                path

            Returns:
                ip
                port
            """
            query = {
                "path": CURRENT_DIR + path,
                "filename": filename
            }
            storages = []
            try:
                record = db.my_collection.find_one(query)
                if record is not None:
                    storages = record['storages']
                else:
                    return {"status": "failed", "response": "file not found"}
            except:
                return {"status": "failed",
                        "response": "server error"}
            return {"storages": storages,
                    "port": PORT,
                    "status": "success",
                    "response": "file found"}
        elif command == "info":
            """POST request

            Route: nameserver:8080/file

            Args:
                filename
                path

            Returns:
                ip
                size
                datetime
            """
            query = {
                "path": CURRENT_DIR + path,
                "filename": filename
            }
            response = {"response": "server error", "status": "failed"}
            for element in db.my_collection.find(query):
                try:
                    response = {
                        "storages": element['storages'],
                        "size": element['size'],
                        "datetime": dumps(element['datetime'], default=json_serial),
                        "status": "success",
                        "response": "found record with this file"
                    }
                except:
                    response = {
                        "response": "file not found",
                        "status": "failed"
                    }

            return response
        elif command == "copy":
            """POST request

            Route: nameserver:8080/file

            Args:
                filename
                path
                new path

            sends request to storage server copy command
            """
            if filename != "":
                server_response = request_server("copy_file",
                                                 CURRENT_DIR + path,
                                                 filename,
                                                 new_directory)  # ROOT_DIR
                if check_server_response(server_response):
                    response = {
                        "status": "success",
                        "response": "copied file"
                    }
                else:
                    response = {
                        "status": "failed",
                        "response": "no such directory"
                    }
            else:
                response = {
                    "status": "failed",
                    "response": "no filename provided"
                }
            return response
        elif command == "move":
            """POST request

            Route: nameserver:8080/file

            Args:
                filename
                path
                new path

            sends request to storage server with move command
            """
            server_response = request_server("move_file",
                                             CURRENT_DIR+path,
                                             new_directory)
            if check_server_response(server_response):
                response = {
                    "status": "success",
                    "response": "moved file"
                }
            else:
                response = {
                    "status": "failed",
                    "response": "no such directory"
                }
            return response
        elif command == "delete":
            """DELETE request

            Route: nameserver:8080/file

            Args:
                filename
                path

            sends request to storage server hosting that file
            """
            query = {
                "path": CURRENT_DIR + path,
                "filename": filename
            }
            try:
                r = db.my_collection.find_one(query)
                for storage in r['storages']:
                    message = SEPARATOR.join(["delete_file", filename])
                    server_response = send_n_recv_message(storage,
                                                          PORT,
                                                          message)
                db.my_collection.delete_one(query)
                if check_server_response(server_response):
                    response = {
                        "status": "success",
                        "response": "deleted file"
                    }
                else:
                    response = {
                        "status": "failed",
                        "response": "server error please try again"
                    }
            except:
                response = {
                    "status": "failed",
                    "response": "such file does not exist"
                }
            return response
        else:
            return {
                "response": "invalid command",
                "status": "failed"
            }


class Directory(Resource):
    def get(self):
        return {"response": "user's current directory",
                "status": "success",
                "current_directory": CURRENT_DIR}

    def post(self):
        global CURRENT_DIR
        args = parser_dir.parse_args()
        command = args['command']
        path = args['current_directory']
        target_directory = args['target_directory']

        if command == "open":
            """POST request

            Route: nameserver:8080/dir

            Args:
                filename
                path
                target directory

            nameserver updates client's directory tracker
            """
            if target_directory:
                if target_directory == "..":
                    if CURRENT_DIR == ROOT_DIR:
                        return {"response": "no parent directory found", "status": "failed"}
                    else:
                        CURRENT_DIR = "/".join(CURRENT_DIR.split("/")[:-1])
                        return {"response": "successfully set new directory", "status": "success"}
                else:
                    query = {"path": target_directory}
                    if db.my_collection.find_one(query) is not None:
                        CURRENT_DIR = format_dir(target_directory)
                        return {"response": "successfully set new directory", "status": "success"}
                    else:
                        return {"response": "no such directory", "status": "failed"}
            else:
                return {"response": "provide target directory", "status": "failed"}
        elif command == "read":
            """POST request

            Route: nameserver:8080/dir

            Args:
                target directory

            Returns:
                all files in current directory
            """
            app.logger.info("*%s* DIR", target_directory)
            if target_directory is None or target_directory == "":
                query = {
                    "path": {"$regex": f"^{CURRENT_DIR}$"}
                }
            else:
                query = {
                    "path": {"$regex": f"^{target_directory}$"}
                }
            files_dirs = []
            for elem in db.my_collection.find(query):
                try:
                    file = elem['filename']
                    files_dirs.append(file)
                except:
                    directory = elem['path']
                    files_dirs.append(directory)

                return {"files": list(set(files_dirs)),
                        "response": "found files",
                        "status": "success"}
        elif command == "make":
            """POST request

            Route: nameserver:8080/dir

            Args:
                target directory

            creates directory record
            """
            try:
                query = {"path": CURRENT_DIR + target_directory}
                if db.my_collection.find_one(query) is not None:
                    return {"status": "failed", "response": "such directory exists"}
                item = {
                    "path": CURRENT_DIR + target_directory,
                    "datetime": datetime.datetime.now(),
                }
                # if item["target_directory"] == -1:
                    # return {"status": "failed",
                            # "response": "server error"}

                db.my_collection.insert_one(item)
                return {"status": "success",
                        "response": "created directory"}
            except:
                return {"status": "failed",
                        "response": "server error"}
        elif command == "delete":
            """POST request

            Route: nameserver:8080/dir

            Args:
                directory to delete

            checks whether that dir has files if not deletes that directory
            """
            query = {
                "path": {"$regex": f"^{CURRENT_DIR + target_directory}"}
            }

            new_path = path if path[0] == "/" else "/" + path

            for storage in STORAGES:
                message = SEPARATOR.join(["delete_dir", CURRENT_DIR + new_path])
                server_response = send_n_recv_message(storage,
                                                      PORT,
                                                      message)

                if server_response.split(SEPARATOR)[1] == "False":
                    return {"response": "no permission", "status": "failed"}

            db.my_collection.delete_many(query)
            return {"response": "successfully deleted", "status": "success"}

            # if len(list(db.my_collection.find(query))) == 1:
            #     db.my_collection.delete_one(query)
            #     return {"response": "deleted folder", "status": "success"}
            # else:
            #     if path == "yes":
            #         # TODO: receive user permission
            #         for element in db.my_collection.find(query):
            #             for storage in element['storages']:
            #                 message = SEPARATOR.join(["delete_dir", path])
            #                 server_response = send_n_recv_message(storage,
            #                                                       PORT,
            #                                                       message)
            #         db.my_collection.delete_many(query)
            #         if check_server_response(server_response):
            #             return {"response": "deleted folder", "status": "success"}
            #         else:
            #             return {"response": "no such directory", "status": "failed"}
            #     else:
            #         return {"response": "no permission", "status": "failed"}
        else:
            return {"response": "incorrect command", "status": "failed"}


api.add_resource(Initialize, "/init")
api.add_resource(File, "/file")
api.add_resource(Directory, "/dir")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
