import requests
import os
import socket

BUFFER_SIZE = 1024
SEPARATOR = "<SEPARATOR>"

namenode_IP = "10.0.15.12"
port = 8080
url = "http://" + namenode_IP + ":" + str(port)


def initialize():
    try:
        r = requests.get(url + "/init")
        try:
            j = r.json()
            
            if j['status'] == 'success':
                print(j['response'])
                print("Available size {} MB".format(j['size']))
            elif j['status'] == 'failed':
                print(j['response'])
                
        except:
            print("can't read json")
    except:
        print('no connection')


def create_file(file):
    path, filename = os.path.split(file)
    try:
        r = requests.post(url + "/file", params={'command': 'create', 'filename': filename, 'path': path, 'size': 0})
        try:
            j = r.json()
            print(j['response'])
        except:
            print("can't read json")
    except:
        print('no connection')


def read_file(file):
    path, filename = os.path.split(file)
    try:
        r = requests.post(url + "/file", params={'command': 'read', 'filename': filename, 'path': path})
        
        try:
            j = r.json()
            
            if j['status'] == 'success':
                print(j['response'])
                ip_list = j['storages']
                port = j['port']
                ip = ip_list[0]

                try:
                    s = socket.socket()
                    s.connect((ip, port))
                    f = open(filename, "w")
                    outcoming_stream = 'read_file' + SEPARATOR + path + '/' + filename
                    s.sendall(outcoming_stream.encode())
                    s.sendall("<DONE>".encode())
                    incoming_stream = ''
                    data = s.recv(BUFFER_SIZE)
                    while data:
                        incoming_stream += data.encode()
                        data = s.recv(BUFFER_SIZE)
                    parameters = incoming_stream.split(SEPARATOR)
                    data = parameters[0]
                    flag = parameters[1]
                    if flag == 'True':
                        f.write(data)
                    else:
                        print(data)
                    f.close()
                    s.close()
                except:
                    print('no connection with storage')
                    
            elif j['status'] == 'failed':
                print(j['response'])
            
        except:
            print("can't read json")
            
    except:
        print('no connection')

        
def write_file(file):
    path, filename = os.path.split(file)
    size = os.stat(file).st_size
    try:
        r = requests.post(url + "/file", params={'command': 'write', 'filename': filename, 'path': path, 'size': size})
        
        try:
            j = r.json()
            
            if j['status'] == 'success':
                print(j['response'])
                ip_list = j['storages']
                port = j['port']
                new_filename = j['new_filename']

                try:
                    s = socket.socket()
                    s.connect((ip_list[0], port))
                    data = ''
                    with open(file, "rb") as file:
                        for line in file:
                            data += line.decode()                    
                    stream = 'write_file' + SEPARATOR + filename + SEPARATOR + " ".join(ip_list[1:]) + SEPARATOR + data
                    s.sendall(stream.encode())
                    s.sendall('<DONE>'.encode())
                    s.close()
                except:
                    print('no connection with storage')
                    
            elif j['status'] == 'failed':
                print(j['response'])
            
        except:
            print("can't read json")
            
    except:
        print('no connection')


def delete_file(file):
    path, filename = os.path.split(file)
    try:
        r = requests.post(url + "/file", params={'command': 'delete', 'filename': filename, 'path': path})
        
        try:
            j = r.json()
            print(j['response'])
        except:
            print("can't read json")
        
    except:
        print('no connection')


def file_info(file):
    path, filename = os.path.split(file)
    try:
        r = requests.post(url + "/file", params={'command': 'info', 'filename': filename, 'path': path})
        
        try:
            j = r.json()
            
            if j['status'] == 'success':
                print(j['response'])
                size = j['size']
                storages = j['storages']
                datetime = j['datetime']
                print("File size {}".format(size))
                print("Time {}".format(datetime))
                print("Storages ")
                print(storages)
            elif j['status'] == 'failed':
                print(j['response'])
                
        except:
            print("can't read json")
        
    except:
        print('no connection')


def copy_file(file, new_dir):
    path, filename = os.path.split(file)
    size = os.stat(file).st_size
    try:
        r = requests.post(url + "/file", params={'command': 'copy', 'filename': filename, 'path': path, 'new_directory': new_dir})
        
        try:
            j = r.json()
            
            if j['status'] == 'success':
                print(j['response'])
            elif j['status'] == 'failed':
                print(j['response'])
            
        except:
            print("can't read json")
        
    except:
        print('no connection')


def move_file(file, new_dir):
    path, filename = os.path.split(file)
    try:
        r = requests.post(url + "/file", params={'command': 'move', 'filename': filename, 'path': path, 'new_directory': new_dir})
        
        try:
            j = r.json()
            
            if j['status'] == 'success':
                print(j['response'])
            elif j['status'] == 'failed':
                print(j['response'])
                
                if j['response'] == 'no such directory':
                    make_dir(new_dir)
                    try:
                        r = requests.post(url + "/file", params={'command': 'move', 'filename': filename, 'path': path, 'new_directory': new_dir})
                        j = r.json()
                        print(j['response'])
                    except:
                        print('no connection')
            
        except:
            print("can't read json")
        
    except:
        print('no connection')


def open_dir(target_directory):
    try:
        r = requests.post(url + "/dir", params={'command': 'open', 'target_directory': target_directory})
        
        try:
            j = r.json()
            print(j['response'])
        except:
            print("can't read json")
        
    except:
        print('no connection')
        

def read_dir(target_directory):
    if target_directory == "null":
        
        try:
            r = requests.post(url + "/dir", params={'command': 'read'})
            
            try:
                j = r.json()
                print(j['files'])
            except:
                print("can't read json")
            
        except:
            print('no connection')
    
    else:
         
        try:
            r = requests.post(url + "/dir", params={'command': 'read', 'target_directory': target_directory})
            
            try:
                j = r.json()
                print(j['files'])
            except:
                print("can't read json")
            
        except:
            print('no connection')


def make_dir(target_directory):
    try:
        r = requests.post(url + "/dir", params={'command': 'make', 'target_directory': target_directory})
        
        try:
            j = r.json()
            print(j['response'])
        except:
            print("can't read json")
        
    except:
        print('no connection')


def delete_dir(target_directory):
    try:
        r = requests.post(url + "/dir", params={'command': 'delete', 'target_directory': target_directory})
        
        try:
            j = r.json()
            
            if j['response'] == 'no permission':
                print(j['response'])
                permission = input("Do you want to delete directory with files? yes/no\n")
                print(permission)
                
                if permission == 'yes':
                    
                    try:
                        r = requests.post(url + "/dir", params={'command': 'delete', 'target_directory': target_directory, 'current_directory': permission})

                        try:
                            j = r.json()
                            print(j['response'])
                        except:
                            print("can't read json2")

                    except:
                        print('no connection')
                
            else:
                print(j['response'])
            
        except:
            print("can't read json1")
        
    except:
        print('no connection')


def test(arg):
    print(arg+arg)
    

def get_current_directory():
    try:
        r = requests.get(url + "/dir", params={'command': 'get'})
        try:
            j = r.json()
            data = {'current_directory': j['current_directory'], 'status': 'success', 'response': ''}
        except:
            data = {'current_directory': '', 'status': 'failed', 'response': "can't read json"}
    except:
        data = {'current_directory': '', 'status': 'failed', 'response': 'no connection'}
        
    return data
    

while True:
    data = get_current_directory()
    if data['status'] == 'success':
        string = input(data['current_directory'] + "$ ")
    elif data['status'] == 'failed':
        print(data['response'])
        string = input("$ ")
    arguments = string.split(" ")
    command = arguments[0]
    try:
        if command == "init":
            initialize()
        elif command == "touch":
            create_file(arguments[1])
        elif command == "download":
            read_file(arguments[1])
        elif command == "upload":
            write_file(arguments[1])
        elif command == "rm" and arguments[1] != "-r":
            delete_file(arguments[1])
        elif command == "info":
            file_info(arguments[1])
        elif command == "cp":
            copy_file(arguments[1], arguments[2])
        elif command == "mv":
            move_file(arguments[1], arguments[2])
        elif command == "cd":
            open_dir(arguments[1])
        elif command == "ls":
            try:
                read_dir(arguments[1])
            except:
                read_dir('null')
        elif command == "mkdir":
            make_dir(arguments[1])
        elif command == "rm" and arguments[1] == "-r":
            delete_dir(arguments[2])
        elif command == "exit":
            break
        elif command =="test":
            test(arguments[1])
        else:
            print("No such a command")
    except IndexError:
        print('need arguments')
