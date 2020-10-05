import requests
import os
import socket

BUFFER_SIZE = 1024
SEPARATOR = "<SEPARATOR>"

namenode_IP = "10.0.15.12"
port = 8080
url = "http://" + namenode_IP + ":" + str(port)

def initialize():
    r = requests.get(url + "/init")
    data = r.json()
    print(data['size'])


def create_file(file):
    path, filename = os.path.split(file)
    r = requests.post(url + "/file", params={'command': 'create',
                                             'filename': filename,
                                            'path': path})

    
def read_file(file):
    path, filename = os.path.split(file)
    r = requests.post(url + "/file", params={'command': 'read',
                                             'filename': filename,
                                            'path': path})
    data = r.json()
    ip = data['ip']
    port = data['port']
    
    s = socket.socket()
    s.bind((ip, port))
    s.listen()
    f = open(file, "wb")
    while True:
        conn, addr = s.accept()
        stream = conn.recv(BUFFER_SIZE)
        parameters = stream.split(SEPARATOR)
        data = parameters[0]
        flag = parameters[1]
        while data:
            f.write(data)
            data = conn.recv(BUFFER_SIZE)
            f.close()
        conn.close()


def write_file(file):
    path, filename = os.path.split(file)
    r = requests.post(url + "/file", params={'command': 'write',
                                             'filename': filename,
                                            'path': path})
    data = r.json()
    ip_list = data['ip']
    port = data['port']
    new_filename = data['new_filename']
    
    s = socket.socket()
    s.connect((ip_list[0], port))
    f = open(file, "rb")
    data = f.read(BUFFER_SIZE)
    stream = 'write' + SEPARATOR + filename + SEPARATOR + " ".join(ip_list) + SEPARATOR + data
    while stream:
        s.send(stream)
    f.close()
    s.close()


def delete_file(file):
    path, filename = os.path.split(file)
    r = requests.post(url + "/file", params={'command': 'delete',
                                             'filename': filename,
                                            'path': path})


def file_info(file):
    path, filename = os.path.split(file)
    r = requests.post(url + "/file", params={'command': 'info',
                                             'filename': filename,
                                            'path': path})
    data = r.json()


def copy_file(file, new_dir):
    path, filename = os.path.split(file)
    r = requests.post(url + "/file", params={'command': 'copy',
                                             'filename': filename,
                                            'path': path,
                                            'new_dir': new_dir})
    if r.status_code == 'no such directory':
        make_dir(new_dir)
        r = requests.post(url + "/file", params={'command': 'copy',
                                             'filename': filename,
                                            'path': path,
                                            'new_dir': new_dir})


def move_file(file, new_dir):
    path, filename = os.path.split(file)
    r = requests.post(url + "/file", params={'command': 'move',
                                             'filename': filename,
                                            'path': path,
                                            'new_dir': new_dir})
    if r.status_code == 'no such directory':
        make_dir(new_dir)
        r = requests.post(url + "/file", params={'command': 'move',
                                             'filename': filename,
                                            'path': path,
                                            'new_dir': new_dir})


def open_dir(current_dirrectory, target_directory):
    r = requests.post(url + "/dir", params={'command': 'open',
                                             'current_dirrectory': current_dirrectory,
                                            'target_directory': target_directory})


def read_dir(target_directory):
    r = requests.post(url + "/dir", params={'command': 'read',
                                             'target_directory': target_directory})
    data = r.json()
    print(data['files'])


def make_dir(target_directory):
    r = requests.post(url + "/dir", params={'command': 'make',
                                            'target_directory': target_directory})


def delete_dir(target_directory):
    r = requests.post(url + "/dir", params={'command': 'delete',
                                             'target_directory': target_directory})
