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
        data = conn.recv(BUFFER_SIZE)
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
    ip = data['ip']
    port = data['port']
    new_filename = data['new_filename']
    
    s = socket.socket()
    s.connect((ip, port))
    f = open(file, "rb")
    data = f.read(BUFFER_SIZE)
    while data:
        s.send(data)
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


def open_dir(cur_dir, new_dir):
    r = requests.post(url + "/dir", params={'command': 'open',
                                             'cur_dir': cur_dir,
                                            'new_dir': new_dir})


def read_dir(directory):
    r = requests.post(url + "/dir", params={'command': 'read',
                                             'directory': directory})
    data = r.json()
    print(data['files'])


def make_dir(new_dir):
    r = requests.post(url + "/dir", params={'command': 'make',
                                            'new_dir': new_dir})


def delete_dir(directory):
    r = requests.post(url + "/dir", params={'command': 'delete',
                                             'directory': directory})

