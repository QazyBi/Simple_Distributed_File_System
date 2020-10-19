from flask import Flask, request
import os
import shutil


initialized = False
main_path = None
availabe_size = None
SEPARATOR = '<SEPARATOR>'
BUFFER_SIZE = 1024
LISTENING_IP = "0.0.0.0"
LISTENING_PORT = 8083
DEBUG = True

app = Flask(__name__)


def create_file(filename, IPs=[], data=''):
    '''
    Creates a new file with the path and name <filename> on the current machine
    then replicas it on the nodes with the specified IPs.
    If the procedure was done succefully, it returns True. Otherwise, it returns
    a message with the error as a string.
    '''

    # handle the empty filename case
    if filename == '':
        return ("Error: filename can't be an empty string", False)

    # get rid of '/' at the beginning
    if filename[0] == '/':
        filename = filename[1:]

    full_path = main_path + '/' + filename  # full path in the local machine to read/write files

    # check if the filename is taken
    if os.path.isfile(full_path):
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        return ('Error: file with name {} already exists in IP {}'.format(filename, local_ip), False)

    # get the path of the directory
    path = full_path.split('/')
    if len(path) == 1:
        path = '.'
    else:
        path = '/'.join(path[:-1])

    # create the directory if doesn't exit
    if not os.path.isdir(path):
        os.makedirs(path)

    # create a file with data. If data = '', then it's an empty file
    with open(full_path, 'w') as file:
        file.write(data)

    # replicate the file on the remaining IPs
    if len(IPs) > 0:
        try:
            s = socket.socket()
            s.connect((IPs[0], 8080))

            message = SEPARATOR.join(['create_file', filename, ' '.join(IPs[1:]), data])
            s.sendall(message.encode())
            s.sendall('<DONE>'.encode())

            response = s.recv(BUFFER_SIZE).decode().split(SEPARATOR)

            if response[1] == 'True':
                return (response[0], True)
            else:
                return (response[0], False)
        except:
            return (f"Error: couldn't connect to IP {IPs[0]}", False)

    return ('<DONE>', True)


def read_file(filename):
    '''
    Reads the file <filename> and returns its contiant.
    Returns an error meassage as a string if something wrong happens
    '''
    # handle the empty filename case
    if filename == '':
        return ("Error: filename can't be an empty string", False)

    # get rid of '/' at the beginning
    if filename[0] == '/':
        filename = filename[1:]

    full_path = main_path + '/' + filename  # full path in the local machine to read/write files

    if os.path.exists(full_path):
        if os.path.isdir(full_path):
            return ("Error: {} is a directory".format(filename), False)
        else:
            with open(full_path, 'r') as file:
                ret = ''
                for line in file:
                    ret += line
            return (ret, True)
    else:
        return ("Erorr: {} does not exist".format(filename), False)


def write_file(filename, IPs=[], data=''):
    '''
    Creates a new file with the path and name <filename> on the current machine
    then replicas it on the nodes with the specified IPs.
    If the procedure was done succefully, it returns True. Otherwise, it returns
    a message with the error as a string.
    '''
    return create_file(filename, IPs, data)


def copy_file(filename, target_filename):
    '''
    copy file from <filename> to <target_filename>
    '''
    # create a file with the same name if <target_filename> doens't ends with '/'
    splits_filename = filename.split('/')
    splits_target_filename = target_filename.split('/')
    if splits_target_filename[-1] == '':
        target_filename += splits_filename[-1]

    message, flag = read_file(filename)  # containt of <filename>
    # if an error happend, then return it
    if not flag:
        return (message, flag)

    # write the continat of <filename> to <target_filename>
    return write_file(target_filename, IPs=[], data=message)


def move_file(filename, target_filename):
    '''
    move file from <filename> to <target_filename>
    '''
    # create a file with the same name if <target_filename> doens't ends with '/'
    splits_filename = filename.split('/')
    splits_target_filename = target_filename.split('/')
    if splits_target_filename[-1] == '':
        target_filename += splits_filename[-1]

    # check if they both have the same filename
    if filename == target_filename or ('./' + target_filename) == filename or target_filename == ('./' + filename):
        return ('Error: {} and {} have the same filename'.format(filename, target_filename), False)

    message, flag = read_file(filename)  # containt of <filename>
    # if an error happend, then return it
    if not flag:
        return (message, flag)

    # write the continat of <filename> to <target_filename>
    message, flag = write_file(target_filename, IPs=[], data=message)
    # if an error happend, ther return it
    if not flag:
        return (message, flag)

    # delete filename
    return delete_file(filename)


def delete_file(filename):
    '''
    delete a file called <filename>
    '''
    # handle the empty filename case
    if filename == '':
        return ("Error: filename can't be an empty string", False)

    # get rid of '/' at the beginning
    if filename[0] == '/':
        filename = filename[1:]

    full_path = main_path + '/' + filename  # full path in the local machine to read/write files

    if os.path.exists(full_path):
        if os.path.isdir(full_path):
            return ("Error: {} is a directory".format(filename), False)
        else:
            os.remove(full_path)
            return ('<DONE>', True)
    else:
        return ("Erorr: {} does not exist".format(filename), False)

    '''
    returns a space-separated list of file names listed in directory <path> as a string.
    returns an error meassage as a string if something wrong happens.
    '''


def read_dir(path):
    # handle the empty path case
    if path == '':
        return ("Error: path can't be an empty string", False)

    # get rid of '/' at the beginning
    if path[0] == '/':
        path = path[1:]

    full_path = main_path + '/' + path  # full path in the local machine to read/write files

    if os.path.exists(full_path):
        if os.path.isfile(full_path):
            return ("Error: {} is a file".format(path), False)
        else:
            return (' '.join(os.listdir(full_path)), True)
    else:
        return ("Erorr: {} does not exist".format(path), False)


def delete_dir(path, permission=False):
    '''
    deletes the given path directory
    returns an error meassage as a string if something wrong happens.
    '''
    # handle the empty path case
    if path == '':
        return ("Error: path can't be an empty string", False)

    # get rid of '/' at the beginning
    if path[0] == '/':
        path = path[1:]

    full_path = main_path + '/' + path  # full path in the local machine to read/write files

    if os.path.exists(full_path):
        if os.path.isfile(full_path):
            return ("Error: {} is a file".format(path), False)
        else:
            # return an error if the folder isn't empty and no permission given
            _, flag = get_directory_size(full_path)
            if flag and not permission:
                return ("Error: permission need because {} contains files".format(path), False)

            shutil.rmtree(full_path)
            return ('<DONE>', True)
    else:
        return ('<DONE>', True)


def get_directory_size(directory):
    '''
    return the size of files in a given directory and whether or not it contains a file
    '''
    """Returns the `directory` size in bytes."""
    total = 0
    flag = False
    try:
        # print("[+] Getting the size of", directory)
        for entry in os.scandir(directory):
            if entry.is_file():
                # if it's a file, use stat() function
                total += entry.stat().st_size
                flag = True
            elif entry.is_dir():
                # if it's a directory, recursively call this function
                temp = get_directory_size(entry.path)
                total += temp[0]
                flag = max(flag, temp[1])
    except NotADirectoryError:
        # if `directory` isn't a directory, get the file size then
        return os.path.getsize(directory)
    except PermissionError:
        # if for whatever reason we can't open the folder, return 0
        return 0
    return (total, flag)


def disk_size():
    '''
    return the availbe disk size
    '''
    return (str(2048 - get_directory_size(main_path)[0]), True)


def copy_to_server(filename, IP):
    '''
    copys the file with filename to a new server
    returns True if it was done succefully. Otherwise it returns the error
    '''
    data, flag = read_file(filename)

    if flag:
        try:
            s = socket.socket()
            s.connect((IP, 8080))

            message = SEPARATOR.join(['create_file', filename, '', data])
            s.sendall(message.encode())
            s.sendall('<DONE>'.encode())

            response = s.recv(BUFFER_SIZE).decode().split(SEPARATOR)

            if response[1] == 'True':
                return (response[0], True)
            else:
                return (response[0], False)
        except:
            return (f"Error: couldn't connect to IP {IP[0]}", False)

        return ('<DONE>', True)
    else:
        return (data, False)


def list_dir(path):
    pass


@app.route("/init", methods=['GET'])
def initialize():
    global initialized
    global main_path
    global availabe_size

    initialized = True
    main_path = '/home/qazybek/Development/GitHub/f20/ds/TEMP_PROJECT/Simple_Distributed_File_System/s3/data'
    availabe_size = 1024 * 2  # availabe disk size on the storage server in MB

    # remove the directory if it exists
    if os.path.isdir(main_path):
        shutil.rmtree(main_path)

    # create the directory where data is stored
    os.mkdir(main_path)

    return {"response": f"{availabe_size}", "status": "200"}


@app.route("/ping", methods=['GET'])
def ping_route():
    response = {"response": "echo"}
    return response


@app.route("/disk", methods=['GET'])
def disk_route():
    response = {"response": disk_size()}
    return response


@app.route("/file/<filename>/move", methods=['POST'])
def file_move_route(filename):
    target_filename = app.args.get("target_filename")
    response = {"response": move_file(filename, target_filename)}
    return response


@app.route("/file/<filename>/copy", methods=['POST'])
def file_copy_route(filename):
    target_filename = request.args.get("target_filename")
    response = {"response": copy_file(filename, target_filename)}
    return response


@app.route("/file/<filename>/send-to-server", methods=['POST'])
def file_send_to_server_route(filename):
    IPs = request.args.get("IPs")
    response = {"response": copy_to_server(filename, IPs)}
    return response


@app.route("/file/<filename>", methods=['POST', 'GET', 'DELETE'])
def file_route(filename):
    if request.method == "POST":
        IPs = request.args.get('IPs')
        try:
            data = request.args.get('data')
        except:
            data = ""
        return {"response": write_file(filename, IPs, data)}

    elif request.method == "DELETE":
        response = {"response": delete_file(filename)}
        return response
    else:
        return app.send_static_file(filename)


@app.route("/dir/<path>", methods=['POST', 'GET', 'DELETE'])
def dir_route(path):
    if request.method == "POST":
        return {"response": "NOT IMPLEMENTED"}
    elif request.method == "DELETE":
        try:
            permission = True if request.args.get("permission") else False
        except:
            permission = False

        response = {"response": delete_dir(path, permission)}
        return response

    else:
        response = {"response": list_dir(path)}
        return response


if __name__ == "__main__":
    app.run(host=LISTENING_IP, port=LISTENING_PORT, debug=DEBUG)
    initialize()

    app.logger.info(f'[RUNNING] Listening on ({LISTENING_IP}, {LISTENING_IP})')
