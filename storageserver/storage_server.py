import socket                   
import os
import shutil

initialized = False
main_path = None
availabe_size = None
SEPARATOR = '<SEPARATOR>'
BUFFER_SIZE = 1024


'''
Initializes the node, deletes any files if exist, and returns the 
available size (1536 MB by default).
'''
def initialize():
	global init
	initialized = True

	global main_path
	main_path = '/data'
	
	global availabe_size
	availabe_size = 1024 * 2 # availabe disk size on the storage server in MB

	# remove the directory if it exists
	if os.path.isdir(main_path):
		shutil.rmtree(main_path) 

	# create the directory where data is stored
	os.mkdir(main_path) 

	return (str(availabe_size), True)

'''
Creates a new file with the path and name <filename> on the current machine
then replicas it on the nodes with the specified IPs.
If the procedure was done succefully, it returns True. Otherwise, it returns
a message with the error as a string.
'''
def create_file(filename, IPs = [], data = ''):
	# handle the empty filename case
	if filename == '':
		return ("Error: filename can't be an empty string", False)

	# get rid of '/' at the beginning
	if filename[0] == '/':
		filename = filename[1:]

	full_path = main_path + '/' + filename # full path in the local machine to read/write files

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

			message = SEPARATOR.join(['create_file', filename, ' '.join(IPs[1: ]), data])
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

'''
Reads the file <filename> and returns its contiant.
Returns an error meassage as a string if something wrong happens
'''
def read_file(filename):
	# handle the empty filename case
	if filename == '':
		return ("Error: filename can't be an empty string", False)

	# get rid of '/' at the beginning
	if filename[0] == '/':
		filename = filename[1:]

	full_path = main_path + '/' + filename # full path in the local machine to read/write files

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

'''
Creates a new file with the path and name <filename> on the current machine
then replicas it on the nodes with the specified IPs.
If the procedure was done succefully, it returns True. Otherwise, it returns
a message with the error as a string.
'''
def write_file(filename, IPs = [], data = ''):
	return create_file(filename, IPs, data)

'''
copy file from <filename> to <target_filename>
'''
def copy_file(filename, target_filename):
	# create a file with the same name if <target_filename> doens't ends with '/'
	splits_filename = filename.split('/')
	splits_target_filename = target_filename.split('/')
	if splits_target_filename[-1] == '':
		target_filename += splits_filename[-1]

	message, flag = read_file(filename) # containt of <filename>
	# if an error happend, then return it
	if flag == False: 
		return (message, flag)

	# write the continat of <filename> to <target_filename>
	return write_file(target_filename, IPs = [], data = message) 

'''
move file from <filename> to <target_filename>
'''
def move_file(filename, target_filename):
	# create a file with the same name if <target_filename> doens't ends with '/'
	splits_filename = filename.split('/')
	splits_target_filename = target_filename.split('/')
	if splits_target_filename[-1] == '':
		target_filename += splits_filename[-1]

	# check if they both have the same filename
	if filename == target_filename or ('./' + target_filename) == filename or target_filename == ('./' + filename):
		return ('Error: {} and {} have the same filename'.format(filename, target_filename), False)

	message, flag = read_file(filename) # containt of <filename>
	# if an error happend, then return it
	if flag == False: 
		return (message, flag)

	# write the continat of <filename> to <target_filename>
	message, flag = write_file(target_filename, IPs = [], data = message) 
	# if an error happend, ther return it
	if flag == False:
		return (message, flag)

	# delete filename
	return delete_file(filename)

'''
delete a file called <filename>
'''
def delete_file(filename):
	# handle the empty filename case
	if filename == '':
		return ("Error: filename can't be an empty string", False)

	# get rid of '/' at the beginning
	if filename[0] == '/':
		filename = filename[1:]

	full_path = main_path + '/' + filename # full path in the local machine to read/write files

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

	full_path = main_path + '/' + path # full path in the local machine to read/write files

	if os.path.exists(full_path):
		if os.path.isfile(full_path):
			return ("Error: {} is a file".format(path), False)
		else:
			return (' '.join(os.listdir(full_path)), True)
	else:
		return ("Erorr: {} does not exist".format(filename), False)

'''
deletes the given path directory
returns an error meassage as a string if something wrong happens.
'''
def delete_dir(path, permission = False):
	# handle the empty path case
	if path == '':
		return ("Error: path can't be an empty string", False)

	# get rid of '/' at the beginning
	if path[0] == '/':
		path = path[1:]

	full_path = main_path + '/' + path # full path in the local machine to read/write files

	if os.path.exists(full_path):
		if os.path.isfile(full_path):
			return ("Error: {} is a file".format(path), False)
		else:
			# return an error if the folder isn't empty and no permission given 
			_, flag = get_directory_size(full_path)
			if flag == True and permission == False:
				return ("Error: permission need because {} contains files".format(path), False)

			shutil.rmtree(full_path)
			return ('<DONE>', True)
	else:
		return ('<DONE>', True)

'''
return the size of files in a given directory and whether or not it contains a file
'''
def get_directory_size(directory):
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

'''
return the availbe disk size
'''
def disk_size():
	return (str(2048 - get_directory_size(main_path)[0]), True)

'''
copys the file with filename to a new server
returns True if it was done succefully. Otherwise it returns the error
'''
def copy_to_server(filename, IP):
	data, flag = read_file(filename)

	if flag == True:
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
			return (f"Error: couldn't connect to IP {IPs[0]}", False)

		return ('<DONE>', True)
	else:
		return (data, False)

'''
send an acknowledgement message to the target server
'''
def acknowledgement(c, message):
	if message[1]:
		print('The command was excuted succefully!')
	else:
		print(message[0])

	# merge message using SEPARATOR
	message = SEPARATOR.join([str(i) for i in message])

	# send acknowledgement message
	c.sendall(message.encode())


# initialize the server automatically whenever it's turned on
initialize()

# Create a socket object
port = 8080
s = socket.socket()
ip = socket.gethostbyname(socket.gethostname())   

# Bind to the port
s.bind((ip, port))

# Now wait for client connection.
s.listen(5)
print('The server is running on the private ip:', ip)	
print('Listening on port:', port)
print('-----------')

while True:
	# Establish connection with client.
	c, addr = s.accept()    

	print('Recived a connection from {}', addr)

	# receive the data stream
	stream = ''
	while True:
		data = c.recv(BUFFER_SIZE)
		if data.decode()[-6: ] == '<DONE>':
			break
		stream += data.decode()

	# split the stream into parameters
	parameters = stream.split(SEPARATOR)
	command = parameters[0]

	print(f'Recieved the follwoing command: {command}')

	if command == 'initialize':
		acknowledgement(c, initialize())

	elif command == 'create_file':
		filename = parameters[1]
		if parameters[2] == '':
			IPs = []
		else:
			IPs = parameters[2].split(' ')
		acknowledgement(c, create_file(filename, IPs))

	elif command == 'read_file':
		filename = parameters[1]
		acknowledgement(c, read_file(filename))

	elif command == 'write_file':
		filename = parameters[1]
		if parameters[2] == '':
			IPs = []
		else:
			IPs = parameters[2].split(' ')
		data = parameters[3]
		acknowledgement(c, write_file(filename, IPs, data))

	elif command == 'copy_file':
		filename = parameters[1]
		target_filename = parameters[2]
		acknowledgement(c, copy_file(filename, target_filename))

	elif command == 'move_file':
		filename = parameters[1]
		target_filename = parameters[2]
		acknowledgement(c, move_file(filename, target_filename))

	elif command == 'delete_file':
		filename = parameters[1]
		acknowledgement(c, delete_file(filename))

	elif command == 'read_dir':
		path = parameters[1]
		acknowledgement(c, read_dir(path))
	
	elif command == 'delete_dir':
		path = parameters[1]
		permission = False
		if len(parameters) > 2 and parameters[2] == 'yes':
			permission = True
		acknowledgement(c, delete_dir(path, permission))

	elif command == 'disk_size':
		acknowledgement(c, disk_size())

	elif command == 'ping':
		acknowledgement(c, ('echo', True))

	elif command == 'copy_to_server':
		filename = parameters[1]
		IP = parameters[2]
		acknowledgement(c, copy_to_server(filename, IP))
	else:
		acknowledgement(c, ('Error: no such a command!', False))

	print('')
	c.close()


