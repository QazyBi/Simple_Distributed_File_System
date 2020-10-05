import socket                   
import os

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
	main_path = '/home/ubuntu/data'
	
	global availabe_size
	availabe_size = 1024 * 2 # availabe disk size on the storage server in MB

	# remove the directory if it exists
	if os.path.isdir(main_path):
		os.path.rmdir(main_path)
	# create the directory where data is stored
	os.mkdir(main_path) 

	return (str(availabe_size), True)

'''
Creates a new file with the path and name <filename> on the current machine
then replicas it on the nodes with the specified IPs.
If the procedure was done succefully, it returns True. Otherwise, it returns
a message with the error as a string.
'''
def create_file(filename, IPs = [], data = None):
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

	# create directory if doesn't exist
	if not os.path.isdir(path):
		os.makedirs(path)

	if data == None:
		# create an empty file
		with open(full_path, 'w') as file: 
			pass
	else:
		# create a file with data
		with open(full_path, 'w') as file:
			file.write(data)

	'''
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!TO be done later!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	replicate the file on the remaining IPs
	'''

	return ('<DONE>', True)

'''
Reads the file <filename> and returns its contiant.
Returns an error meassage as a string if the 
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
def write_file(filename, IPs = [], data):
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
send an acknowledgement message to the target server
'''
def acknowledgement(target_IP, target_port, message):
	local_s = socket.socket()
	local_ip = socket.gethostbyname(socket.gethostname())   

	# Bind to the port 8000
	local_s.bind((local_ip, 8000))

	# coonecnt to the target server
	local_s.connect((target_IP, target_port))

	# send acknowledgement message
	local_s.sendall(message.encode())


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

	# receive the data stream
	stream = ''
	while True:
		data = c.recv(BUFFER_SIZE)
		if not data:
			break
		stream += data.decode()

	# split the stream into parameters
	parameters = stream.split(SEPARATOR)
	command = parameters[0]

	if command == 'initialize':
		acknowledgement(addr, port, initialize())

	elif command == 'create_file':
		filename = parameters[1]
		IPs = parameters[2].split(' ')
		acknowledgement(addr, port, create_file(filename, IPs))

	elif command == 'read_file':
		filename = parameters[1]
		acknowledgement(addr, port, read_file(filename))

	elif command == 'write_file':
		filename = parameters[1]
		IPs = parameters[2].split(' ')
		data = parameters[3]
		acknowledgement(addr, port, write_file(filename, IPs, data))

	elif command == 'copy_file':
		filename = parameters[1]
		target_filename = parameters[2]
		acknowledgement(addr, port, copy_file(filename, target_filename))
		
	'''
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!TO be done later!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	excute the command
	'''

	s.shutdown(socket.SHUT_WR)
	s.close()
