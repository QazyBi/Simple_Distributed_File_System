import socket                   
import os

initialized = False
main_path = None
availabe_size = None

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
	availabe_size = 1536 # availabe disk size on the storage server in MB

	# remove the directory if it exists
	if os.path.isdir(main_path):
		os.path.rmdir(main_path)
	# create the directory where data is stored
	os.mkdir(main_path) 

	return availabe_size


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
    
    # receive the file name
    command = c.recv(1024)
    command = command.decode()

    if command == 'initialize':
    	initialize()


    '''
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!TO be done later!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	excute the command
	'''

    s.shutdown(socket.SHUT_WR)
    s.close()how to shutdown a port in linux
	
