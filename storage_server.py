import socket                   

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

    '''
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!TO be done later!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	excute the command
	'''
	
    s.shutdown(socket.SHUT_WR)
    s.close()how to shutdown a port in linux
	
