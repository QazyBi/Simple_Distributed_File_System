# How to launch and use system
# Architectural diagrams
![client nameserver storage](https://github.com/QazyBi/Simple_Distributed_File_System/blob/main/img/client_nameserver_storage.png)
![client storage](https://github.com/QazyBi/Simple_Distributed_File_System/blob/main/img/client_storage.png)
![client nameserver only](https://github.com/QazyBi/Simple_Distributed_File_System/blob/main/img/nameserver_client_only.png)
# Description of communication protocols
We used 2 ways of communication: HTTP methods and sockets; to compare and work with both of them.
## 1. Description of communication between Client and NameServer
The communication happens via GET and POST methods. Several examples:
```
r = requests.get(url + "/init")
r = requests.post(url + "/file", params={'command': 'create', 'filename': filename, 'path': path})
```
NameServer gets parameters dictionary from Client. Example:
```
params={'command': 'read', 'filename': filename, 'path': path}
```
Client gets response from NameServer in JSON fromat. Example:
```
response = {
                        "ip": element['storages'],
                        "size": element['size'],
                        "datetime": element['datetime'],
                        "status": "success",
                        "response": "found record with this file"
                    }
```
## 2. Description of communication between Client and StorageServer
The communication, more precisely files transfer, happens via sockets. It's a common and good way to transfer big files. Moreover we practiced it during lab. Example:
```
s = socket.socket()
conn, addr = s.accept()
conn.send(outcoming_stream)
data = conn.recv(BUFFER_SIZE)
```
## 3. Description of communication between NameServer and StorageServer
The communication, more precisely messaging, happens via sockets. StorageServer already was written using sockets, that's why NameServer used sockets to communicate with storages. Example:
```
s.connect((ip, port))
s.sendall(message.encode())
s.sendall("<DONE>".encode())
```
# Contribution of each team member
