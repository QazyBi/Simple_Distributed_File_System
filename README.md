# Description of communication protocols
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
j = r.json()
size = j['size']
storages = j['storages']
datetime = j['datetime']
```
## 2. Description of communication between Client and StorageServer
The communication happens via sockets.
## 3. Description of communication between NameServer and StorageServer
