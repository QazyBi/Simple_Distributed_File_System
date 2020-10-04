# Simple_Distributed_File_System
### Nameserver
- stores tuple (dir, partion id, storage server) in MongoDB document
- receives connections from clients
  - [ ] add authorization
- Connection established through REST API
- GET request will return output of `ls` command
- POST request will store new file info in MongoDB document and return to client ip addresses and port numbers of servers to send files
