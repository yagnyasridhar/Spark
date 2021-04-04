import sys
import socket
import time

#input from batch 
host = sys.argv[1]
port = int(sys.argv[2])

def start(h, p, content):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((h,p))
    sock.sendall(content)
    time.sleep(10)
    sock.shutdown(socket.SHUT_WR)

    res = ""
    while(True):
        data = sock.recv(1024)
        if(not data):
            break;
        res += data.decode()
    print(res)
    sock.close()

content = "This has error"
start(host, port, content)
