import socket
import random
import time
from datetime import datetime

host, port = ('127.0.0.1', 65431)

print('Server Srtarting')

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((host, port))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print('Connected by ', addr)
        while True:
            conn.sendall((str(datetime.now()) + ", " + str(random.random()*1000) + ", ABCD123\n").encode())
            conn.sendall((str(datetime.now()) + ", " + str(random.random()*1000) + ", ABCD123\n").encode())
            conn.sendall((str(datetime.now()) + ", " + str(random.random()*1000)+ ", ABCD123\n").encode())
            conn.sendall((str(datetime.now()) + ", " + str(random.random()*1000)+ ", ABCD123\n").encode())
            time.sleep(4)
