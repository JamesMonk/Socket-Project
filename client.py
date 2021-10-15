# *************************************************************************
# Author: James Monk
# Group: Socket Group 81 - SOLO
# Assignment: Socket Project
# Class: CSE 434 Computer Networks
# Updated: 10/15/2021
# *************************************************************************
import socket
import time
import csv
import threading
from threading import Thread
HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 30000        # The port used by the server
identifier = 0      # DHT identifier
n = 0               # ring size
h_table = {}        # this user's hash table
username = ''       # this user's username
h = ""              # host IP of this user
p = 0               # port number for this user
rightNode = ""      # this user's right neighbor (if InDHT)
leftNode = ""       # this user's left neighbor  (if InDHT)

# This function handles the DHT-handling operations of a peer with status InDHT
# It is run as a sub-thread of the main thread which handles user input
def DHT(h, p):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2:
        rightNode = ""
        leftNode = ""
        forward = []
        n = 0
        identifier = 0
        h_table = {}
        while True:
            # Listen for a connection, accept, and read inbound data
            s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s2.bind((h, p))
            s2.listen()
            c, addr = s2.accept()
            data = c.recv(30000)
            data = data.decode()

            # Parse the inbound data, delimit by comma. Ignore commas inside double quotes.
            data = [ "{}".format(x.replace(',', '')) for x in list(csv.reader([data], delimiter=',', quotechar='"'))[0] ]
            
            # Handles "set-id" command. Set identifier and n to the values sent by the leader
            if data[0] == "set-id":
                print("\nmessage received:", data[0])
                del data[0]
                identifier = int(data[0])
                n = int(data[1])
                leftNode = data[2:5]
                rightNode = data[5:8]
                print("id: " + str(identifier))
                print("n: " + str(n))
                print("left neighbor:\n\tusername: " + leftNode[0] + "\n\tIP: " + leftNode[1] + "\n\tport: " + leftNode[2])
                print("right neighbor:\n\tusername: " + rightNode[0] + "\n\tIP: " + rightNode[1] + "\n\tport: " + rightNode[2])

            # Handles "store" command. Checks if the id of each entry matches own identifier.
            # If yes, store the entry in local table
            # If no, append entry to forward list
            # If forward list is empty, do not forward. Storing is complete.
            # Otherwise, forward "store" command to right neighbor followed by all entries in forward list
            elif data[0] == "store":
                print("message received:", data[0])
                del data[0]
                forward = []
                for i in range(0, int(len(data)/11)):
                    if int(data[i*11]) == identifier:
                        h_table[data[i*11+5]] = data[i*11+2:i*11+11]
                    else:
                        forward.append(data[i*11:i*11+11])
                if forward != []:
                    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s3.connect((rightNode[1], int(rightNode[2])))
                    message = "store"
                    for i in range(0, len(forward)):
                        for j in forward[i]:
                            message += "," + str(j)
                    s3.sendall(message.encode())
                else:
                    print("DHT is complete")                

            # Handles "query" command. Calculates the id of the Long Name being queried
            # If the id meets local identifier, try to look up the value in the local table
            # If entry is found, return it to the sender of the "query" command
            # If the id meets local identifier and entry is NOT found, return FAILURE
            # If the id does not meet local identifier, forward "query" command to right neighbor
            # Return right neighbor's response to sender of the "query" command
            elif data[0] == "query":
                print("\nmessage received:", data[0])
                del data[0]
                pos = 0
                for i in data[0]:
                    pos += ord(i)
                pos = pos % 353
                id = pos % n
                if identifier == id:
                    msg = ""  
                    try:
                        for i in h_table[data[0]]:
                            msg += i + ","
                        c.send(msg.encode())        
                    except:
                        c.send(b"FAILURE")
                else:
                    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s3.connect((rightNode[1], int(rightNode[2])))
                    msg = "query," + data[0]
                    s3.send(msg.encode())
                    line = s3.recv(1024).decode()
                    c.send(line.encode())
            
            # Handles teardown-dht command sent by the user
            # send "teardown" command to right neighbor to propigate around the ring
            elif data[0] == "teardown-dht":
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((rightNode[1], int(rightNode[2])))
                s.send(b"teardown")
                s.close()
                s2.close()
                s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s3.bind((h, p))
                s3.listen()
                c, addr = s3.accept()
                if c.recv(1024).decode() == "teardown":
                    identifier = 0
                    h_table = {}
                    s3.close()

            # Handles leave-dht command sent by the user
            # send "teardown" command to right neighbor to propigate around the ring
            # Once "teardown" command is received, this means it has gone all the way around
            # Sends "reset-id" command to right neighbor and wait for that command to 
            # make its way around. Then, send "reset-left" and "reset-right" commands 
            # to right and left neighbors respectively. Finally, send "rebuild-dht" command
            # to right neighbor
            elif data[0] == "leave-dht":
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((rightNode[1], int(rightNode[2])))
                s.send(b"teardown")
                s2.close()
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind((h, p))
                s.listen()
                c, addr = s.accept()
                if c.recv(1024).decode() == "teardown":
                    s.close()
                    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s3.connect((rightNode[1], int(rightNode[2])))
                    msg = "reset-id,0," + str(n-1)
                    s3.send(msg.encode())
                    s2.close()
                    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s3.bind((h, p))
                    s3.listen()
                    c, addr = s3.accept()
                    s3.close()
                    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s3.connect((rightNode[1], int(rightNode[2])))
                    msg = "reset-left," + leftNode[0] + "," + leftNode[1] + "," + leftNode[2]
                    s3.send(msg.encode())
                    time.sleep(0.5)
                    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s3.connect((leftNode[1], int(leftNode[2])))
                    msg = "reset-right," + rightNode[0] + "," + rightNode[1] + "," + rightNode[2]
                    s3.send(msg.encode())
                    s3.close()
                    time.sleep(0.5)
                    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s3.connect((rightNode[1], int(rightNode[2])))
                    s3.send(b"rebuild-dht")
          
            # Handles "teardown" command. Sets identifier to 0 and deletes local table
            # Forwards "teardown" command to right neighbor
            elif data[0] == "teardown":
                print("\nmessage received:", data[0])
                identifier = 0
                h_table = {}
                time.sleep(0.5)
                s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s3.connect((rightNode[1], int(rightNode[2])))
                s3.send(b"teardown")
                s3.close()
              
            # Handles "reset-id" command. Sets local identifier and n value to the ones received
            # Forwards reset-id command to right neighbor.
            elif data[0] == "reset-id":
                print("\nmessage received:", data[0])
                del data[0]
                identifier = int(data[0])
                
                n = int(data[1])
                s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s3.connect((rightNode[1], int(rightNode[2])))
                msg = "reset-id," + str(identifier + 1) + "," + str(n)
                print("id: " + str(identifier))
                print("n: " + str(n))
                s3.send(msg.encode())

            # Handles "reset-right" command. Resets the stored right neighbor to the one received
            elif data[0] == "reset-right":
                print("\nmessage received:", data[0])
                del data[0]
                rightNode = []
                rightNode.append(data[0])
                rightNode.append(data[1])
                rightNode.append(data[2])
                print("new right neighbor:\n\tusername: " + rightNode[0] + "\n\tIP: " + rightNode[1] + "\n\tport: " + rightNode[2])

            # Handles "reset-left" command. Resets the stored left neighbor to the one received
            elif data[0] == "reset-left":
                print("\nmessage received:", data[0])
                del data[0]
                leftNode = []
                leftNode.append(data[0])
                leftNode.append(data[1])
                leftNode.append(data[2])
                print("new left neighbor:\n\tusername: " + leftNode[0] + "\n\tIP: " + leftNode[1] + "\n\tport: " + leftNode[2])

            # Handles "rebuild-dht" command. Reads the csv file line by line
            # and forwards the whole file to the right neighbor in a "store" command 
            elif data[0] == "rebuild-dht":
                print("\nmessage received:", data[0])
                with open("StatsCountry.csv", "r") as o:
                    line = o.readline()
                    forward = []
                    for i in range(0, 241):
                        line = o.readline()
                        parsed = [ "{}".format(x) for x in list(csv.reader([line], delimiter=',', quotechar='"'))[0] ]
                        pos = 0
                        for j in parsed[3]:
                            pos += ord(j)
                        pos = pos % 353
                        id = pos % n
                        forward.append([id, pos, line])
                    s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s3.connect((rightNode[1], int(rightNode[2])))
                    message = "store"
                    for i in range(0, len(forward)):
                        message += "," + str(forward[i][0]) + "," + str(forward[i][1]) + "," + forward[i][2][0:len(forward[i][2])-1]
                    s3.send(message.encode())

            # Handles "add-new-right" command. Sets right neighbor to the node sent over with the "add-new-right" command
            # Sends "reset-left" to previous right neighbor to complete the insertion of the new node.
            # Set the new node's id and n value by sending "set-id,1,<n>" command
            # Reset all ids and n values in the ring by initiating a "reset-id" command to propigate around the DHT ring
            elif data[0] == "add-new-right":
                print("\nmessage received:", data[0])
                del data[0]
                s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s3.connect((rightNode[1], int(rightNode[2])))
                msg = "reset-left," + data[0] + "," + data[1] + "," + data[2]
                s3.send(msg.encode())
                temp = rightNode
                rightNode = []
                rightNode.append(data[0])
                rightNode.append(data[1])
                rightNode.append(data[2])
                n += 1
                print("new right neighbor:\n\tusername: " + rightNode[0] + "\n\tIP: " + rightNode[1] + "\n\tport: " + rightNode[2])
                s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s3.connect((rightNode[1], int(rightNode[2])))
                msg = "set-id,1," + str(n) + "," + username + "," + h + "," + str(p) + "," + temp[0] + "," + temp[1] + "," + temp[2]
                s3.send(msg.encode())
                time.sleep(0.5)
                s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s3.connect((rightNode[1], int(rightNode[2])))
                msg = "reset-id,1," + str(n)
                s3.send(msg.encode())
                s2.close()
                s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s3.bind((h, p))
                s3.listen()
                c, addr = s3.accept()
                s3.close()




# Main thread where user input is taken in and handled. 
while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        
        # Prompt user for a command and send it to server and display return code
        inp = input("command: ")        
        s.connect((HOST, PORT))
        s.sendall(inp.encode())
        data = s.recv(7)
        data = data.decode()
        print("code:", data)

        # If server returns "SUCCESS", handle command
        if data == "SUCCESS":
            parsed = inp.split(" ")     # parse the input command and store as list, delimit by " "
            
            # Handle successful query-dht command. Take long name from user to query the DHT
            if parsed[0] == "query-dht":
                data = s.recv(1024)
                data = data.decode()
                data = data.split(",")
                s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s2.connect((data[1], int(data[2])))
                inp = input("What would you like to query? (give the full name): ")
                msg = "query," + inp
                s2.send(msg.encode())
                data = s2.recv(1024).decode()
                parsed = [ "{}".format(x) for x in list(csv.reader([data], delimiter=',', quotechar='"'))[0] ]
                if parsed == ['FAILURE']:
                    print("ERROR! The record for \"" + inp + "\" was not found in the DHT.")
                else:
                    print("Query results for \"" + inp + "\":")
                    print("\tCountry Code:", parsed[0])
                    print("\tShort Name:", parsed[1])
                    print("\tTable Name:", parsed[2])
                    print("\tLong Name:", parsed[3])
                    print("\t2-Alpha Code:", parsed[4])
                    print("\tCurrency Unit:", parsed[5])
                    print("\tRegion:", parsed[6])
                    print("\tWB-2 Code:", parsed[7])
                    print("\tLatest Population Census:", parsed[8])
            
            # Handles successful "register" command. Starts new thread to listen for connections
            elif parsed[0] == "register":
                username = parsed[1]
                h = parsed[2]
                p = int(parsed[3])
                x = threading.Thread(target=DHT, args=(h, p))
                x.start()

            # Handles successful "setup-dht" command. Does the following actions:
            #   Send "set-id" commands to each host (including local) with identifier, n, and left and right neighbors
            #   Send "store" command to local host with the following syntax:
            #       store,<id>,<n>,<pos>,<line_1_from_csv>,<id>,<n>,<pos>,<line_2_from_csv>,...
            elif parsed[0] == "setup-dht":
                n = int(parsed[1])
                data = s.recv(1024)
                data = data.decode()
                data = data.split(",")
                setup = []
                i = 0
                while i <= len(data)-3:
                    setup.append(data[i:i+3])
                    i += 3
                leftNode = setup[n-1]
                rightNode = setup[1]
                for i in range(0, n):
                    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    message = 'set-id,'
                    host = setup[i][1]
                    port = int(setup[i][2])
                    s2.connect((host, port))
                    message += str(i) + ',' + str(n) + ',' + setup[(i-1) % n][0] + "," + setup[(i-1) % n][1] + "," + setup[(i-1) % n][2]
                    message += ',' + setup[(i+1) % n][0] + ',' + setup[(i+1) % n][1] + ',' + setup[(i+1) % n][2]
                    s2.send(message.encode())
                    s2.close()
                with open("StatsCountry.csv", "r") as o:
                    line = o.readline()
                    forward = []
                    for i in range(0, 241):
                        line = o.readline()
                        parsed = [ "{}".format(x) for x in list(csv.reader([line], delimiter=',', quotechar='"'))[0] ]
                        pos = 0
                        for j in parsed[3]:
                            pos += ord(j)
                        pos = pos % 353
                        id = pos % n
                        forward.append([id, pos, line])
                    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s2.connect((h, p))
                    message = "store"
                    for i in range(0, len(forward)):
                        message += "," + str(forward[i][0]) + "," + str(forward[i][1]) + "," + forward[i][2][0:len(forward[i][2])-1]
                    s2.sendall(message.encode())
            
            # Handles successful "teardown-dht" command. Sends "teardown-dht" to local host
            elif parsed[0] == "teardown-dht":
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((h, p))
                s.send(b"teardown-dht")
            
            # Handles successful "leave-dht" command. Sends "leave-dht" to local host
            elif parsed[0] == "leave-dht":
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((h, p))
                s.send(b"leave-dht")
            
            # Handles successful "join-dht" command.
            #   send "teardown-dht" command to leader (leader information received from server)
            #   send "add-new-right" command to leader to be inserted into the ring
            #   send "rebuild-dht" command to leader to reconstruct the new DHT
            elif parsed[0] == "join-dht":
                data = s.recv(1024)
                data = data.decode()
                data = data.split(",")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((data[1], int(data[2])))
                s.send(b"teardown-dht")
                s.close()
                time.sleep(1)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((data[1], int(data[2])))
                msg = "add-new-right," + username + "," + h + "," + str(p)
                s.send(msg.encode())
                s.close()
                time.sleep(1)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((data[1], int(data[2])))
                s.send(b"rebuild-dht")

            # Handles successful "dht-complete" command. Do nothing.
            elif parsed[0] == "dht-complete":
                pass

            # Handles successful "dht-rebuilt" command. Do nothing.
            elif parsed[0] == "dht-rebuilt":
                pass

            # Handles successful "teardown-complete" command. Do nothing.
            elif parsed[0] == "teardown-complete":
                pass

            # Handles successful "deregister" command. Delete username.
            elif parsed[0] == "deregister":
                username = ""

            # Will never be reached
            else:
                print("Unknown command")