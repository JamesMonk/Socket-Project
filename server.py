# *************************************************************************
# Author: James Monk
# Group: Socket Group 81 - SOLO
# Assignment: Socket Project
# Class: CSE 434 Computer Networks
# Updated: 10/15/2021
# *************************************************************************
import socket
import random

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 30000        # Port to listen on (non-privileged ports are > 1023)

def contains(table, element, x): # Checks if a username (x=0) or port(x=2) is registered and FREE
    for i in table:
        if i[x] == element:
            return True
    return False

def findRow(table, element): # Returns the 3-tuple of a given username
    for i in table:
        if i[0] == element:
            return i

def fail(conn):
    conn.send("FAILURE".encode())
    
def success(conn):
    conn.send("SUCCESS".encode())

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    free = []       # Table for 3-tuples of FREE peers
    dht = []        # Table for 3-tuples of peers maintaining the DHT
    leader = ""     # Username of leader
    leaderRow = []
    setup = False   # True: DHT has been setup. False: DHt has not been setup.
    leavingUser = ""
    while True:
        conn, addr = s.accept()
        print('Connected by', addr)
        data = conn.recv(1024)
        data = data.decode()
        parsed = data.split(" ")
        print("message received:", data)

        # register command handler. Sends FAILURE if not enough arguments, given username
        # is already registered as FREE, or port is already in use. Otherwise, registers 
        # new user with FREE status and sends SUCCESS.
        if (parsed[0] == "register"):
            del parsed[0]
            if len(parsed) != 3 or contains(free, parsed[0], 0) or contains(free, parsed[2], 2): 
                fail(conn)
            else:
                free.append(parsed)
                success(conn)        
        
        # setup-dht command handler. Sends FAILURE if not enough arguments, given username
        # is not registered, n < 2, fewer users are registered than n, or the DHT is already setup.
        # Otherwise, sends SUCCESS and sends a comma-separated list of free users and appends 
        # n users to DHT table while removing their free status
        elif (parsed[0] == "setup-dht"):
            del parsed[0]
            if len(parsed) != 2 or not contains(free, parsed[1], 0) or int(parsed[0]) < 2 or int(parsed[0]) > len(free) or setup:
                fail(conn)
            else:
                leader = parsed[1]
                leaderRow = findRow(free, leader)
                success(conn)
                data = ""
                for i in free:
                    for j in i:
                        data = data + j + ","
                conn.send(data.encode())
                for i in range(0, int(parsed[0])):
                    dht.append(free[0])
                    del free[0]
        
        # dht-complete command handler. Sends FAILURE if not enough arguments or the given 
        # username is not the leader. Otherwise, sends SUCCESS and records that DHT has been
        # setup.
        elif (parsed[0] == "dht-complete"):
            del parsed[0]
            if len(parsed) != 1 or parsed[0] != leader:
                fail(conn)
            setup = True
            success(conn)
        
        # query-dht command handler. Sends FAILURE if not enough arguments, DHT has not
        # been setup, or the given username is registered as FREE. Otherwise
        elif (parsed[0] == "query-dht"):
            del parsed[0]
            if len(parsed) != 1 or not setup or not contains(free, parsed[0], 0):
                fail(conn)
            else:
                val = random.randint(0, len(dht)-1)
                success(conn)
                msg = "" + dht[val][0] + "," + dht[val][1] + "," + dht[val][2]
                conn.send(msg.encode())
        
        # deregister command handler. Sends FAILURE if not enough arguments or given username
        # is not registered as FREE. Otherwise, given user is removed from the FREE registry
        # and sends SUCCESS.
        elif (parsed[0] == "deregister"):
            del parsed[0]
            if len(parsed) != 1 or not contains(free, parsed[0], 0):
                fail(conn)
            else:    
                free.remove(findRow(free, parsed[0]))
                success(conn)
        
        # teardown-dht command handler. Sends FAILURE if not enough arguments, the initiator
        # is not the leader of the DHT, or the DHT is not setup. Otherwise, setup = False, 
        # and return SUCCESS.
        elif (parsed[0] == "teardown-dht"):
            del parsed[0]
            if len(parsed) != 1 or parsed[0] != leader or not setup:
                fail(conn)
            else:
                setup = False
                success(conn)

        # teardown-complete command handler. Sends FAILURE if not enough arguments, the user
        # is not the leader, or the DHT is still setup (e.g. teardown-dht was never initiated).
        # Otherwise, delete leader information and change status of DHT-handlers to FREE.
        elif (parsed[0] == "teardown-complete"):
            del parsed[0]
            if len(parsed) != 1 or parsed[0] != leader or setup:
                fail(conn)
            else:
                leader = ""
                for i in dht:
                    free.append(i)
                dht = []
                success(conn)

        # leave-dht command handler. Sends FAILURE if not enough arguments, the user is not
        # in the DHT, or the DHT is not setup. Otherwise, save the user trying to leave and
        # return SUCCESS.
        elif (parsed[0] == "leave-dht"):
            del parsed[0]
            if len(parsed) != 1 or not contains(dht, parsed[0], 0) or not setup:
                fail(conn)
            else:
                leavingUser = parsed[0]
                success(conn)
            
        # dht-rebuilt command handler. Sends FAILURE if not enough arguments or the user is
        # not the same one that originally sent the leave-dht command. Otherwise, change the
        # status of the user from InDHT to FREE--effectively removing them from the DHT 
        # ring--save the new leader received from the user, and return SUCCESS.
        elif (parsed[0] == "dht-rebuilt"):
            del parsed[0]
            if len(parsed) != 2 or parsed[0] != leavingUser:
                fail(conn)
            else:
                free.append(findRow(dht, leavingUser))
                dht.remove(findRow(dht, leavingUser))
                leader = parsed[1]
                leaderRow = findRow(dht, leader)
                print("new leader:", leader)
                leavingUser = ""
                success(conn)

        elif (parsed[0] == "join-dht"):
            del parsed[0]
            if len(parsed) != 1 or not contains(free, parsed[0], 0):
                fail(conn)
            else:
                success(conn)
                msg = ""
                dht.append(findRow(free, parsed[0]))
                msg = "" + leaderRow[0] + "," + leaderRow[1] + "," + leaderRow[2]
                conn.send(msg.encode())

        # Invalid command handler. Returns FAILURE by default.
        else:
            fail(conn)
