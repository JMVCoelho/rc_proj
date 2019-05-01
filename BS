#!/usr/bin/env python3

import socket
import sys
import signal
import select
import os
from os import listdir
from glob import glob
from os.path import isdir, isfile, join
import shutil
import datetime
import time


BUFFER_SIZE = 1024
SOCKET_TIMEOUT = 2


def getArguments():
    """
    Read the script's arguments

    Return: self port and CS's UDP IP and port
    """
    if len(sys.argv) == 1:
        PORT_SELF = 59000
        UDP_IP = socket.gethostbyname(socket.gethostname())
        UDP_PORT = 58042
    elif len(sys.argv) == 3:
        #only BSport as argument
        if sys.argv[1] == "-b":
            PORT_SELF = int(sys.argv[2])
            UDP_IP = socket.gethostbyname(socket.gethostname())
            UDP_PORT = 58042
        # only CSname as argument
        elif sys.argv[1] == "-n":
            PORT_SELF = 59000
            UDP_IP = sys.argv[2]
            UDP_PORT = 58042
        # only CSport as argument
        elif sys.argv[1] == "-p":
            PORT_SELF = 59000
            UDP_IP = socket.gethostbyname(socket.gethostname())
            UDP_PORT = int(sys.argv[2])
        else:
            print("Error: Bad argument\n")
            sys.exit(1)
    elif len(sys.argv) == 5:
        if sys.argv[1] == "-n" and sys.argv[3] == "-p":
            PORT_SELF = 59000
            UDP_IP = sys.argv[2]
            UDP_PORT = int(sys.argv[4])
        elif sys.argv[1] == "-p" and sys.argv[3] == "-n":
            PORT_SELF = 59000
            UDP_IP = sys.argv[4]
            UDP_PORT = int(sys.argv[2])
        elif sys.argv[1] == "-b" and sys.argv[3] == "-n":
            PORT_SELF = int(sys.argv[2])
            UDP_IP = sys.argv[4]
            UDP_PORT = 58042
        elif sys.argv[1] == "-n" and sys.argv[3] == "-b":
            PORT_SELF = int(sys.argv[4])
            UDP_IP = sys.argv[2]
            UDP_PORT = 58042
        elif sys.argv[1] == "-b" and sys.argv[3] == "-p":
            PORT_SELF = int(sys.argv[2])
            UDP_IP = socket.gethostbyname(socket.gethostname())
            UDP_PORT = int(sys.argv[4])
        elif sys.argv[1] == "-p" and sys.argv[3] == "-b":
            PORT_SELF = int(sys.argv[4])
            UDP_IP = socket.gethostbyname(socket.gethostname())
            UDP_PORT = int(sys.argv[2])
        else:
            print("Error: Bad arguments\n")
            sys.exit(1)
    elif len(sys.argv) == 7:
        if sys.argv[1] == "-b" and sys.argv[3] == "-n" and sys.argv[5] == "-p":
            PORT_SELF = int(sys.argv[2])
            UDP_IP = sys.argv[4]
            UDP_PORT = int(sys.argv[6])
        elif sys.argv[1] == "-b" and sys.argv[3] == "-p" and sys.argv[5] == "-n":
            PORT_SELF = int(sys.argv[2])
            UDP_IP = sys.argv[6]
            UDP_PORT = int(sys.argv[4])
        elif sys.argv[1] == "-n" and sys.argv[3] == "-b" and sys.argv[5] == "-p":
            PORT_SELF = int(sys.argv[4])
            UDP_IP = sys.argv[2]
            UDP_PORT = int(sys.argv[6])
        elif sys.argv[1] == "-n" and sys.argv[3] == "-p" and sys.argv[5] == "-b":
            PORT_SELF = int(sys.argv[6])
            UDP_IP = sys.argv[2]
            UDP_PORT = int(sys.argv[4])
        elif sys.argv[1] == "-p" and sys.argv[3] == "-b" and sys.argv[5] == "-n":
            PORT_SELF = int(ys.argv[4])
            UDP_IP = sys.argv[6]
            UDP_PORT = int(sys.argv[2])
        elif sys.argv[1] == "-p" and sys.argv[3] == "-n" and sys.argv[5] == "-b":
            PORT_SELF = int(sys.argv[6])
            UDP_IP = sys.argv[4]
            UDP_PORT = int(sys.argv[2])
        else:
            print("Error: Bad arguments\n")
    else:
        print("Error: Bad arguments\n")
        sys.exit(1)
    return PORT_SELF, UDP_IP, UDP_PORT
    #BS takes both UDP and TCP requests at PORT_SELF.


def writeBuffer(msg, s):
    """
    Guarantees the whole message is sent
    msg: message to be sent
    s: socket object
    """
    totalSent = 0
    msgLen = len(msg)
    try:
        # keep sending until there's nothing left
        while totalSent < msgLen:
            sent = s.send(msg[totalSent:])
            if sent == 0:
                raise RuntimeError("Error: Socket connection broken\n")
            totalSent = totalSent + sent
    except socket.error:
        print("Error: Disconnected from server\n")


def readBuffer(s):
    """
    Guarantees the whole message is read
    s: socket object

    Return: message read
    """
    firstMsg = s.recv(BUFFER_SIZE)
    try:
        # keep reeding until there's nothing left or after the socket times out
        s.settimeout(SOCKET_TIMEOUT)
        nextMsg = s.recv(BUFFER_SIZE)
        # run cycle as long as the message isn't empty ('')
        while nextMsg:
            firstMsg += nextMsg
            nextMsg = s.recv(BUFFER_SIZE)
    except socket.timeout:
        pass
    return firstMsg


def validUser(c):
    """
    Guarantees the user is valid
    c: socket object

    Return: boolean and user Id
    """
    authMsg = c.recv(BUFFER_SIZE).decode('utf-8').split(' ')
    #print(authMsg)

    if authMsg[0] != 'AUT' or len(authMsg) != 3:
        reply = "ERR\n"
        writeBuffer(reply.encode('utf-8'), c)
        return False, ''
    else:
        user = authMsg[1]
        password = authMsg[2].split('\n')[0]

        try:
            filePath = "./user_" + user + ".txt"
            f = open(filePath, "r")
            storedPassword = f.read()
            if storedPassword == password:
                authReply = "AUR OK\n"
                print("User: " + user)
                writeBuffer(authReply.encode('utf-8'), c)
                return True, user
            else:
                authReply = "AUR NOK\n"
                writeBuffer(authReply.encode('utf-8'), c)
                return False, ''

        except (OSError, IOError) as e:
            authReply = "AUR NOK\n"
            writeBuffer(authReply.encode('utf-8'), c)
            return False, ''


def tcpHandler(LOCAL_IP, PORT_SELF):
    """
    Handles the client's TCP requests
    LOCAL_IP: local IP
    PORT_SELF: self port
    """
    tcp_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_s.bind((LOCAL_IP, PORT_SELF))
    tcp_s.listen(5)

    # SIGINT handler for the TCP handler processes
    def sigHandlerChild(signum, frame):
        time.sleep(1)
        os.killpg(0, signal.SIGKILL)
        sys.exit(0)

    signal.signal(signal.SIGINT, sigHandlerChild)

    # Handle tcp requests
    while True:
        tcp_s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        c, add = tcp_s.accept()

        tcp_child_pid = os.fork()
        if tcp_child_pid == 0: # is child
            treatUserRequest(c)
        else: # wait for child processes but don't hang
            os.wait3(os.WNOHANG)


def treatUserRequest(c):
    """
    Treats the user's request
    c: socket object
    """
    isValid, user = validUser(c)
    if isValid:
        commandMsg = readBuffer(c)

        fullData = commandMsg.split()

        reqType = fullData[0].decode('utf-8')

        if reqType == "RSB":
            dirName = fullData[1].decode('utf-8')
            restoreToUser(c, dirName, user)
        elif reqType == "UPL":
            backupData(c, commandMsg, user)


def restoreToUser(c, dirName, user):
    """
    Restores a directory to the user
    c: socket object
    dirName: name of the directory to be restored
    user: user ID
    """
    dir = 'user_' + str(user) + "/" + dirName
    files = [f for f in listdir(dir) if isfile(join(dir, f))]

    s = ", ".join(files)
    messageToPrint = "Sent: " + s

    filesSize = []
    for i in range(len(files)):
        filesSize.append(os.path.getsize(dir +'/' + files[i]))

    filesDateTime = []
    for i in range(len(files)):
        filesDateTime.append(str(datetime.datetime.fromtimestamp(os.stat(dir + '/' + files[i]).st_mtime).strftime('%d.%m.%Y %H:%M:%S')))

    requestBRBMsg = "RBR " + str(len(files))

    # send the header
    writeBuffer(requestBRBMsg.encode('utf-8'), c)

    # send the files
    for i in range(len(files)):
        fileMsg = " " + files[i] + " " + filesDateTime[i] + " " + str(filesSize[i]) + " "
        writeBuffer(fileMsg.encode('utf-8'), c)
        fileName = files[i]
        fileSize = filesSize[i]

        f = open(dir + "/" + fileName, 'rb')
        fBytes = f.read(fileSize)
        writeBuffer(fBytes, c)
        f.close()

    c.send("\n".encode('utf-8'))
    print(messageToPrint)
    c.close()
    os._exit(0)

def backupData(c, commandMsg, user):
    """
    Backs up a directory sent by the client
    c: socket object
    commandMsg: the message received from the client
    user: user ID
    """
    try:
        fullData = commandMsg.split()
        dirUser = str(user)
        os.makedirs("./user_" + dirUser, exist_ok=True)

        dirName = fullData[1].decode('utf-8')
        messageToPrint = dirName + ": "
        os.makedirs("./user_" + dirUser + "/" + dirName, exist_ok=True)

        numFiles = int(fullData[2])
        fileName = fullData[3].decode('utf-8')
        fileSize = int(fullData[6])

        fileDateTime = fullData[4].decode('utf-8') + " " + fullData[5].decode('utf-8')

        newDateTime = time.mktime(datetime.datetime.strptime(fileDateTime, '%d.%m.%Y %H:%M:%S').timetuple())

        indexFileStart = 0
        for i in range(7):
            indexFileStart += len(fullData[i]) + 1

        # save first file
        with open(("./user_" + dirUser + "/" + dirName + "/" + fileName), 'wb') as myfile:
            indexFileEnd = indexFileStart+fileSize
            myfile.write(commandMsg[indexFileStart:indexFileEnd])

        restOfFile = commandMsg[(indexFileEnd+1):]
        os.utime("./user_" + dirUser + "/" + dirName + "/" + fileName, (newDateTime, newDateTime))

        messageToPrint += fileName + " " + str(fileSize) + " bytes received | "

        # save the other files, if there are more
        while numFiles > 1:
            numFiles -= 1
            partialData = restOfFile.split()

            fileName = partialData[0].decode('utf-8')
            fileSize = int(partialData[3])
            fileDateTime = partialData[1].decode('utf-8') + " " + partialData[2].decode('utf-8')

            newDateTime = time.mktime(datetime.datetime.strptime(fileDateTime, '%d.%m.%Y %H:%M:%S').timetuple())

            indexFileStart = 0
            for i in range(4):
                indexFileStart += len(partialData[i]) + 1

            with open(("./user_" + dirUser + "/" + dirName + "/" + fileName), 'wb') as myfile:
                indexFileEnd = indexFileStart+fileSize
                myfile.write(restOfFile[indexFileStart:indexFileEnd])

            messageToPrint += fileName + " " + str(fileSize) + " bytes received | "

            os.utime("./user_" + dirUser + "/" + dirName + "/" + fileName, (newDateTime, newDateTime))
            restOfFile = restOfFile[(indexFileEnd+1):]

        reply = "UPR OK\n"
        writeBuffer(reply.encode('utf-8'), c)
        print(messageToPrint)

        c.close()
        os._exit(0)

    except IndexError:
        reply = "UPR NOK\n"
        writeBuffer(reply.encode('utf-8'), c)

        c.close()
        os._exit(0)


def main():
    """Main function"""

    # SIGINT handler for the main process
    def sigHandler(signum, frame):
        msg = 'UNR ' + LOCAL_IP + ' ' + str(PORT_SELF) + '\n'
        # print(msg)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.sendto(msg.encode('utf-8'), (UDP_IP, UDP_PORT))
        s.close()
        message, address = udp_s.recvfrom(BUFFER_SIZE)
        print(message.decode('utf-8'))
        sys.exit(0)

    signal.signal(signal.SIGINT, sigHandler)
    os.setpgrp()

    PORT_SELF, UDP_IP, UDP_PORT = getArguments()
    LOCAL_IP = socket.gethostbyname(socket.gethostname())
    # This UDP_IP is the IP where CS is running so we need to use connect()

    # Open a UDP connection with CS to send the register message
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    registerMsg = "REG " + LOCAL_IP + " " + str(PORT_SELF) + "\n"

    #print(registerMsg)

    s.sendto(registerMsg.encode('utf-8'), (UDP_IP, UDP_PORT)) # TODO UDP precisa de writeBuffer?
    registerAnswer = s.recv(BUFFER_SIZE).decode('utf-8')

    print(registerAnswer)
    s.close()

    # Create child process to handle tcp requests
    pid = os.fork()
    if pid == 0: # if is child
        tcpHandler(LOCAL_IP, PORT_SELF)
    else:
        pid_father = pid

    udp_s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    udp_s.bind((LOCAL_IP, PORT_SELF))

    # handle UDP requests
    while True:
        message, address = udp_s.recvfrom(BUFFER_SIZE)
        # print(message)
        udpHandler(message, address, udp_s)

def udpHandler(message, address, s):
    """
    Handles UDP messages
    message: message from the CS
    s: socket object
    address: CS address
    """
    inputArray = message.decode('utf-8').split()

    if inputArray[0] == "LSU":
        errMsg = "LUR ERR\n"
        okMsg = "LUR OK\n"
        nokMsg = "LUR NOK\n"

        files = [f for f in listdir(".") if isfile(join(f))]
        if len(inputArray) != 3:
            s.sendto(errMsg.encode('utf-8'), address)
        elif "user_" + inputArray[1] + ".txt" not in files:
            with open("./user_" + inputArray[1] + ".txt", "w+") as f:
                f.write(inputArray[2])
                f.close
            if not os.path.exists("./user_" + inputArray[1]):
                os.makedirs("./user_" + inputArray[1])
            s.sendto(okMsg.encode('utf-8'), address)
            print('New user: ' + inputArray[1])
        elif "user_" + inputArray[1] + ".txt" in files:
            s.sendto(nokMsg.encode('utf-8'), address)

    elif inputArray[0] == "DLB":
        okMsg = "DBR OK\n"
        nokMsg = "DBR NOK\n"

        if len(inputArray) != 3:
            s.sendto(nokMsg.encode('utf-8'), address)
        elif not os.path.exists("./user_" + inputArray[1]):
            s.sendto(nokMsg.encode('utf-8'), address) #User did not get his backup folder created.
        elif os.path.exists("./user_" + inputArray[1]):
            if not os.path.exists("./user_" + inputArray[1] + "/" + inputArray[2]):
                s.sendto(nokMsg.encode('utf-8'), address) #Directory dir doesnt exist
            else: #/user_XXXXX/dir exists.
                shutil.rmtree("./user_" + inputArray[1] + "/" + inputArray[2], ignore_errors=True)
                userDirectories = glob("user_" +  inputArray[1] + "/*/")
                if len(userDirectories) == 0:
                    os.rmdir('./user_' + inputArray[1])
                    os.remove('./user_' + inputArray[1] + ".txt")

                s.sendto(okMsg.encode('utf-8'), address)
                print("Deleted directory: " + inputArray[2])

    elif inputArray[0] == "LSF":
        if len(inputArray) != 3:
            print("Error: invalid arguments")
        else:
            if os.path.exists("./user_" + inputArray[1] + "/" + inputArray[2]):
                dir = "./user_" + inputArray[1] + "/" + inputArray[2]
                files = [f for f in listdir(dir) if isfile(join(dir, f))]

                filesSize = []
                for i in range(len(files)):
                    filesSize.append(os.path.getsize(dir +'/' + files[i]))

                filesDateTime = []
                for i in range(len(files)):
                    filesDateTime.append(str(datetime.datetime.fromtimestamp(os.stat(dir + '/' + files[i]).st_mtime).strftime('%d.%m.%Y %H:%M:%S')))

                msg = "LFD " + str(len(files))
                for i in range(len(files)):
                    msg += " " + files[i] + " " + filesDateTime[i] + " " + str(filesSize[i])

                msg += "\n"
                s.sendto(msg.encode('utf-8'), address)
            else:
                msg = "LFD 0\n"
                s.sendto(msg.encode('utf-8'),address)
    else:
        print("Error: Unexpected protocol message\n")

    return


if __name__ == "__main__":
    main()
