#!/usr/bin/env python3

import socket
import sys
import os
import signal
import select
from glob import glob
import random
import shutil
from os.path import isdir, isfile, join
from os import listdir
import datetime


BUFFER_SIZE = 1024
SOCKET_TIMEOUT = 2


def writeBuffer(msg, s):
    """
    Guarantees the whole message is sent
    msg: message to be sent
    s: socket object
    """
    totalSent = 0
    msgLen = len(msg)
    try:
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
        s.settimeout(SOCKET_TIMEOUT)
        nextMsg = s.recv(BUFFER_SIZE)
        # run cycle as long as the message isn't empty ('')
        while nextMsg:
            firstMsg += nextMsg
            nextMsg = s.recv(BUFFER_SIZE)
    except socket.timeout:
        pass
    return firstMsg


def getArguments():
    """
    Read the script's arguments

    Return: self port
    """
    if len(sys.argv) == 1:
        PORT_SELF = 58042
    elif len(sys.argv) == 3:
        if sys.argv[1] == "-p":
            PORT_SELF = int(sys.argv[2])
        else:
            print("Error: Bad argument\n")
            sys.exit(1)
    else:
        print("Error: Bad arguments\n")
        sys.exit(1)
    return PORT_SELF


def tcpHandler(LOCAL_IP, PORT_SELF):
    """
    Handles the TCP requets from the user, forking for each new one.
    LOCAL_IP: IP from where the CS is running
    PORT_SELF: Port where the CS takes TCP requests
    """
    tcp_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_s.bind((LOCAL_IP, PORT_SELF))
    tcp_s.listen(5)

    while True:
        tcp_s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        c, add = tcp_s.accept()

        tcp_child_pid = os.fork()
        if tcp_child_pid == 0: # TCP child waiting for the USER
            treatUserRequest(c)
            c.close()
            os._exit(0)
        else:
            os.wait3(os.WNOHANG)


def treatUserRequest(c):
    """
    Treats the message received from a user
    c: socket
    """
    user, password, isValid = User(c)
    if isValid:
        commandMsg = readBuffer(c)
        if commandMsg:

            fullData = commandMsg.split()

            reqType = fullData[0].decode('utf-8')
            if reqType == "DLU":
                okMsg = "DLR OK\n"
                nokMsg = "DLR NOK\n"
                if len(glob("user_" +  user + "/*/")) == 0: #user has no directories, can be deleted
                    #We dont need to send anything to the BS since BS deletes the user folder when there are no more directories from that user.
                    os.rmdir('./user_' + user)
                    os.remove('./user_' + user + ".txt")
                    writeBuffer(okMsg.encode('utf-8'), c)
                    print('Deleted user ' + user)
                else:#There are still some backed up diretories, cant be deleted
                    writeBuffer(nokMsg.encode('utf-8'), c)

            elif reqType == "BCK":
                dir = fullData[1].decode('utf-8')

                userBSs = list()
                if os.path.exists("./user_" + user + "/" + dir): #User already backed up a version of the directory he's trying to backup now. The BS will be the one before.
                    chosenBackup =  [f for f in listdir("./user_" + user + "/" + dir) if isfile(join("./user_" + user + "/" + dir, f))][0]
                    chosenBackupIP = os.path.splitext(chosenBackup)[0]

                    f = open("./user_" + user + "/" + dir + "/" + chosenBackup, "r")
                    chosenBackupPort = f.read()
                    f.close()
                else: #The directory has no previous versions so a random BS will be chosen from the list.
                    backupServers = [f for f in listdir('.') if isfile(join('.', f)) and f.endswith('.bs')]
                    random.seed(datetime.datetime.now())
                    try:
                        chosenBackup = random.choice(backupServers)
                    except IndexError:
                        print('No active BS\n')
                        eofMsg = "BKR EOF\n"
                        writeBuffer(eofMsg.encode('utf-8'),c)
                        c.close()
                        os._exit(0)
                        return

                    chosenBackupIP = chosenBackup.split('_')[0]

                    f = open(chosenBackup, "r")
                    chosenBackupPort = f.read()
                    f.close()

                #Getting a list (userBSs) of all the Backup Servers where the user has directories
                userDirectories = glob("user_" +  user + "/*/")

                print("BCK " + user + " " + dir + " " + chosenBackupIP + " " + chosenBackupPort)

                for i in range(len(userDirectories)):
                    for f in listdir(userDirectories[i]):
                        if isfile(join(userDirectories[i],f)):
                            userBSs.append(f)


                if chosenBackupIP + ".bs" in userBSs: #The user is not new to the chosen BS
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.connect((chosenBackupIP, int(chosenBackupPort)))
                    msg = "LSF " + user + " " + dir + '\n'
                    s.send(msg.encode('utf-8'))
                    answer = s.recv(BUFFER_SIZE).decode('utf-8')
                    s.close()


                    parts_1 = answer.split()

                    if parts_1[1] == "NOK":
                        eofMsg = "BKR EOF\n"
                        writeBuffer(eofMsg.encode('utf-8'),c)
                        c.close()
                        os._exit(0)
                        return

                    backedUpFiles = []
                    backedUpSize = []
                    backedUpDate = []
                    backedUpTime = []

                    if parts_1[1] != "0": #Making arrays that have the description of all the backedup files
                        for i in range(int(parts_1[1])):
                            backedUpFiles.append(parts_1[4*i+2])
                            backedUpSize.append(parts_1[4*i+5])
                            backedUpDate.append(parts_1[4*i+3])
                            backedUpTime.append(parts_1[4*i+4])

                    backedUpDateTime = []
                    for i in range(len(backedUpDate)):
                        s = backedUpDate[i] + " " + backedUpTime[i]
                        backedUpDateTime.append(s)


                    parts_2 = commandMsg.decode('utf-8').split()

                    filesToBackUp = []
                    filesDate = []
                    filesTime = []
                    filesSize = []

                    for i in range(int(parts_2[2])): #Making arrays that have the description of the files to back up
                        filesToBackUp.append(parts_2[4*i+3])
                        filesDate.append(parts_2[4*i+4])
                        filesTime.append(parts_2[4*i+5])
                        filesSize.append(parts_2[4*i+6])

                    filesDateTime = []
                    for i in range(len(filesTime)):
                        s = filesDate[i] + " " + filesTime[i]
                        filesDateTime.append(s)


                    msg = "BKR " + chosenBackupIP + " " + chosenBackupPort + " "
                    n = 0
                    for i in filesToBackUp:
                        if i in backedUpFiles:
                            n=n+1
                    N = len(filesToBackUp) - n

                    for i in range(len(filesToBackUp)): #Compare the previously mentioned arrays to get the number of files to backup (N)
                        for j in range(len(backedUpFiles)):
                            if filesToBackUp[i] == backedUpFiles[j]:
                                if (filesSize[i] !=  backedUpSize[j]) or (filesDateTime[i] != backedUpDateTime[j]):
                                    N=N+1

                    msg += str(N)

                    for i in range(len(filesToBackUp)): #Get the files that are going to be backed up because they are new
                        if filesToBackUp[i] not in backedUpFiles:
                            msg += " " + filesToBackUp[i] + " " + filesDate[i] + " " +filesTime[i] + " " + filesSize[i]



                    for i in range(len(filesToBackUp)): #Get the files that are going to be backed up because they were changed
                        for j in range(len(backedUpFiles)):
                            if filesToBackUp[i] == backedUpFiles[j]:
                                if (filesSize[i] !=  backedUpSize[j]) or (filesDateTime[i] != backedUpDateTime[j]):
                                    msg += " " + filesToBackUp[i] + " " + filesDate[i] + " " +filesTime[i] + " " + filesSize[i]



                    msg += "\n"

                    if parts_1[1] == '0':
                        os.makedirs("user_" + user + "/" + dir, exist_ok=True)
                        f = open("user_" + user + "/" + dir + "/" + chosenBackupIP + ".bs", "w+")
                        f.write(chosenBackupPort)
                        f.close()

                    writeBuffer(msg.encode('utf-8'), c)


                else: #The user is new to the chosen BS
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.connect((chosenBackupIP, int(chosenBackupPort)))
                    msg = "LSU " + user + " " + password
                    s.send(msg.encode('utf-8'))
                    answer = s.recv(BUFFER_SIZE).decode('utf-8')

                    s.close()
                    if answer == "LUR OK\n":
                        #userlist updated, i can send the files.
                        ignoreLen = len(reqType) + 1 + len(dir) + 1
                        msg = "BKR " + chosenBackupIP + " " + chosenBackupPort + " " + commandMsg[ignoreLen:].decode('utf-8')
                        writeBuffer(msg.encode('utf-8'), c)
                        os.makedirs("user_" + user + "/" + dir, exist_ok=True)
                        f = open("user_" + user + "/" + dir + "/" + chosenBackupIP + ".bs", "w+")
                        f.write(chosenBackupPort)
                        f.close()
                    else: #answer = NOK or ERR
                        eofMsg = "BKR EOF\n"
                        writeBuffer(eofMsg.encode('utf-8'),c)

            elif reqType == "RST":
                userDirectories = glob("user_" +  user + "/*/")
                dir = fullData[1].decode('utf-8')

                for i in range(len(userDirectories)):
                    userDirectories[i] = userDirectories[i][11:-1]

                if dir in userDirectories: #directory is backedup
                    BS = os.listdir("user_" + user +"/" + dir)[0]
                    backupIP = os.path.splitext(BS)[0]
                    f = open("user_" + user +"/" + dir + "/" + BS, "r")
                    backupPort = f.read()
                    f.close()

                    msg = "RSR " + backupIP + " " + backupPort + "\n"

                    writeBuffer(msg.encode('utf-8'), c)
                    print("Restore " + dir)

                else:
                    msg = "RSR EOF\n"
                    writeBuffer(msg.encode('utf-8'), c)

            elif reqType == "DEL":
                userDirectories = glob("user_" +  user + "/*/")
                dir = fullData[1].decode('utf-8')

                for i in range(len(userDirectories)):
                    userDirectories[i] = userDirectories[i][11:-1]

                if dir in userDirectories: #directory is backedup
                    BS = os.listdir("user_" + user +"/" + dir)[0]
                    BS_IP = os.path.splitext(BS)[0]
                    f = open("user_" + user +"/" + dir + "/" + BS, "r")
                    BS_PORT = int(f.read())
                    f.close()

                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    msg = "DLB " + user + " " + dir + '\n'
                    s.sendto(msg.encode('utf-8'), (BS_IP, BS_PORT))
                    answer = s.recv(BUFFER_SIZE).decode('utf-8')
                    s.close()

                    if answer == "DBR OK\n":
                        msg = "DDR OK\n"
                        shutil.rmtree("./user_" + user + "/" + dir, ignore_errors=True)
                        writeBuffer(msg.encode('utf-8'),c)
                        print('Deleted directory ' + dir)

                    else: #answer fom BS was NOK or ERR
                        msg = "DDR NOK\n"
                        writeBuffer(msg.encode('utf-8'),c)

                else:#Directory not backed up
                    msg = "DDR NOK\n"
                    writeBuffer(msg.encode('utf-8'),c)

            elif reqType == "LSD":
                userDirectories = glob("user_" +  user + "/*/")
                msg_aux = ""
                n = 0
                for x in userDirectories:
                    msg_aux += x.split('/')[-2] + " "
                    n += 1
                msg = "LDR " + str(n) + " " + msg_aux + "\n"
                writeBuffer(msg.encode('utf-8'),c)
                print('List directories request')

            elif reqType == "LSF":
                userDirectories = glob("user_" +  user + "/*/")
                dir = fullData[1].decode('utf-8')
                globDir = "user_" + user + "/" + dir + "/"
                if globDir in userDirectories:

                    pathDir = "./" + globDir
                    files = [f for f in listdir(pathDir) if isfile(join(pathDir, f))]
                    BS_IP = files[0].split('.bs')[0]

                    f = open(("./" + globDir + files[0]), "r")
                    BS_PORT = int(f.readline())
                    f.close()

                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    msg = "LSF " + user + " " + dir + '\n'
                    BS_up = True
                    try:
                        s.connect((BS_IP, BS_PORT))
                    except ConnectionRefusedError:
                        BS_up = False

                    if not BS_up:
                        msg = "LFD NOK\n"
                        writeBuffer(msg.encode('utf-8'),c)

                    else:
                        s.send(msg.encode('utf-8'))
                        answer = s.recv(BUFFER_SIZE).decode('utf-8')
                        s.close()

                        answer_split = answer.split()

                        msg_aux = ''
                        for i in range(int(answer_split[1])):
                            msg_aux += answer_split[4*i+2] + " "
                            msg_aux += answer_split[4*i+3] + " "
                            msg_aux += answer_split[4*i+4] + " "

                        n = answer_split[1]
                        msg_aux += "\n"
                        msg = "LFD " + BS_IP + " " + str(BS_PORT) + " " + n + " " + msg_aux
                        writeBuffer(msg.encode('utf-8'),c)
                        print('List files in directory ' + dir + ' request')

                else: #unexpected protocol Message
                    msg = "LFD NOK\n"
                    writeBuffer(msg.encode('utf-8'),c)

            else: #unexpected protocol Message
                msg = "ERR\n"
                writeBuffer(msg.encode('utf-8'),c)
                print("Error: Unexpected protocol message\n")

    c.close()
    os._exit(0)


def User(c):
    """
    Checks if the login was successful.
    c: socket

    Returns: username, password, and if the login was successful (Boolean)
    """
    auth = readBuffer(c)

    authMsg = auth.decode('utf-8').split(' ')

    if authMsg[0] != 'AUT' or len(authMsg) != 3:
        reply = "ERR\n"
        writeBuffer(reply.encode('utf-8'), c)
        return "","",False
    else:
        user = authMsg[1]
        password = authMsg[2]

        try:
            filePath = "./user_" + user + ".txt"
            if os.path.exists(filePath):
                f = open(filePath, "r")
                storedPassword = f.read()
                if storedPassword == password:
                    authReply = "AUR OK\n"
                    print('User: ' + user)
                    writeBuffer(authReply.encode('utf-8'), c)
                    return user, password, True
                else:
                    authReply = "AUR NOK\n"
                    writeBuffer(authReply.encode('utf-8'), c)
                    return "", "", False
            else:#newUser
                with open(filePath, "w+") as f:
                    f.write(password)
                    f.close()
                os.makedirs("user_" + user) #FOLDER FOR FURTURE BACKUPS
                authReply = "AUR NEW\n"
                writeBuffer(authReply.encode('utf-8'), c)
                print('New user: ' + user)
                return user, password, False

        except (OSError, IOError) as e:
            authReply = "AUR NOK\n"
            writeBuffer(authReply.encode('utf-8'), c)
            return "", "", False


def cleanBSregist():
    """Clears all the files that provide information about backup servers"""
    my_dir = "./"
    for fname in os.listdir(my_dir):
        if not fname.startswith("user") and not fname.startswith("CS") and not fname.startswith("BS") and not fname.startswith("make") and not fname.startswith("readme") and not fname.startswith("proj"):
            os.remove(os.path.join(my_dir, fname))


def main():
    """Main"""
    cleanBSregist()

    add = socket.gethostbyname(socket.gethostname())

    PORT_SELF = getArguments()
    LOCAL_IP = socket.gethostbyname(socket.gethostname())

    tcp_first_child_pid = os.fork()
    if tcp_first_child_pid == 0: # TCP child waiting for the USER
        tcpHandler(LOCAL_IP, PORT_SELF)

    udp_s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

    udp_s.bind((LOCAL_IP, PORT_SELF))

    while True:
        message, address = udp_s.recvfrom(BUFFER_SIZE)


        inputArray = message.decode('utf-8').split()

        if inputArray[0] == "REG":
            okMsg = "RGR OK"
            nokMsg = "RGR NOK"
            errMsg = "RGR ERR"
            with open(inputArray[1] + "_" + inputArray[2] + ".bs", "w+") as f:
                f.write(inputArray[2])
                f.close
            udp_s.sendto(okMsg.encode('utf-8'), address)
            print("+BS: " + inputArray[1] + " " + inputArray[2] + "\n")

        elif inputArray[0] == "UNR":
            cmd = message.decode('utf-8').split(' ')
            BS_IP_down = cmd[1]
            BS_PORT_down = cmd[2].split('\n')[0]
            okMsg = "UAR OK"
            nokMsg = "UAR NOK"
            errMsg = "UAR ERR"
            if isfile(BS_IP_down + '_' + BS_PORT_down + ".bs"):
                os.remove(BS_IP_down + '_' + BS_PORT_down + ".bs")
                udp_s.sendto(okMsg.encode('utf-8'), address)
                print("-BS: " + BS_IP_down + " " + BS_PORT_down + "\n")
            else:
                udp_s.sendto(nokMsg.encode('utf-8'), address)


if __name__ == "__main__":
    main()
