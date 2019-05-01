#!/usr/bin/env python3

import socket
import sys
import time
import os
from os import listdir
from os.path import isdir, isfile, join
import datetime


BUFFER_SIZE = 1024
SOCKET_TIMEOUT = 2


def getArguments():
    """
    Read the script's arguments

    Return: CS port and IP
    """
    if len(sys.argv) == 1:
        TCP_IP = socket.gethostbyname(socket.gethostname())
        TCP_PORT = 58042
    elif len(sys.argv) == 3:
        # only CSname as argument
        if sys.argv[1] == "-n":
            TCP_IP = sys.argv[2]
            TCP_PORT = 58042
        # only CSport as argument
        elif sys.argv[1] == "-p":
            TCP_IP = socket.gethostbyname(socket.gethostname())
            TCP_PORT = int(sys.argv[2])
        else:
            print("Error: Bad argument\n")
            sys.exit(1)
    elif len(sys.argv) == 5:
        if sys.argv[1] == "-n" and sys.argv[3] == "-p":
            TCP_IP = sys.argv[2]
            TCP_PORT = int(sys.argv[4])
        elif sys.argv[1] == "-p" and sys.argv[3] == "-n":
            TCP_PORT = int(sys.argv[2])
            TCP_IP = sys.argv[4]
        else:
            print("Error: Bad arguments\n")
            sys.exit(1)
    else:
        print("Error: Bad arguments\n")
        sys.exit(1)
    return TCP_IP, TCP_PORT


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
                print("Error: Socket connection broken\n")
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


def validLogin(inputArray):
    """
    Validates login
    inputArray: user input

    Return: boolean
    """
    if (len(inputArray[1]) == 5 and len(inputArray[2]) == 8):
        try:
            # check if the first argument is a number
            int(inputArray[1])
            return True
        except ValueError:
            print("Error: Username must be a 5 digit number\n")
            return False
    else:
        print("Error: Incorrect argument size\n")
        return False


def authenticate(s, user, password):
    """
    Authenticate user
    s: socket object
    user: user ID
    password: user password

    Return: authentication reply
    """
    authMsg = "AUT " + user + " " + password + "\n"
    writeBuffer(authMsg.encode('utf-8'), s)
    reply = s.recv(BUFFER_SIZE)
    return reply.decode('utf-8')


def validAuth(authReply):
    """
    Validates authentication reply
    authReply: authentication reply

    Return: boolean
    """
    authStatus = authReply.split(' ')
    if authStatus[0] == "ERR\n":
        print("Error: Unexpected protocol message\n")
        return False
    elif authStatus[1] == "NOK\n":
        print("Error: Authentication failed\n")
        return False
    else:
        return True


def requestDelUser(s):
    """
    Sends message to delete user
    s: socket object

    Return: server reply
    """
    delUserMsg = "DLU\n"
    writeBuffer(delUserMsg.encode('utf-8'), s)
    return readBuffer(s).decode('utf-8')


def validDelUser(requestDelUserReply):
    """
    Validates delete user reply
    requestDelUserReply: request delete user reply

    Return: boolean
    """
    delUserStatus = requestDelUserReply.split(' ')
    if delUserStatus[0] == "ERR\n":
        print("Error: Unexpected protocol message\n")
        return False
    elif delUserStatus[1] == "NOK\n":
        print("Error: Delete user failed\n")
        return False
    else:
        return True


def requestDirList(s):
    """
    Sends message to list directories
    s: socket object

    Return: server reply
    """
    dirListMsg = "LSD\n"
    writeBuffer(dirListMsg.encode('utf-8'), s)
    return readBuffer(s).decode('utf-8')


def requestFileList(s, dir):
    """
    Sends message to list files in a directory
    s: socket object
    dir: target directory

    Return: server reply
    """
    fileListMsg = "LSF " + dir + "\n"
    writeBuffer(fileListMsg.encode('utf-8'), s)
    return readBuffer(s).decode('utf-8')


def requestDelDir(s, dir):
    """
    Sends message to delete a directory
    s: socket object
    dir: target directory

    Return: server reply
    """
    delDirMsg = "DEL " + dir + "\n"
    writeBuffer(delDirMsg.encode('utf-8'), s)
    return readBuffer(s).decode('utf-8')


def requestRestore(s, dir):
    """
    Sends message to restore a directory (CS)
    s: socket object
    dir: target directory

    Return: server reply
    """
    requestMsg = "RST " + dir + "\n"
    writeBuffer(requestMsg.encode('utf-8'), s)
    return readBuffer(s).decode('utf-8')


def requestRestoreFromBS(s, dir):
    """
    Sends message to delete a directory (BS)
    s: socket object
    dir: target directory

    Return: server reply
    """
    requestMsgBS = "RSB " + dir + "\n"
    writeBuffer(requestMsgBS.encode('utf-8'), s)
    # no decode on this message, as it has the file bytes, not just message strings
    return readBuffer(s)


def validRestoreCS(requestRestoreReply):
    """
    Validates restore directory reply (CS)
    requestRestoreReply: request restore directory reply

    Return: boolean
    """
    requestRestoreStatus = requestRestoreReply.split(' ')
    if requestRestoreStatus[0] == "ERR\n":
        print("Error: Unexpected protocol message\n")
        return False
    elif requestRestoreStatus[1] == "EOF\n":
        print("Error: Couldn't reach the backup server\n")
        return False
    elif requestRestoreStatus[1] == "ERR\n":
        print("Error: Restore request not correctly formulated\n")
        return False
    else:
        return True


def validRestoreBS(BSreply):
    """
    Validates restore directory reply (BS)
    BSreply: request restore directory reply

    Return: boolean
    """
    BSrestoreStatus = BSreply.split()
    if BSrestoreStatus[0] == "ERR\n":
        print("Error: Unexpected protocol message\n")
        return False
    elif BSrestoreStatus[1] == "EOF\n":
        print("Error: Couldn't reach the backup server or directory not found\n")
        return False
    elif BSrestoreStatus[1] == "ERR\n":
        print("Error: Restore request not correctly formulated\n")
        return False
    else:
        return True


def requestBackup(s, dir):
    """
    Sends message to backup a directory (CS)
    s: socket object
    dir: target directory

    Return: Boolean, server reply
    """
    files = [f for f in listdir(dir) if isfile(join(dir, f))]

    # if the directory is empty
    if len(files) == 0:
        return False, "Error: Trying to upload an empty directory"

    filesSize = []
    for i in range(len(files)):
        filesSize.append(os.path.getsize(dir +'/' + files[i]))

    filesDateTime = []
    for i in range(len(files)):
        filesDateTime.append(str(datetime.datetime.fromtimestamp(os.stat(dir + '/' + files[i]).st_mtime).strftime('%d.%m.%Y %H:%M:%S')))

    requestBKMsg = "BCK " + dir + " " + str(len(files))
    for i in range(len(files)):
        requestBKMsg += " " + files[i] + " " + filesDateTime[i] + " " + str(filesSize[i])

    requestBKMsg += "\n"
    writeBuffer(requestBKMsg.encode('utf-8'), s)
    return True, readBuffer(s).decode('utf-8')


def requestBackupFromBS(s, dir, BSinfoArray):
    """
    Sends message to backup a directory (BS)
    s: socket object
    dir: target directory
    BSinfoArray: array with the info of what files should be backed up

    Return: server reply
    """
    numFiles = int(BSinfoArray[0])
    BSinfoArray[-1] = BSinfoArray[-1][:-1] # remove \n at the end of the message
    requestBSBKMsg = "UPL " + dir + " " + str(numFiles)

    writeBuffer(requestBSBKMsg.encode('utf-8'), s)

    for i in range(numFiles):
        fileIndex = 4 * i
        fileHeaderMsg = " " + BSinfoArray[fileIndex+1] +  \
        " " + BSinfoArray[fileIndex+2] + \
        " " + BSinfoArray[fileIndex+3] + \
        " " + BSinfoArray[fileIndex+4] + " "

        writeBuffer(fileHeaderMsg.encode('utf-8'), s)

        fileName = BSinfoArray[fileIndex+1]
        fileSize = int(BSinfoArray[fileIndex+4])

        # send files
        f = open(dir + "/" + fileName, 'rb')
        fBytes = f.read(fileSize)
        writeBuffer(fBytes, s)
        f.close()

    writeBuffer("\n".encode('utf-8'), s)
    return readBuffer(s).decode('utf-8')


def dirExists(dir):
    """Checks if directory dir"""
    return os.path.isdir("./" + dir)


def validBackupCS(requestBackupReply):
    """
    Validates back up directory reply (CS)
    requestBackupReply: request back up directory reply

    Return: boolean
    """
    requestBackupStatus = requestBackupReply.split(' ')
    if requestBackupStatus[0] == "ERR\n":
        print("Error: Unexpected protocol message\n")
        return False
    elif requestBackupStatus[1] == "EOF\n":
        print("Error: Could not reach the backup server\n")
        return False
    elif requestBackupStatus[1] == "ERR\n":
        print("Error: Request not correctly formulated\n")
        return False
    elif requestBackupStatus[3] == "0\n":
        print("Backup aborted. No new files to upload\n")
        return False
    else:
        return True


def main():
    """Main"""
    TCP_IP, TCP_PORT = getArguments()
    loggedIn = False
    user = ''
    password = ''

    # wait for and treat user commands
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((TCP_IP, TCP_PORT))

        userInput = input()
        inputArray = userInput.split(' ')

        if '' in inputArray:
            print("Error: Incorrect input\n")
        elif inputArray[0] == "login":
            if len(inputArray) != 3:
                print("Error: Login takes 2 arguments\n")
            elif loggedIn:
                print("Error: Already logged in as user " + user + "\n")
            else:
                if validLogin(inputArray):
                    authReply = authenticate(s, inputArray[1], inputArray[2])
                    # print(authReply)
                    authStatus = authReply.split(' ')
                    if authStatus[0] == "ERR\n":
                        print("Error: Unexpected protocol message\n")
                    elif authStatus[1] == "NOK\n":
                        print("Error: Login failed\n")
                    elif authStatus[1] == "OK\n":
                        loggedIn = True
                        user = inputArray[1]
                        password = inputArray[2]
                        print("Successfully logged in\n")
                    elif authStatus[1] == "NEW\n":
                        loggedIn = True
                        user = inputArray[1]
                        password = inputArray[2]
                        print('User "' + user + '" created')
                        print("Successfully logged in\n")

        elif inputArray[0] == "deluser":
            if len(inputArray) > 1:
                print("Error: Command deluser takes no arguments\n")
            elif not loggedIn:
                print("Error: You are not logged in\n")
            else:
                authReply = authenticate(s, user, password)
                # print(authReply)
                if validAuth(authReply):
                    requestDelUserReply = requestDelUser(s)
                    # print(requestDelUserReply)
                    if validDelUser(requestDelUserReply):
                        loggedIn = False
                        print("Successfully deleted user and logged out\n")
                    else:
                        print("Error: Could not log out\n")

        elif inputArray[0] == "dirlist":
            if len(inputArray) > 1:
                print("Error: Command dirlist takes no arguments\n")
            elif not loggedIn:
                print("Error: You are not logged in\n")
            else:
                authReply = authenticate(s, user, password)
                # print(authReply)
                if validAuth(authReply):
                    requestDirListReply = requestDirList(s)
                    # print(requestDirListReply)
                    numFolders = int(requestDirListReply.split(' ')[1])
                    if numFolders > 0:
                        folders = requestDirListReply.split(' ')[2:]
                        print(' '.join(folders))
                    else:
                        print("No directories uploaded\n")
                    if requestDirListReply.split(' ')[0] == "ERR\n":
                        print("Error: Unexpected protocol message\n")

        elif inputArray[0] == "filelist":
            if len(inputArray) != 2:
                print("Error: Command filelist takes 1 argument\n")
            elif not loggedIn:
                print("Error: You are not logged in\n")
            else:
                if len(inputArray[1]) > 19:
                    print("Error: Invalid directory name\n")
                else:
                    authReply = authenticate(s, user, password)
                    # print(authReply)
                    if validAuth(authReply):
                        requestFileListReply = requestFileList(s, inputArray[1])
                        # print(requestFileListReply)
                        fileListStatus = requestFileListReply.split(' ')
                        if fileListStatus[0] == "ERR\n":
                            print("Error: Unexpected protocol message\n")
                        elif fileListStatus[1] == "NOK\n":
                            print("Error: Could not list files\n")
                        else:
                            numFiles = int(fileListStatus[3])
                            listNames = ''
                            for i in range(numFiles):
                                listNames += fileListStatus[3*i+4] + " "
                            print("Files in directory " + inputArray[1] + ": " + listNames)

        elif inputArray[0] == "backup":
            if len(inputArray) != 2:
                print("Error: Command backup takes 1 argument\n")
            elif not loggedIn:
                print("Error: You are not logged in\n")
            elif not dirExists(inputArray[1]):
                print("Error: Directory doesn't exist\n")
            else:
                if len(inputArray[1]) > 19:
                    print("Error: Invalid directory name\n")
                else:
                    authReply = authenticate(s, user, password)
                    # print(authReply)
                    if validAuth(authReply):
                        validBackup, requestBackupReply = requestBackup(s, inputArray[1])

                        if not validBackup:
                            print(requestBackupReply)
                        elif validBackupCS(requestBackupReply):
                            # print(requestBackupReply)
                            BSinfoArray = requestBackupReply.split(' ')
                            BS_TCP_IP = BSinfoArray[1]
                            BS_TCP_PORT = int(BSinfoArray[2])

                            s.close()

                            # connect to the BS
                            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            s.connect((BS_TCP_IP, BS_TCP_PORT))

                            authReply = authenticate(s, user, password)
                            # print(authReply)
                            if validAuth(authReply):
                                BSreply = requestBackupFromBS(s, inputArray[1], BSinfoArray[3:])
                                # print(BSreply)

                                numUploaded = int(requestBackupReply.split(' ')[3])
                                dirUploaded = inputArray[1]
                                fileNames = ''
                                for i in range(numUploaded):
                                    fileNames += requestBackupReply.split(' ')[4*i+4] + " "

                                print('completed - ' + dirUploaded + ": " + fileNames)
                                BSreplyStatus = BSreply.split(' ')
                                if BSreply[0] == "ERR\n":
                                    print("Error: Unexpected protocol message\n")
                                elif BSreply[1] == "NOK\n":
                                    print("Error: Could not upload the files\n")

        elif inputArray[0] == "restore":
            if len(inputArray) != 2:
                print("Error: Command restore takes 1 argument\n")
            elif not loggedIn:
                print("Error: You are not logged in\n")
            else:
                dirName = inputArray[1]
                authReply = authenticate(s, user, password)
                # print(authReply)
                if validAuth(authReply):
                    requestRestoreReply = requestRestore(s, dirName)
                    # print(requestRestoreReply)

                    if validRestoreCS(requestRestoreReply):
                        BSinfoArray = requestRestoreReply.split(' ')
                        BS_TCP_IP = BSinfoArray[1]
                        BS_TCP_PORT = int(BSinfoArray[2])

                        s.close()

                        # connect to the BS
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                        BS_up = True
                        try:
                            s.connect((BS_TCP_IP, BS_TCP_PORT))
                        except ConnectionRefusedError: # BS that holds the folder is unreachable
                            BS_up  = False

                        if not BS_up:
                            print('Error: Backup server not available\n')
                        else:
                            authReply = authenticate(s, user, password)
                            # print(authReply)
                            if validAuth(authReply):
                                BSreply = requestRestoreFromBS(s, dirName)
                                if validRestoreBS(BSreply):
                                    listNames = ''
                                    fullData = BSreply.split()
                                    numFiles = int(fullData[1])
                                    fileName = fullData[2].decode('utf-8')
                                    listNames += fileName + ' '
                                    fileDateTime = fullData[3].decode('utf-8') + " " + fullData[4].decode('utf-8')
                                    fileSize = int(fullData[5])

                                    newDateTime = time.mktime(datetime.datetime.strptime(fileDateTime, '%d.%m.%Y %H:%M:%S').timetuple())

                                    os.makedirs("./" + dirName, exist_ok=True)

                                    indexFileStart = 0
                                    for i in range(6):
                                        indexFileStart += len(fullData[i]) + 1

                                    # restore first file
                                    with open(("./" + dirName + "/" + fileName), 'wb') as myfile:
                                        indexFileEnd = indexFileStart+fileSize
                                        myfile.write(BSreply[indexFileStart:indexFileEnd])

                                    # set the restored file "true date"
                                    os.utime("./" + dirName + "/" + fileName, (newDateTime,newDateTime))

                                    restOfFile = BSreply[(indexFileEnd+1):]

                                    # restore the other files, if there are any
                                    while numFiles > 1:
                                        numFiles -= 1
                                        # restOfFile = restOfFile[(indexFileEnd+1):]
                                        partialData = restOfFile.split()

                                        fileName = partialData[0].decode('utf-8')
                                        listNames += fileName + ' '
                                        fileDateTime = partialData[1].decode('utf-8') + " " + partialData[2].decode('utf-8')
                                        fileSize = int(partialData[3])

                                        newDateTime = time.mktime(datetime.datetime.strptime(fileDateTime, '%d.%m.%Y %H:%M:%S').timetuple())

                                        indexFileStart = 0
                                        for i in range(4):
                                            indexFileStart += len(partialData[i]) + 1

                                        with open(("./" + dirName + "/" + fileName), 'wb') as myfile:
                                            indexFileEnd = indexFileStart+fileSize
                                            myfile.write(restOfFile[indexFileStart:indexFileEnd])

                                        # set the restored file "true date"
                                        os.utime("./" + dirName + "/" + fileName, (newDateTime,newDateTime))
                                        restOfFile = restOfFile[(indexFileEnd+1):]
                                    print('success - ' + dirName + ": " + listNames)

        elif inputArray[0] == "delete":
            if len(inputArray) != 2:
                print("Error: Command delete takes 1 argument\n")
            elif not loggedIn:
                print("Error: You are not logged in\n")
            else:
                if len(inputArray[1]) > 19:
                    print("Error: Invalid directory name\n")
                else:
                    authReply = authenticate(s, user, password)
                    # print(authReply)

                    if validAuth(authReply):
                        requestDelDirReply = requestDelDir(s, inputArray[1])
                        # print(requestDelDirReply)
                        delDirStatus = requestDelDirReply.split(' ')
                        if delDirStatus[0] == "ERR\n":
                            print("Error: Unexpected protocol message\n")
                        elif delDirStatus[1] == "NOK\n":
                            print("Error: Could not delete directory " + inputArray[1])
                        else:
                            print('Deleted directory ' + inputArray[1])

        elif inputArray[0] == "logout":
            if len(inputArray) > 1:
                print("Error: Command logout takes no arguments\n")
            elif not loggedIn:
                print("Error: You are not logged in\n")
            else:
                loggedIn = False
                print(user + " logged out\n")

        elif inputArray[0] == "exit":
            if len(inputArray) > 1:
                print("Error: Command exit takes no arguments\n")
            else:
                loggedIn = False
                print("Exiting application\n")
                s.close()
                sys.exit(0)

        else:
            print("Error: Invalid command\n")

        s.close()


if __name__ == "__main__":
    main()
