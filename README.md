# Computer Networks - RC Cloud Backup
School Project for Computer Networks.

## Project

The project is implemented in Python3.
The makefile only contains instructions to grant execute access to the user.
After running the make file, the files can be ran like any other script:
  $./<script_name>

All 3 scripts, right at the top of the file, have a global variable named SOCKET_TIMEOUT.
This variable is the number of seconds before the TCP socket connections time out.

There's a description of the implemented protocol, but it's in portuguese (contact me if you want it)

WARNING:
The BS servers details connected to the CS are stored in files in the directory of the CS script.
Therefore, whenever the CS is started, it will delete all files in its directory except files that have names starting with 'user', 'CS', 'BS', 'make', 'readme' or 'proj'.

Only runs in UNIX based OS since we use fork().

### Authors

João Coelho\
Rui Alves\
Manuel Rego
