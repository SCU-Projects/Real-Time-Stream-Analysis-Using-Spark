Steps to execute our program:

Open three terminals
1.  nc -lk 9876
2. ncat  -u -l 8880 --sh-exec "nc -lk 9876"
3.  java -jar assignment4-data-generator.jar --destIPaddress 127.0.0.1 --destPortNumber 8880 --transmissionTime 30  --transmissionRate 100

This bridges tcp port and udp port and converts the tcp to udp.

3rd terminal is started after starting the program.

Output is generated to output file under resources folder.