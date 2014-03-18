CC = gcc
INCLUDES = -I .
CFLAGS = -Wall -g 
LIBS = -lzmq -lczmq
MAIN = broker discovery be_test name_server

.PHONY: clean

all:    $(MAIN)

broker: broker.o
	$(CC) $(CFLAGS) -o broker broker.o $(LIBS)
discovery: discovery.o
	$(CC) $(CFLAGS) -o discovery discovery.o $(LIBS)
be_test: be_test.o
	$(CC) $(CFLAGS) -o be_test be_test.o $(LIBS)
name_server: name_server.o
	$(CC) $(CFLAGS) -o name_server name_server.o $(LIBS)

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<  -o $@

clean:
	$(RM) *.o *~ $(MAIN)
