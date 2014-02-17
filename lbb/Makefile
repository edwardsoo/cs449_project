CC = gcc
INCLUDES = -I .
CFLAGS = -Wall -g 
LIBS = -lzmq -lczmq
MAIN = client worker broker

.PHONY: clean

all:    $(MAIN)

client: client.o
	$(CC) $(CFLAGS) -o client client.o $(LIBS)
worker: worker.o
	$(CC) $(CFLAGS) -o worker worker.o $(LIBS)
broker: broker.o
	$(CC) $(CFLAGS) -o broker broker.o $(LIBS)

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<  -o $@

clean:
	$(RM) *.o *~ $(MAIN)
