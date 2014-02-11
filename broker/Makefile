CC = gcc
INCLUDES = -I/usr/include/python2.7
CFLAGS = -Wall -g 
LIBS = -lzmq -lczmq
MAIN = ventilator sink 

.PHONY: clean

all:    ventilator sink

ventilator: ventilator.o
	$(CC) $(CFLAGS) -o ventilator ventilator.o $(LIBS)
sink: sink.o
	$(CC) $(CFLAGS) -o sink sink.o $(LIBS)

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<  -o $@

clean:
	$(RM) *.o *~ $(MAIN)
