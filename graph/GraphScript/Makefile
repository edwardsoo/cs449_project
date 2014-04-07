ip=127.0.0.1:5555#IP
d=1#Delay

all:
	gcc -o GraphRead GraphRead.c -lzmq

run: all
	./GraphRead $(ip) $(d)

clean:
	rm -f GraphRead
