
INC= -I.
LIB= -lpthread

CC=gcc
CC_FLAG=-Wall -g

PRG=splitter
OBJ= $(patsubst %.c,%.o,$(wildcard *.c))

$(PRG):$(OBJ)
	$(CC) $(INC) $(LIB) -o $@ $(OBJ)
	
%.o:%.c
	$(CC) -c  $(CC_FLAG) $(INC) $< -o $@

.PRONY:clean
clean:
	@echo "Removing linked and compiled files......"
	rm -f $(OBJ) $(PRG)
