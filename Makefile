CC = g++
CFLAGS = --std=c++17 -Wall -Werror -pedantic -g
LIB = -larrow -lparquet -lz -lsnappy -llz4 -lbz2 -lthrift

# Your .hpp files
DEPS = csv.hpp

# Your compiled .o files
OBJECTS = csv_to_parquet.o

# The name of your program
PROGRAM = CSV_PARQUET

.PHONY: all clean lint

all: $(PROGRAM)

# Wildcard recipe to make .o files from corresponding .cpp file
%.o: %.cpp $(DEPS)
	$(CC) $(CFLAGS) -c $<

$(PROGRAM): $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $^ $(LIB)

clean:
	rm -f *.o $(PROGRAM)

lint:
	cpplint *.cpp *.hpp
