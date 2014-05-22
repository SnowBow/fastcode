PROJ = matrix_mul
CC = g++

CFLAGS = -c -g -Wall -I/opt/local/include -I$(HOME)/cppunit/include
LDFLAGS = -L/opt/local/lib -L$(HOME)/cppunit/lib
LIBS = -lcppunit -ldl
OBJS = $(patsubst %.cpp,%.o,$(wildcard *.cpp))
OMPFLAGS = -fopenmp
UNROLLFLAG = -funroll-loops
#SIMDFLAGS = -msse2 -msse3 -O3 -ffast-math -ftree-slp-vectorize -ftree-vectorizer-verbose=2 -lm
SIMDFLAGS = -msse2 -msse3 -O2 -lm

all: $(PROJ)

$(PROJ): $(OBJS)
	$(CC) $(OMPFLAGS) $(LDFLAGS) $(UNROLLFLAG) $(SIMDFLAGS) $^ -o $@ $(LIBS)

%.o : %.cpp
	$(CC) $(CFLAGS) $< -o $@ $(OMPFLAGS) $(UNROLLFLAG) $(SIMDFLAGS)

%.o : %.cpp %.h
	$(CC) $(CFLAGS) $< -o $@ $(OMPFLAGS) $(UNROLLFLAG) $(SIMDFLAGS)

clean:
	rm -f $(PROJ) $(OBJS) *.xml
