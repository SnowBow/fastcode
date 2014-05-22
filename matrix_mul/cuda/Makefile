PROJ = matrix_mul
CC = g++
NVCC = nvcc

CFLAGS = -c -g -Wall -I/opt/local/include -I$(HOME)/cppunit/include
LDFLAGS = -L/opt/local/lib -L$(HOME)/cppunit/lib
LIBS = -lcppunit -ldl
NVCCFLAGS = --ptxas-options=-v -arch=sm_20
OBJS = matrix_mul.o tests.o

all: $(PROJ)

$(PROJ): $(OBJS) $(TEST_CASES)
	$(NVCC) $(LDFLAGS) $(NVCCFLAGS) $^ -o $@ $(LIBS)

%.o : %.cu %.h
	$(NVCC) $(NVCCFLAGS) -c $< -o $@

%.o : %.cpp
	$(CC) $(CFLAGS) $< -o $@ 

clean:
	rm -f $(PROJ) $(OBJS) $(TEST_CASES) *.xml
