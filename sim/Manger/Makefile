CXX=/opt/intel/impi/4.1.0/intel64/bin/mpiicpc
LIB=-L/opt/intel/impi/4.1.0/lib64/ -lpthread -lmpichcxx
INCLUDE=-I/opt/intel/impi/4.1.0/include64
#INCLUDE=-I./
OBJ=work.o task_struct.o KTS_cluster_manger.o
OBJ_PATH=./obj
BIN_PATH=./bin

.PHONY:all clean
EXE=./KTS_cluster_manger.exe

all:$(EXE)

%.o: %.cpp
	$(CXX) -c -DMPICH_IGNORE_CXX_SEEK $< $(INCLUDE)

$(EXE):$(OBJ) 
	$(CXX) -o  $@ $^ $(LIB) 
#	cp hx_cluster_rgb $(BIN_PATH)
	rm -rf $(OBJ)
	
clean:
#	rm -rf $(BIN_PATH)/* $(OBJ_PATH)/* 

task_struct.o:task_struct.cpp task_struct.h
work.o:work.cpp work.h
KTS_cluster_manger.o:KTS_cluster_manger.cpp
