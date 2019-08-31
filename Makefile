CXX=g++
RM=rm -rf

a.out: main.cpp
	$(CXX) main.cpp

clean:
	$(RM) a.out
