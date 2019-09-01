CXX       = g++
CPPFLAGS  = -Iinclude
LIBRARIES = -Wl,-rpath=lib/ -Llib -lsqlparser
SRC_DIR   = src
SRCS      = $(wildcard $(SRC_DIR)/*.cpp)
RM        = rm -rf

a.out: $(SRCS)
	$(CXX) $(SRCS) $(CPPFLAGS) $(LIBRARIES)

clean:
	$(RM) a.out
