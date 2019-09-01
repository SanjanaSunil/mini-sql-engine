CXX       = g++
CPPFLAGS  = -Iinclude -Isql-parser/src
LIBRARIES = -Wl,-rpath=sql-parser/ -Lsql-parser -lsqlparser
SRC_DIR   = src
SRCS      = $(wildcard $(SRC_DIR)/*.cpp)
RM        = rm -rf

a.out: $(SRCS)
	$(CXX) $(SRCS) $(CPPFLAGS) $(LIBRARIES)

clean:
	$(RM) a.out
