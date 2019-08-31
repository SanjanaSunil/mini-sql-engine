CXX		 =	g++
CPPFLAGS = -I include
SRC_DIR  = src
SRCS 	 = $(wildcard $(SRC_DIR)/*.cpp)
RM		 = rm -rf

a.out: $(SRCS)
	$(CXX) $(SRCS) $(CPPFLAGS)

clean:
	$(RM) a.out
