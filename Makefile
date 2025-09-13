CC = gcc
CFLAGS = -Wall
SRC_COMMON = buffer_mgr.c buffer_mgr_stat.c dberror.c storage_manager.c

# Default target
all: test1.exe test2.exe

# Build first test binary
test1.exe: $(SRC_COMMON) test_assign2_1.c
	$(CC) $(CFLAGS) -o $@ $(SRC_COMMON) test_assign2_1.c

# Build second test binary
test2.exe: $(SRC_COMMON) test_assign2_2.c
	$(CC) $(CFLAGS) -o $@ $(SRC_COMMON) test_assign2_2.c

# Run both test binaries
run: test1.exe test2.exe
	./test1.exe
	./test2.exe

# Clean up generated files
clean:
ifeq ($(OS),Windows_NT)
	del /Q *.exe *.o *.bin 2>nul || true
else
	rm -f *.exe *.o *.bin
endif
