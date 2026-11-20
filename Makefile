CFLAGS = -Wall -Wextra -g -pthread -Iinclude

all: build/brokerd build/test_cluster_metadata

build/brokerd: src/*.c include/*.h
	@mkdir -p build
	mpicc $(CFLAGS) src/*.c -o $@

build/test_cluster_metadata: tests/test_cluster_metadata.c src/cluster_metadata.c src/topic_stub.c include/*.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/test_cluster_metadata.c src/cluster_metadata.c src/topic_stub.c -o $@

test: build/test_cluster_metadata
	./build/test_cluster_metadata

run: build/brokerd
	mpirun --oversubscribe -np 3 ./build/brokerd 9000 data

clean:
	rm -rf build data
.PHONY: all test run clean
