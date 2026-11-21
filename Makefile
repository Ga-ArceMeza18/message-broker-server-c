CFLAGS = -Wall -Wextra -g -pthread -Iinclude

all: build/brokerd build/test_cluster_metadata build/test_message build/test_partition_log

build/brokerd: src/*.c include/*.h
	@mkdir -p build
	mpicc $(CFLAGS) src/*.c -o $@

build/test_cluster_metadata: tests/test_cluster_metadata.c src/cluster_metadata.c src/topic_stub.c include/*.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/test_cluster_metadata.c src/cluster_metadata.c src/topic_stub.c -o $@

build/test_message: tests/test_message.c src/message.c include/message.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/test_message.c src/message.c -o $@

build/test_partition_log: tests/test_partition_log.c src/partition_log.c src/message.c include/partition_log.h include/message.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/test_partition_log.c src/partition_log.c src/message.c -o $@

test: build/test_cluster_metadata build/test_message build/test_partition_log
	./build/test_cluster_metadata
	./build/test_message
	./build/test_partition_log

run: build/brokerd
	mpirun --oversubscribe -np 3 ./build/brokerd 9000 data

clean:
	rm -rf build data
.PHONY: all test run clean
