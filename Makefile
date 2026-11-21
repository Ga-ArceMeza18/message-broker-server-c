CFLAGS = -Wall -Wextra -g -pthread -Iinclude

CLIENT_SRC = src/mbcli.c src/mbbench.c
BROKER_SRC = $(filter-out $(CLIENT_SRC),$(wildcard src/*.c))

all: build/brokerd build/mbcli build/mbbench build/test_cluster_metadata build/test_message build/test_partition_log build/test_failure_detector build/test_consumer_group

build/brokerd: $(BROKER_SRC) include/*.h
	@mkdir -p build
	mpicc $(CFLAGS) $(BROKER_SRC) -o $@

build/mbcli: src/mbcli.c src/protocol.c include/protocol.h
	@mkdir -p build
	$(CC) $(CFLAGS) src/mbcli.c src/protocol.c -o $@

build/mbbench: src/mbbench.c src/protocol.c include/protocol.h
	@mkdir -p build
	$(CC) $(CFLAGS) src/mbbench.c src/protocol.c -o $@

build/test_cluster_metadata: tests/test_cluster_metadata.c src/cluster_metadata.c src/topic_stub.c include/*.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/test_cluster_metadata.c src/cluster_metadata.c src/topic_stub.c -o $@

build/test_message: tests/test_message.c src/message.c include/message.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/test_message.c src/message.c -o $@

build/test_partition_log: tests/test_partition_log.c src/partition_log.c src/message.c include/partition_log.h include/message.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/test_partition_log.c src/partition_log.c src/message.c -o $@

build/test_failure_detector: tests/test_failure_detector.c src/failure_detector.c include/failure_detector.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/test_failure_detector.c src/failure_detector.c -o $@

build/test_consumer_group: tests/test_consumer_group.c src/consumer_group_t.c include/consumer_group_t.h include/group_member_t.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/test_consumer_group.c src/consumer_group_t.c -o $@

test: build/test_cluster_metadata build/test_message build/test_partition_log build/test_failure_detector build/test_consumer_group
	./build/test_cluster_metadata
	./build/test_message
	./build/test_partition_log
	./build/test_failure_detector
	./build/test_consumer_group

run: build/brokerd
	mpirun -np 3 ./build/brokerd 9000 data

clean:
	rm -rf build data
.PHONY: all test run clean
