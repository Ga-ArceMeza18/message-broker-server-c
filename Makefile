CFLAGS = -Wall -Wextra -g -pthread -Iinclude

all: build/brokerd build/test_cluster_metadata build/test_failure_detector

build/brokerd: src/*.c include/*.h
	@mkdir -p build
	mpicc $(CFLAGS) src/*.c -o $@

build/test_cluster_metadata: tests/test_cluster_metadata.c src/cluster_metadata.c src/topic_stub.c include/*.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/test_cluster_metadata.c src/cluster_metadata.c src/topic_stub.c -o $@

build/test_failure_detector: tests/test_failure_detector.c src/failure_detector.c include/*.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/test_failure_detector.c src/failure_detector.c -o $@

test: build/test_cluster_metadata build/test_failure_detector
	./build/test_cluster_metadata
	./build/test_failure_detector

run: build/brokerd
	mpirun --oversubscribe -np 3 ./build/brokerd 9000 data

clean:
	rm -rf build data
.PHONY: all test run clean
