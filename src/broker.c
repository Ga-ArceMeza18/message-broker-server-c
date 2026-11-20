#include "broker.h"

// func: create leader of a partition
int leader_of_partition(broker_t *broker, int partition_id, int N) {
    (void)broker;
    return partition_id % N;
}

// func: get the leader of a partition
int get_partition_leader(broker_t *broker, int partition_id) {
    return broker->partitions[partition_id].leader_rank;
}

// func: create the replica of a partition
int replica_of_partition(broker_t *broker, int partition_id, int N) {
    int replica_index = (partition_id + 1) % N;
    return broker->partitions[partition_id].replica_ranks[replica_index];
}

// func: get the replica of a partition
int get_replica_of_partition(broker_t *broker, int partition_id, int replica_index) {
    return broker->partitions[partition_id].replica_ranks[replica_index];
}
