// topic.h - Tópicos, particiones y la conexión de cada partición con su log

#ifndef TOPIC_H
#define TOPIC_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>

#include "partition_log.h"

// Una partición tiene un líder y sus réplicas. Cada broker que la guarda tiene 
// su propio log en <data_dir>/<topico>-<partición>/. En los demás brokers log queda en NULL.
// El log tiene sus propios locks.
typedef struct {
    int partition_id;
    int leader_rank; // -1 mientras no se asigne
    int num_replicas; // igual al replication_factor del tópico
    int* replica_ranks; // num_replicas brokers que guardan la partición, incluido el líder
    int* isr_ranks; // réplicas sincronizadas, los espacios vacíos tienen -1
    partition_log_t* log; // NULL si este broker no guarda la partición
} partition_t;

typedef struct {
    char* name;
    int num_partitions;
    int replication_factor;
    long retention_bytes; // 0 es sin límite
    partition_t* partitions;

    // Turno para repartir los mensajes sin clave entre las particiones. Es
    // atómico porque varios hilos del broker producen al mismo tiempo
    atomic_uint next_partition;
} topic_t;

// Crea un tópico con sus particiones sin asignar. El nombre solo puede tener
// letras, números, punto, guion y guion bajo, porque se usa como carpeta
topic_t* topic_create(const char* name, int num_partitions, int replication_factor, long retention_bytes);

// Cierra los logs abiertos y libera el tópico
void topic_destroy(topic_t* t);

// Partición de un mensaje. La misma clave va siempre a la misma partición 
// Sin clave se reparten en turno rotativo
int topic_partition_for_key(topic_t* t, const char* key, int key_size);

// Partición por número, NULL si no existe
partition_t* topic_get_partition(topic_t* t, int partition_id);

// Abre el log de cada partición que guarda el broker y le pone su rol. Se
// llama después de que cluster_metadata asigna líderes y réplicas. Si el log ya
// existía en disco se recupera. Devuelve 0 o -1
int topic_open_logs(topic_t* t, int my_rank, const char* data_dir, const log_config_t* config);

// Actualiza el rol de cada log abierto y las réplicas sincronizadas 
// después de un cambio de líder o de ISR. Devuelve 0 o -1
int topic_update_roles(topic_t* t, int my_rank);

// Despierta a los que esperan en los logs y rechaza nuevas escrituras 
void topic_shutdown_logs(topic_t* t);

// Consultas sobre una partición
bool partition_is_leader(const partition_t* p, int rank);
bool partition_is_replica(const partition_t* p, int rank);

// Número de seguidor de rank sin contar al líder, o -1 si no es seguidor
// Es el índice que se pasa a log_replica_ack y log_set_replica_in_sync
int partition_follower_index(const partition_t* p, int rank);

// Cuántas réplicas sincronizadas hay (los -1 no cuentan)
int partition_isr_count(const partition_t* p);

#endif
