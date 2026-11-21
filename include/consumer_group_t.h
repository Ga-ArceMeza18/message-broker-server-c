// consumer_group_t.h - Representa un grupo de consumidores que colaboran para consumir mensajes de uno o varios tópicos

#ifndef CONSUMER_GROUP_T_H
#define CONSUMER_GROUP_T_H

#include <pthread.h>
#include "group_member_t.h"

typedef struct {
    char* group_id; // Identificador único del grupo de consumidores
    int generation; // Versión del estado del grupo. Permite identificar cambios de membresía y asignación
    int* assignment; // Representa la asignación de particiones a los miembros
    long* committed_offsets; // Guarda los offsets confirmados por el grupo
    pthread_mutex_t lock; // Protege el estado compartido del grupo frente a accesos concurrentes

    group_member_t* members; // Arreglo de miembros del grupo
    int member_count; // Número de miembros actualmente en el grupo
    int member_capacity; // Capacidad máxima del arreglo de miembros
    int partition_count; // Número de particiones que el grupo está consumiendo
} consumer_group_t;

// Inicialización y destrucción
int group_init(consumer_group_t* group, const char* group_id, int partition_count);

void group_destroy(consumer_group_t* group);

// Administracion de miembros
int group_join(consumer_group_t* group, int client_fd, int* member_id);
int group_leave(consumer_group_t* group, int member_id);
int group_heartbeat(consumer_group_t* group, int member_id);

// Administracion de particiones
int group_rebalance(consumer_group_t* group);

// Confirmacion de offsets
int group_commit(consumer_group_t* group, int member_id, int partition_id, long offset);

// Consulta de informacion
int group_describe(consumer_group_t* group, char* buffer, size_t buffer_size);

#endif // CONSUMER_GROUP_T_H
