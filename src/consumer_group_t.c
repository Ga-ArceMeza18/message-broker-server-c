// consumer_group_t.c - Implementación de consumidor de grupos de usuarios

#include "consumer_group_t.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Funciones auxiliares
// Busca un miembro por su identificador. Debe llamarse con el mutex adquirido.
static int find_member_index(consumer_group_t* group, int member_id) {
    for (int i = 0; i < group->member_count; i++) {
        if (group->members[i].member_id == member_id) {
            return i;
        }
    }

    return -1;
}

// Genera el siguiente identificador disponible.
static int generate_member_id(consumer_group_t* group) {
    if (group->next_member_id <= 0) return -1;

    int id = group->next_member_id;

    if (group->next_member_id == INT_MAX) {
        group->next_member_id = 0;
    } else {
        group->next_member_id++;
    }

    return id;
}

// Aumenta la capacidad del arreglo de miembros. Debe llamarse con el mutex adquirido.
static int reserve_members(consumer_group_t* group) {
    if (group->member_count < group->member_capacity) {
        return 0;
    }

    int new_capacity = 0;

    if (group->member_capacity == 0) {
        new_capacity = 2;
    } else {
        if (group->member_capacity > INT_MAX / 2) {
            return -1;
        }

        new_capacity = group->member_capacity * 2;
    }

    group_member_t* new_members = realloc(group->members, (size_t)new_capacity * sizeof(group_member_t));

    if (new_members == NULL) {
        return -1;
    }

    group->members = new_members;
    group->member_capacity = new_capacity;

    return 0;
}

// Ejecuta el rebalanceo. Debe llamarse con el mutex adquirido.
static int rebalance_locked(consumer_group_t* group) {
    for (int i = 0; i < group->partition_count; i++) {
        group->assignment[i] = -1;
    }

    for (int i = 0; i < group->member_count; i++) {
        group->members[i].owned_partition_count = 0;
    }

    if (group->member_count == 0) {
        group->generation++;
        return 0;
    }

    for (int i = 0; i < group->partition_count; i++) {
        int member_index = i % group->member_count;

        group_member_t* member = &group->members[member_index];
        group->assignment[i] = member->member_id;
        member->owned_partitions[member->owned_partition_count] = i;
        member->owned_partition_count++;
    }

    group->generation++;

    return 0;
}

// Funciones públicas
// Inicializa un grupo de consumidores.
int group_init(consumer_group_t* group, const char* group_id, int partition_count) {

    if (group == NULL || group_id == NULL || group_id[0] == '\0' || partition_count <= 0) {
        return -1;
    }

    memset(group, 0, sizeof(consumer_group_t));

    if (pthread_mutex_init(&group->lock, NULL) != 0) {
        return -1;
    }

    size_t id_length = strlen(group_id) + 1;
    group->group_id = malloc(id_length);

    if (group->group_id == NULL) {
        pthread_mutex_destroy(&group->lock);
        return -1;
    }

    memcpy(group->group_id, group_id, id_length);

    group->assignment = malloc((size_t)partition_count * sizeof(int));
    group->committed_offsets = malloc((size_t)partition_count * sizeof(long));

    if (group->assignment == NULL || group->committed_offsets == NULL) {

        free(group->group_id);
        free(group->assignment);
        free(group->committed_offsets);

        group->group_id = NULL;
        group->assignment = NULL;
        group->committed_offsets = NULL;

        pthread_mutex_destroy(&group->lock);

        return -1;
    }

    group->generation = 0;
    group->next_member_id = 1;
    group->partition_count = partition_count;

    group->members = NULL;
    group->member_count = 0;
    group->member_capacity = 0;

    for (int i = 0; i < partition_count; i++) {
        group->assignment[i] = -1;
        group->committed_offsets[i] = -1;
    }

    return 0;
}

// Libera todos los recursos del grupo.
void group_destroy(consumer_group_t* group) {
    if (group == NULL || group->group_id == NULL) {
        return;
    }

    pthread_mutex_lock(&group->lock);

    for (int i = 0; i < group->member_count; i++) {
        free(group->members[i].owned_partitions);
    }

    free(group->members);
    free(group->assignment);
    free(group->committed_offsets);
    free(group->group_id);

    group->members = NULL;
    group->assignment = NULL;
    group->committed_offsets = NULL;
    group->group_id = NULL;

    group->member_count = 0;
    group->member_capacity = 0;
    group->partition_count = 0;

    pthread_mutex_unlock(&group->lock);

    pthread_mutex_destroy(&group->lock);
}

// Registra un nuevo miembro en el grupo.
int group_join(consumer_group_t* group, int client_fd, int* member_id) {

    if (group == NULL || member_id == NULL || client_fd < 0) {
        return -1;
    }

    // Bloquea el mutex para proteger el estado del grupo durante la operación de unión.
    pthread_mutex_lock(&group->lock);

    // Genera un nuevo identificador de miembro y reserva espacio en el arreglo de miembros.
    int new_id = generate_member_id(group);

    if (new_id == -1 || reserve_members(group) != 0) {
        pthread_mutex_unlock(&group->lock);
        return -1;
    }

    // Reserva espacio para las particiones asignadas al nuevo miembro.
    int* partitions = malloc((size_t)group->partition_count * sizeof(int));

    if (partitions == NULL) {
        pthread_mutex_unlock(&group->lock);
        return -1;
    }

    // Inicializa las particiones asignadas a -1 (ninguna asignada).
    for (int i = 0; i < group->partition_count; i++) {
        partitions[i] = -1;
    }

    // Agrega el nuevo miembro al arreglo de miembros del grupo.
    group_member_t* member = &group->members[group->member_count];

    member->member_id = new_id;
    member->client_fd = client_fd;
    member->last_heartbeat = (long)time(NULL);
    member->owned_partitions = partitions;
    member->owned_partition_count = 0;

    group->member_count++;

    // Realiza el rebalanceo de las particiones entre los miembros del grupo.
    rebalance_locked(group);

    // Devuelve el identificador del nuevo miembro al llamador.
    *member_id = new_id;

    // Libera el mutex antes de salir de la función.
    pthread_mutex_unlock(&group->lock);

    return 0;
}

// Elimina un miembro del grupo.
int group_leave(consumer_group_t* group, int member_id) {

    if (group == NULL || member_id <= 0) {
        return -1;
    }

    // Bloquea el mutex para proteger el estado del grupo durante la operación de salida.
    pthread_mutex_lock(&group->lock);

    // Busca el índice del miembro a eliminar en el arreglo de miembros.
    int index = find_member_index(group, member_id);

    if (index == -1) {
        pthread_mutex_unlock(&group->lock);
        return -1;
    }

    // Libera la memoria de las particiones asignadas al miembro que se va a eliminar.
    free(group->members[index].owned_partitions);

    for (int i = index; i < group->member_count - 1; i++) {
        group->members[i] = group->members[i + 1];
    }

    group->member_count--;

    // Realiza el rebalanceo de las particiones entre los miembros del grupo.
    rebalance_locked(group);

    // Libera el mutex antes de salir de la función.
    pthread_mutex_unlock(&group->lock);

    return 0;
}

// Actualiza el heartbeat de un miembro.
int group_heartbeat(consumer_group_t* group, int member_id) {

    if (group == NULL || member_id <= 0) {
        return -1;
    }

    // Bloquea el mutex para proteger el estado del grupo durante la operación de heartbeat.
    pthread_mutex_lock(&group->lock);
    
    // Busca el índice del miembro que envió el heartbeat en el arreglo de miembros.
    int index = find_member_index(group, member_id);

    if (index == -1) {
        pthread_mutex_unlock(&group->lock);
        return -1;
    }

    // Actualiza el timestamp del último heartbeat recibido para el miembro.
    group->members[index].last_heartbeat = (long)time(NULL);

    pthread_mutex_unlock(&group->lock);

    return 0;
}


// Elimina los miembros que dejaron de enviar heartbeats.
int group_expire_members(consumer_group_t* group, long timeout_seconds) {

    if (group == NULL || timeout_seconds <= 0) {
        return -1;
    }

    pthread_mutex_lock(&group->lock);

    long now = (long)time(NULL);
    int expired_count = 0;
    int i = 0;

    while (i < group->member_count) {

        group_member_t* member = &group->members[i];

        // Comprueba si el miembro superó el tiempo permitido.
        if (now >= member->last_heartbeat && now - member->last_heartbeat >= timeout_seconds) {

            // Liberar las particiones asignadas al miembro.
            free(member->owned_partitions);

            // Desplazar los miembros restantes.
            for (int j = i; j < group->member_count - 1; j++) {
                group->members[j] = group->members[j + 1];
            }

            group->member_count--;
            expired_count++;

            // No incrementamos i porque ahora hay otro miembro
            // ocupando la posición actual.
        } else {
            i++;
        }
    }

    // Solo realizar rebalanceo si realmente hubo expiraciones.
    if (expired_count > 0) {
        rebalance_locked(group);
    }

    pthread_mutex_unlock(&group->lock);

    return expired_count;
}

// Redistribuye las particiones entre los miembros.
int group_rebalance(consumer_group_t* group) {
    if (group == NULL) {
        return -1;
    }

    // Bloquea el mutex para proteger el estado del grupo durante la operación de rebalanceo.
    pthread_mutex_lock(&group->lock);

    // Realiza el rebalanceo de las particiones entre los miembros del grupo.
    int result = rebalance_locked(group);

    // Libera el mutex antes de salir de la función.
    pthread_mutex_unlock(&group->lock);

    return result;
}

// Confirma el offset de una partición.
int group_commit(consumer_group_t* group, int member_id, int partition_id, long offset) {

    // Revisa los parámetros de entrada para asegurarse de que sean válidos.
    if (group == NULL || member_id <= 0 ||
        partition_id < 0 ||
        partition_id >= group->partition_count ||
        offset < 0) {
        return -1;
    }

    // Bloquea el mutex para proteger el estado del grupo durante la operación de commit.
    pthread_mutex_lock(&group->lock);

    // Busca el índice del miembro que está intentando confirmar el offset en el arreglo de miembros.
    int index = find_member_index(group, member_id);

    // Verifica que el miembro exista, que esté asignado a la partición y que el offset sea válido.
    if (index == -1 || group->assignment[partition_id] != member_id ||
        offset < group->committed_offsets[partition_id]) {

        pthread_mutex_unlock(&group->lock);
        return -1;
    }

    // Actualiza el offset confirmado para la partición.
    group->committed_offsets[partition_id] = offset;

    pthread_mutex_unlock(&group->lock);

    return 0;
}

// Genera una descripcion textual del grupo.
int group_describe(consumer_group_t* group, char* buffer, size_t buffer_size) {

    if (group == NULL || buffer == NULL || buffer_size == 0) {
        return -1;
    }

    pthread_mutex_lock(&group->lock);

    size_t used = 0;
    int written;

    // Escribe la información general del grupo en el buffer.
    written = snprintf(buffer, buffer_size,
        "Group: %s\nGeneration: %d\nMembers: %d\nPartitions: %d\n",
        group->group_id,
        group->generation,
        group->member_count,
        group->partition_count
    );

    if (written < 0 || (size_t)written >= buffer_size) {
        pthread_mutex_unlock(&group->lock);
        return -1;
    }

    // Actualiza la cantidad de bytes utilizados en el buffer.
    used = (size_t)written;

    // Itera sobre cada miembro del grupo para escribir su información en el buffer.
    for (int i = 0; i < group->member_count; i++) {
        group_member_t* member = &group->members[i];

        // Escribe la información del miembro en el buffer.
        written = snprintf( buffer + used,
            buffer_size - used,
            "\nMember %d: fd=%d, heartbeat=%ld, partitions=",
            member->member_id,
            member->client_fd,
            member->last_heartbeat
        );

        if (written < 0 || (size_t)written >= buffer_size - used) {
            pthread_mutex_unlock(&group->lock);
            return -1;
        }

        used += (size_t)written;
        // Escribe las particiones asignadas al miembro en el buffer.
        for (int j = 0; j < member->owned_partition_count; j++) {
            written = snprintf(buffer + used, buffer_size - used, "%s%d",
                j == 0 ? "" : ", ",
                member->owned_partitions[j]
            );

            if (written < 0 || (size_t)written >= buffer_size - used) {
                pthread_mutex_unlock(&group->lock);
                return -1;
            }

            // Actualiza la cantidad de bytes utilizados en el buffer después de escribir cada partición.
            used += (size_t)written;
        }

        // Agrega un salto de línea después de la información del miembro.
        written = snprintf(buffer + used, buffer_size - used, "\n");

        if (written < 0 || (size_t)written >= buffer_size - used) {
            pthread_mutex_unlock(&group->lock);
            return -1;
        }

        // Actualiza la cantidad de bytes utilizados en el buffer después de escribir la información del miembro.
        used += (size_t)written;
    }

    pthread_mutex_unlock(&group->lock);

    return 0;
}
