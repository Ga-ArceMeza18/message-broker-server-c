// group_member.h - Representa a un consumidor de un grupo de usuarios

#ifndef GROUP_MEMBER_T_H
#define GROUP_MEMBER_T_H

typedef struct group_member {
    int member_id; // Identificador único del miembro del grupo
    int client_fd; // Descriptor del socket del cliente
    long last_heartbeat; // Último momento en que se recibió actividad o señal de vida del consumidor
    int* owned_partitions; // Particiones actualmente asignadas al consumidor
} group_member_t;

#endif // GROUP_MEMBER_T_H
