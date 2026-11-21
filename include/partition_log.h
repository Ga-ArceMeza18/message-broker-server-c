// partition_log.h - Log de una partición. Mensajes en disco, offsets y high watermark

#ifndef PARTITION_LOG_H
#define PARTITION_LOG_H

#include <stdbool.h>
#include <stddef.h>

#include "message.h"

// Cada partición guarda su log en su propio directorio, partido en segmentos:
//
//   <dir>/00000000000000000000.log   mensajes desde el offset 0
//   <dir>/00000000000000001500.log   mensajes desde el offset 1500
//   <dir>/hw.checkpoint              último high watermark guardado
//
// Un segmento es una fila de registros de message.h unidos de seguidp y se
// llama como el offset de su primer mensaje, entonces ordenar por nombre es ordenar
// por offset. Con segmentos la retención puede borrar archivos completos en vez
// de reescribir el log.

#define LOG_DEFAULT_SEGMENT_BYTES (1L << 20)

// Códigos de retorno
typedef enum {
    LOG_OK = 0,
    LOG_ERR_ARGS = -1, // argumentos inválidos o registro dañado
    LOG_ERR_NOMEM = -2, // sin memoria
    LOG_ERR_IO = -3, // falló una lectura o escritura en disco
    LOG_ERR_OFFSET = -4, // el offset está fuera del rango del log
    LOG_ERR_SPACE = -5, // el búfer no alcanza ni para un registro
    LOG_TIMEOUT = -6, // se acabó el tiempo de espera
    LOG_CLOSED = -7 // el log se está cerrando
} log_status_t;

typedef struct {
    long segment_bytes; // tamaño a partir del cual se empieza otro segmento
    bool fsync_each_append; // true hace fsync después de cada escritura
} log_config_t;

typedef struct partition_log partition_log_t;

// Abre el log que está en dir o lo crea si no existe. Al abrirlo se 
// arma el índice, se corta un registro final a medias y se lee el high
// watermark guardado. config puede ser NULL para usar los valores por defecto
log_status_t log_open(partition_log_t** out, const char* dir, const log_config_t* config);

// Rechaza nuevas escrituras y despierta a los hilos que esperan en log_wait_hw
void log_shutdown(partition_log_t* log);

// Guarda todo en disco y libera el log. Nadie más puede estarlo usando
log_status_t log_destroy(partition_log_t* log);

// Escribe un mensaje al final del log (en el líder). Le asigna el siguiente
// offset, que queda en msg->offset y en *offset si no es NULL
log_status_t log_append(partition_log_t* log, message_t* msg, long* offset);

// Escribe un registro ya serializado que llegó del líder. Su offset
// tiene que ser exactamente el siguiente del log
log_status_t log_append_record(partition_log_t* log, const unsigned char* record, size_t size);

// Lee hasta max_messages mensajes desde offset, sin pasar del high watermark.
// Cada mensaje en out se libera con message_destroy
log_status_t log_read(partition_log_t* log, long offset, message_t* out, int max_messages, int* count);

// Copia registros serializados desde offset hasta el final del log (no solo
// hasta el high watermark), para mandarlos a las réplicas
log_status_t log_read_raw(partition_log_t* log, long offset, unsigned char* buf, size_t buf_size,
    size_t* used, int* count);

// Este broker pasa a ser el líder con num_replicas seguidores. El high
// watermark pasa a ser el mínimo entre el líder y las réplicas sincronizadas
log_status_t log_set_leader(partition_log_t* log, int num_replicas);

// Este broker pasa a ser una réplica, su high watermark lo manda el líder
void log_set_follower(partition_log_t* log);

// En el líder. La réplica ya tiene todos los mensajes antes de end_offset
log_status_t log_replica_ack(partition_log_t* log, int replica, long end_offset);

// En el líder. La réplica se reseteó y ya no tiene ningún mensaje
log_status_t log_replica_reset_progress(partition_log_t *log, int replica, long end_offset);

// En el líder. Devuelve el end_offset de la réplica y si está sincronizada
log_status_t log_replica_progress(partition_log_t *log, int replica, long *end_offset);

// En el líder. Saca o mete una réplica del conjunto de réplicas sincronizadas
log_status_t log_set_replica_in_sync(partition_log_t* log, int replica, bool in_sync);

// En el líder. Devuelve true si todas las réplicas están sincronizadas
bool log_all_replicas_in_sync(partition_log_t *log);

// En una réplica. Actualiza el high watermark con el que mandó el líder
void log_advance_hw(partition_log_t* log, long high_watermark);

// Espera hasta que el mensaje en offset quede por debajo del high watermark.
// timeout_ms negativo espera sin límite
log_status_t log_wait_hw(partition_log_t* log, long offset, int timeout_ms);

// Hace fsync del segmento actual y guarda el high watermark
log_status_t log_flush(partition_log_t* log);

// Consultas
long log_next_offset(partition_log_t* log);
long log_high_watermark(partition_log_t* log);
long log_start_offset(partition_log_t* log);

#endif
