// message.c - Creación, serialización y deserialización de mensajes

#define _POSIX_C_SOURCE 200809L

#include "message.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Funciones privadas

// La tabla del CRC se llena una sola vez aunque varios hilos serialicen al
// mismo tiempo
static uint32_t crc_table[256];
static pthread_once_t crc_once = PTHREAD_ONCE_INIT;

// Llena la tabla con el polinomio 0xEDB88320
static void crc_table_init(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;

        for (int bit = 0; bit < 8; bit++) {
            c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
        }

        crc_table[i] = c;
    }
}

// Sigue un CRC ya empezado con más bytes, así se calcula por partes
static uint32_t crc_update(uint32_t crc, const void* data, size_t size) {
    const unsigned char* bytes = data;

    for (size_t i = 0; i < size; i++) {
        crc = crc_table[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
    }

    return crc;
}

// Escribe un entero de 32 bits en little-endian
static void put_u32(unsigned char* buf, uint32_t value) {
    for (int i = 0; i < 4; i++) {
        buf[i] = (unsigned char)(value >> (8 * i));
    }
}

// Escribe un entero de 64 bits en little-endian
static void put_u64(unsigned char* buf, uint64_t value) {
    for (int i = 0; i < 8; i++) {
        buf[i] = (unsigned char)(value >> (8 * i));
    }
}

// Lee un entero de 32 bits en little-endian
static uint32_t get_u32(const unsigned char* buf) {
    uint32_t value = 0;

    for (int i = 0; i < 4; i++) {
        value |= (uint32_t)buf[i] << (8 * i);
    }

    return value;
}

// Lee un entero de 64 bits en little-endian
static uint64_t get_u64(const unsigned char* buf) {
    uint64_t value = 0;

    for (int i = 0; i < 8; i++) {
        value |= (uint64_t)buf[i] << (8 * i);
    }

    return value;
}

// Hora actual en milisegundos desde 1970
static long current_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    return (long)ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

// Revisa que los tamaños tengan sentido y que haya datos si el tamaño no es 0
static int sizes_valid(const void* key, int key_size, const void* value, int value_size) {
    if (key_size < 0 || value_size < 0) {
        return 0;
    }

    if ((key_size > 0 && key == NULL) || (value_size > 0 && value == NULL)) {
        return 0;
    }

    size_t total = (size_t)MESSAGE_HEADER_SIZE + (size_t)key_size + (size_t)value_size;

    return total <= MESSAGE_MAX_SIZE;
}

// Funciones públicas

uint32_t message_crc32(const void* data, size_t size) {
    pthread_once(&crc_once, crc_table_init);

    return crc_update(0xFFFFFFFFu, data, size) ^ 0xFFFFFFFFu;
}

message_status_t message_init(message_t* msg, const char* key, int key_size, const void* value, int value_size) {
    if (msg == NULL || !sizes_valid(key, key_size, value, value_size)) {
        return MESSAGE_ERR_ARGS;
    }

    memset(msg, 0, sizeof(*msg));
    msg->offset = -1;
    msg->timestamp = current_time_ms();

    if (key_size > 0) {
        msg->key = malloc((size_t)key_size);

        if (msg->key == NULL) {
            return MESSAGE_ERR_ARGS;
        }

        memcpy(msg->key, key, (size_t)key_size);
        msg->key_size = key_size;
    }

    // Se reserva al menos un byte para que un valor vacío no sea NULL
    msg->value = malloc(value_size > 0 ? (size_t)value_size : 1);

    if (msg->value == NULL) {
        message_destroy(msg);
        return MESSAGE_ERR_ARGS;
    }

    if (value_size > 0) {
        memcpy(msg->value, value, (size_t)value_size);
    }

    msg->value_size = value_size;

    return MESSAGE_OK;
}

void message_destroy(message_t* msg) {
    if (msg == NULL) {
        return;
    }

    free(msg->key);
    free(msg->value);
    msg->key = NULL;
    msg->value = NULL;
    msg->key_size = 0;
    msg->value_size = 0;
}

size_t message_serialized_size(const message_t* msg) {
    if (msg == NULL || msg->key_size < 0 || msg->value_size < 0) {
        return 0;
    }

    return (size_t)MESSAGE_HEADER_SIZE + (size_t)msg->key_size + (size_t)msg->value_size;
}

long message_serialize(const message_t* msg, unsigned char* buf, size_t buf_size) {
    if (msg == NULL || buf == NULL
        || !sizes_valid(msg->key, msg->key_size, msg->value, msg->value_size)) {
        return MESSAGE_ERR_ARGS;
    }

    size_t total = message_serialized_size(msg);

    if (buf_size < total) {
        return MESSAGE_ERR_SPACE;
    }

    put_u32(buf, (uint32_t)(total - MESSAGE_LENGTH_SIZE));
    // buf + 4 es el CRC, se llena al final cuando ya están los datos
    put_u64(buf + 8, (uint64_t)msg->offset);
    put_u64(buf + 16, (uint64_t)msg->timestamp);
    put_u32(buf + 24, (uint32_t)msg->key_size);
    put_u32(buf + 28, (uint32_t)msg->value_size);

    if (msg->key_size > 0) {
        memcpy(buf + MESSAGE_HEADER_SIZE, msg->key, (size_t)msg->key_size);
    }

    if (msg->value_size > 0) {
        memcpy(buf + MESSAGE_HEADER_SIZE + msg->key_size, msg->value, (size_t)msg->value_size);
    }

    put_u32(buf + 4, message_crc32(buf + 8, total - 8));

    return (long)total;
}

size_t message_record_size(const unsigned char* buf) {
    if (buf == NULL) {
        return 0;
    }

    size_t length = get_u32(buf);

    if (length < MESSAGE_HEADER_SIZE - MESSAGE_LENGTH_SIZE
        || length > MESSAGE_MAX_SIZE - MESSAGE_LENGTH_SIZE) {
        return 0;
    }

    return length + MESSAGE_LENGTH_SIZE;
}

message_status_t message_deserialize(const unsigned char* buf, size_t buf_size, message_t* out, size_t* consumed) {
    if (buf == NULL || out == NULL) {
        return MESSAGE_ERR_ARGS;
    }

    // Sin el encabezado completo no se puede ni validar el largo
    if (buf_size < MESSAGE_HEADER_SIZE) {
        return MESSAGE_INCOMPLETE;
    }

    size_t total = message_record_size(buf);

    if (total == 0) {
        return MESSAGE_CORRUPT;
    }

    if (buf_size < total) {
        return MESSAGE_INCOMPLETE;
    }

    if (get_u32(buf + 4) != message_crc32(buf + 8, total - 8)) {
        return MESSAGE_CORRUPT;
    }

    int key_size = (int)get_u32(buf + 24);
    int value_size = (int)get_u32(buf + 28);

    // Los tamaños tienen que sumar exactamente el largo del registro
    if (key_size < 0 || value_size < 0
        || (size_t)MESSAGE_HEADER_SIZE + (size_t)key_size + (size_t)value_size != total) {
        return MESSAGE_CORRUPT;
    }

    const unsigned char* key = buf + MESSAGE_HEADER_SIZE;
    const unsigned char* value = key + key_size;

    if (message_init(out, (const char*)key, key_size, value, value_size) != MESSAGE_OK) {
        return MESSAGE_ERR_ARGS;
    }

    out->offset = (long)get_u64(buf + 8);
    out->timestamp = (long)get_u64(buf + 16);

    if (consumed != NULL) {
        *consumed = total;
    }

    return MESSAGE_OK;
}
