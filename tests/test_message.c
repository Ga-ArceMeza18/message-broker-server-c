// test_message.c - Pruebas unitarias de message_t 

#define _POSIX_C_SOURCE 200809L

#include "message.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CONCURRENT_THREADS 8
#define CONCURRENT_ROUNDS 2000

typedef struct {
    const char* name;
    bool (*run)(void);
} SimpleTest;

// Compara dos mensajes campo por campo
static bool same_message(const message_t* a, const message_t* b) {
    if (a->offset != b->offset || a->timestamp != b->timestamp
        || a->key_size != b->key_size || a->value_size != b->value_size) {
        return false;
    }

    if (a->key_size > 0 && memcmp(a->key, b->key, (size_t)a->key_size) != 0) {
        return false;
    }

    return a->value_size == 0 || memcmp(a->value, b->value, (size_t)a->value_size) == 0;
}

// Serializa un mensaje, lo vuelve a leer y revisa que quede igual
static bool round_trip(const message_t* msg) {
    size_t size = message_serialized_size(msg);
    unsigned char* buf = malloc(size);

    if (buf == NULL || message_serialize(msg, buf, size) != (long)size) {
        free(buf);
        return false;
    }

    message_t copy;
    size_t consumed = 0;
    bool ok = message_deserialize(buf, size, &copy, &consumed) == MESSAGE_OK
        && consumed == size && same_message(msg, &copy);

    if (ok) {
        message_destroy(&copy);
    }

    free(buf);
    return ok;
}

// Comprueba el CRC con el valor conocido de "123456789"
static bool test_crc_known_value(void) {
    return message_crc32("123456789", 9) == 0xCBF43926u;
}

// Comprueba que un mensaje con clave queda igual después de ir y volver
static bool test_round_trip_with_key(void) {
    message_t msg;

    if (message_init(&msg, "cliente-7", 9, "pedido 42", 9) != MESSAGE_OK) {
        return false;
    }

    msg.offset = 123456789012L;
    bool ok = round_trip(&msg);
    message_destroy(&msg);

    return ok;
}

// Comprueba mensajes sin clave y con valor vacío
static bool test_round_trip_without_key(void) {
    message_t msg;

    if (message_init(&msg, NULL, 0, "", 0) != MESSAGE_OK) {
        return false;
    }

    bool ok = msg.key == NULL && msg.offset == -1 && round_trip(&msg);
    message_destroy(&msg);

    return ok;
}

// Comprueba un valor binario con ceros y bytes altos
static bool test_round_trip_binary(void) {
    unsigned char value[256];

    for (int i = 0; i < 256; i++) {
        value[i] = (unsigned char)i;
    }

    message_t msg;

    if (message_init(&msg, "k", 1, value, 256) != MESSAGE_OK) {
        return false;
    }

    bool ok = round_trip(&msg);
    message_destroy(&msg);

    return ok;
}

// Comprueba que el formato es el documentado, largo y offset en little-endian
static bool test_layout(void) {
    message_t msg;

    if (message_init(&msg, "ab", 2, "xyz", 3) != MESSAGE_OK) {
        return false;
    }

    msg.offset = 0x0102;
    unsigned char buf[64];
    long size = message_serialize(&msg, buf, sizeof(buf));
    message_destroy(&msg);

    return size == MESSAGE_HEADER_SIZE + 5
        && buf[0] == size - MESSAGE_LENGTH_SIZE && buf[1] == 0
        && buf[8] == 0x02 && buf[9] == 0x01
        && memcmp(buf + MESSAGE_HEADER_SIZE, "abxyz", 5) == 0
        && message_record_size(buf) == (size_t)size;
}

// Comprueba que se rechazan argumentos inválidos y búferes pequeños
static bool test_invalid_args(void) {
    message_t msg;
    unsigned char buf[8];

    if (message_init(&msg, NULL, 3, "v", 1) != MESSAGE_ERR_ARGS
        || message_init(&msg, "k", -1, "v", 1) != MESSAGE_ERR_ARGS
        || message_init(&msg, "k", 1, NULL, 5) != MESSAGE_ERR_ARGS
        || message_init(NULL, "k", 1, "v", 1) != MESSAGE_ERR_ARGS) {
        return false;
    }

    if (message_init(&msg, "k", 1, "v", 1) != MESSAGE_OK) {
        return false;
    }

    bool ok = message_serialize(&msg, buf, sizeof(buf)) == MESSAGE_ERR_SPACE;
    message_destroy(&msg);

    return ok;
}

// Comprueba que un registro cortado se reporta como incompleto
static bool test_truncated_record(void) {
    message_t msg;
    unsigned char buf[64];

    if (message_init(&msg, "k", 1, "valor", 5) != MESSAGE_OK) {
        return false;
    }

    long size = message_serialize(&msg, buf, sizeof(buf));
    message_destroy(&msg);

    for (long cut = 0; cut < size; cut++) {
        message_t out;

        if (message_deserialize(buf, (size_t)cut, &out, NULL) != MESSAGE_INCOMPLETE) {
            return false;
        }
    }

    return true;
}

// Comprueba que cualquier byte cambiado se detecta como corrupto
static bool test_corrupted_record(void) {
    message_t msg;
    unsigned char buf[64];

    if (message_init(&msg, "k", 1, "valor", 5) != MESSAGE_OK) {
        return false;
    }

    long size = message_serialize(&msg, buf, sizeof(buf));
    message_destroy(&msg);

    // Desde el byte 4, el largo se prueba aparte
    for (long i = MESSAGE_LENGTH_SIZE; i < size; i++) {
        message_t out;
        buf[i] ^= 0x40;

        if (message_deserialize(buf, (size_t)size, &out, NULL) != MESSAGE_CORRUPT) {
            return false;
        }

        buf[i] ^= 0x40;
    }

    // Un largo enorme no debe hacer que se reserve memoria ni leer de más
    buf[3] = 0x7F;
    message_t out;

    return message_deserialize(buf, (size_t)size, &out, NULL) == MESSAGE_CORRUPT && message_record_size(buf) == 0;
}

// Comprueba que se pueden leer varios registros seguidos, como en el log
static bool test_consecutive_records(void) {
    unsigned char buf[512];
    size_t used = 0;

    for (int i = 0; i < 5; i++) {
        message_t msg;
        char value[16];
        int len = snprintf(value, sizeof(value), "mensaje %d", i);

        if (message_init(&msg, NULL, 0, value, len) != MESSAGE_OK) {
            return false;
        }

        msg.offset = i;
        long size = message_serialize(&msg, buf + used, sizeof(buf) - used);
        message_destroy(&msg);

        if (size < 0) {
            return false;
        }

        used += (size_t)size;
    }

    size_t pos = 0;

    for (long expected = 0; expected < 5; expected++) {
        message_t out;
        size_t consumed = 0;

        if (message_deserialize(buf + pos, used - pos, &out, &consumed) != MESSAGE_OK) {
            return false;
        }

        bool ok = out.offset == expected;
        message_destroy(&out);

        if (!ok) {
            return false;
        }

        pos += consumed;
    }

    return pos == used;
}

// Cada hilo serializa y deserializa sus propios mensajes al mismo tiempo,
// la tabla del CRC es lo único compartido
static void* concurrent_worker(void* arg) {
    long id = (long)arg;
    long failures = 0;

    for (int i = 0; i < CONCURRENT_ROUNDS; i++) {
        message_t msg;
        char value[32];
        int len = snprintf(value, sizeof(value), "hilo %ld msg %d", id, i);

        if (message_init(&msg, value, len, value, len) != MESSAGE_OK) {
            failures++;
            continue;
        }

        msg.offset = id * CONCURRENT_ROUNDS + i;

        if (!round_trip(&msg)) {
            failures++;
        }

        message_destroy(&msg);
    }

    return (void*)failures;
}

// Comprueba que serializar desde varios hilos a la vez da siempre lo correcto
static bool test_concurrent_serialize(void) {
    pthread_t threads[CONCURRENT_THREADS];

    for (long i = 0; i < CONCURRENT_THREADS; i++) {
        if (pthread_create(&threads[i], NULL, concurrent_worker, (void*)i) != 0) {
            return false;
        }
    }

    long failures = 0;

    for (int i = 0; i < CONCURRENT_THREADS; i++) {
        void* result;
        pthread_join(threads[i], &result);
        failures += (long)result;
    }

    return failures == 0;
}

int main(void) {
    SimpleTest tests[] = {
        {"CRC-32 da el valor conocido", test_crc_known_value},
        {"Mensaje con clave va y vuelve igual", test_round_trip_with_key},
        {"Mensaje sin clave y vacío va y vuelve igual", test_round_trip_without_key},
        {"Valor binario va y vuelve igual", test_round_trip_binary},
        {"El formato coincide con el documentado", test_layout},
        {"Se rechazan argumentos inválidos", test_invalid_args},
        {"Un registro cortado es incompleto", test_truncated_record},
        {"Un registro alterado es corrupto", test_corrupted_record},
        {"Se leen varios registros seguidos", test_consecutive_records},
        {"Serializar desde varios hilos es correcto", test_concurrent_serialize}
    };

    int total = sizeof(tests) / sizeof(tests[0]);
    int passed = 0;

    printf("================================\n");
    printf("  PRUEBAS UNITARIAS MESSAGE\n");
    printf("================================\n\n");

    for (int i = 0; i < total; i++) {
        if (tests[i].run()) {
            printf("[OK] %s\n", tests[i].name);
            passed++;
        } else {
            printf("[FAIL] %s\n", tests[i].name);
        }
    }

    printf("\n================================\n");
    printf("Pruebas de message: %d/%d aprobadas\n", passed, total);

    return passed == total ? EXIT_SUCCESS : EXIT_FAILURE;
}
