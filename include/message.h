// message.h - Mensaje del broker y su formato binario

#ifndef MESSAGE_H
#define MESSAGE_H

#include <stddef.h>
#include <stdint.h>

// Formato de un registro serializado. Todos los enteros van en little-endian
// u significa unsigned, i significa signed, por ejemplo u32 es un entero de 32 bits sin signo
// y i64 es un entero de 64 bits con signo. Los campos son:
//
//   u32 length      bytes que siguen después de este campo
//   u32 crc32       CRC-32 de todo lo que va después de este campo
//   i64 offset      posición del mensaje en la partición
//   i64 timestamp   milisegundos desde 1970
//   i32 key_size    0 si el mensaje no tiene clave
//   i32 value_size
//   key_size bytes de la clave
//   value_size bytes del valor
//

#define MESSAGE_LENGTH_SIZE 4
#define MESSAGE_HEADER_SIZE 32

// Tope de un registro, un largo mayor se toma como dato corrupto
#define MESSAGE_MAX_SIZE (1 << 20)

// Códigos de retorno. Son negativos para que message_serialize pueda devolver
// los bytes escritos en positivo o si es un error en negativo
typedef enum {
    MESSAGE_OK = 0,
    MESSAGE_ERR_ARGS = -1, // argumentos inválidos o sin memoria
    MESSAGE_ERR_SPACE = -2, // el búfer de salida es muy pequeño
    MESSAGE_INCOMPLETE = -3, // faltan bytes, el registro quedó a medias
    MESSAGE_CORRUPT = -4 // el largo o el CRC no cuadran
} message_status_t;

typedef struct {
    long offset; // -1 mientras el líder no le asigne uno
    long timestamp;
    int key_size;
    char* key; // NULL si key_size es 0
    int value_size;
    void* value;
} message_t;

// Crea un mensaje copiando la clave y el valor. El offset queda en -1 y el
// timestamp en la hora actual
message_status_t message_init(message_t* msg, const char* key, int key_size, const void* value, int value_size);

// Libera la clave y el valor del mensaje
void message_destroy(message_t* msg);

// Bytes que ocupa el mensaje ya serializado
size_t message_serialized_size(const message_t* msg);

// Escribe el registro en buf. Devuelve los bytes escritos o un código negativo
long message_serialize(const message_t* msg, unsigned char* buf, size_t buf_size);

// Lee un registro desde buf y deja en consumed los bytes que usó. El mensaje
// queda con memoria propia y se libera con message_destroy
message_status_t message_deserialize(const unsigned char* buf, size_t buf_size, message_t* out, size_t* consumed);

// Tamaño total de un registro a partir de sus primeros 4 bytes, para saber
// cuánto leer del disco. Devuelve 0 si el largo no es válido
size_t message_record_size(const unsigned char* buf);

// CRC-32 (0xEDB88320) para los test
uint32_t message_crc32(const void* data, size_t size);

#endif
