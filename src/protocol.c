#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdbool.h>
#include <stddef.h>

#define PROTO_MAX_TOPIC_LEN    64
#define PROTO_MAX_GROUP_LEN    64
#define PROTO_MAX_KEY_LEN      128
#define PROTO_MAX_PAYLOAD      4096
#define PROTO_MAX_TEXT_INFO    2048
#define PROTO_MAX_FRAME_BYTES  16384

#define PROTO_STATUS_OK        0
#define PROTO_STATUS_ERR       1

#define ERR_BAD_REQUEST        "BAD_REQUEST"
#define ERR_INVALID_STATE      "INVALID_STATE"
#define ERR_NOT_FOUND          "NOT_FOUND"
#define ERR_IO_ERROR           "IO_ERROR"

typedef enum {
    PROTO_OP_UNKNOWN = 0,
    PROTO_OP_TOPIC_CREATE,
    PROTO_OP_TOPIC_LIST,
    PROTO_OP_TOPIC_DESCRIBE,
    PROTO_OP_PRODUCE,
    PROTO_OP_CONSUME_JOIN,
    PROTO_OP_CONSUME_FETCH,
    PROTO_OP_CONSUME_COMMIT,
    PROTO_OP_GROUP_DESCRIBE,
    PROTO_OP_BROKER_PAUSE,
    PROTO_OP_BROKER_RESUME,
    PROTO_OP_SHUTDOWN
} 
proto_op_t;

typedef enum {
    PROTO_ACKS_NONE   = 0, // acks=0: sin confirmación
    PROTO_ACKS_LEADER = 1, // acks=1: confirmado al escribir en el líder
    PROTO_ACKS_ALL    = -1 // acks=all: confirmado al escribir en todas las réplicas sincronizadas
} 
proto_acks_t;

// todos los parámetros que mbcli o mbbench pueden enviar
typedef struct {
    proto_op_t op;
    char topic[PROTO_MAX_TOPIC_LEN];
    char group[PROTO_MAX_GROUP_LEN];
    char key[PROTO_MAX_KEY_LEN];
    int partitions;
    int replication_factor;
    int broker_id;
    int partition_id;
    long offset;
    proto_acks_t acks;
    bool from_beginning;
    size_t value_len;
    unsigned char value[PROTO_MAX_PAYLOAD];
} 
request_t;

//resultado que devuelve el broker tras procesar una petición
typedef struct {
    int status;
    int partition_id;
    long offset;
    size_t data_len;
    char text_info[PROTO_MAX_TEXT_INFO];
    unsigned char payload[PROTO_MAX_PAYLOAD];
} 
response_t;


size_t encode_request(const request_t *req, unsigned char *buf, size_t buf_cap);
int decode_request(const unsigned char *buf, size_t len, request_t *out_req);
size_t encode_response(const response_t *resp, unsigned char *buf, size_t buf_cap);
bool validate_request(const request_t *req);

// Decodificador de respuesta y transporte TCP 
int decode_response(const unsigned char *buf, size_t len, response_t *out_resp);
int proto_connect(const char *host, int port);
int proto_send_request(int fd, const request_t *req);
int proto_recv_response(int fd, response_t *out_resp);
int proto_recv_request(int fd, request_t *out_req);
int proto_send_response(int fd, const response_t *resp);

#endif