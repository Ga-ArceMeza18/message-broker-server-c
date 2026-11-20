#define _GNU_SOURCE

#include "protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netdb.h>

// Funciones auxiliares para convertir numeros formato de red.

// Pasa un int a orden de red, lo guarda en buf y avanza pos 
static void put_int(unsigned char *buf, size_t *pos, int value)
{
    uint32_t n = htonl((uint32_t)value);
    memcpy(buf + *pos, &n, sizeof(n));
    *pos += sizeof(n);
}

//Lee un int desde buf en orden de red, lo pasa a formato local y avanza pos
static int get_int(const unsigned char *buf, size_t *pos)
{
    uint32_t n;
    memcpy(&n, buf + *pos, sizeof(n));
    *pos += sizeof(n);
    return (int)ntohl(n);
}

// Guarda un long de 64 bits en 8 bytes
static void put_long(unsigned char *buf, size_t *pos, long value)
{
    uint64_t n = (uint64_t)(int64_t)value;
    put_int(buf, pos, (int)(n >> 32));
    put_int(buf, pos, (int)(n & 0xFFFFFFFF));
}

// Lee un long de 64 bits desde 8 bytes
static long get_long(const unsigned char *buf, size_t *pos)
{
    uint64_t high = (uint32_t)get_int(buf, pos);
    uint64_t low = (uint32_t)get_int(buf, pos);
    uint64_t n = (high << 32) | low;
    return (long)(int64_t)n;
}



static int send_all(int fd, const unsigned char *buf, size_t len)
{
size_t enviados = 0;
while (enviados < len) {
    ssize_t n = send(fd, buf + enviados, len - enviados, MSG_NOSIGNAL);
    if (n <= 0) {
       return -1;
        }

        enviados += (size_t)n;
    }

    return 0;
}


static int recv_all(int fd, unsigned char *buf, size_t len)
{
size_t recibidos = 0;
while (recibidos < len) {
    ssize_t n = recv(fd, buf + recibidos, len - recibidos, 0);
    if (n <= 0) {
    return -1;
        }

        recibidos += (size_t)n;
    }

    return 0;
}



static int strings_validas(const request_t *req)
{
if (memchr(req->topic, '\0', PROTO_MAX_TOPIC_LEN) == NULL)
    return 0;

if (memchr(req->group, '\0', PROTO_MAX_GROUP_LEN) == NULL)
    return 0;

if (memchr(req->key, '\0', PROTO_MAX_KEY_LEN) == NULL)
    return 0;

return 1;
}


//validar solicitud antes de enviarla
bool validate_request(const request_t *req)
{
 if (req == NULL)
    return false;

 if (req->op <= PROTO_OP_UNKNOWN ||
        req->op > PROTO_OP_SHUTDOWN)
    return false;

 if (!strings_validas(req))
    return false;

 if (req->value_len > PROTO_MAX_PAYLOAD)
    return false;

 if (req->acks != PROTO_ACKS_NONE &&
        req->acks != PROTO_ACKS_LEADER &&
        req->acks != PROTO_ACKS_ALL)
    return false;

 if (req->partitions < 0)
    return false;

 if (req->replication_factor < 0)
     return false;

 return true;
}





//convierte una secuencia en bytes para enviarse por TCP

size_t encode_request(const request_t *req, unsigned char *buf, size_t buf_cap)
{
    size_t pos = 0;

    if (req == NULL || buf == NULL)
    return 0;

    if (!validate_request(req))
    return 0;

 // Calcula el tamaño total en bytes que ocupará
    size_t necesario = 4 + PROTO_MAX_TOPIC_LEN + PROTO_MAX_GROUP_LEN + PROTO_MAX_KEY_LEN + 4 * 4 + 8 + 4 + 1 + 4 + req->value_len;

    if (buf_cap < necesario)
    return 0;

    put_int(buf, &pos, (int)req->op);

     memcpy(buf + pos, req->topic, PROTO_MAX_TOPIC_LEN);
     pos += PROTO_MAX_TOPIC_LEN;

     memcpy(buf + pos, req->group, PROTO_MAX_GROUP_LEN);
     pos += PROTO_MAX_GROUP_LEN;

     memcpy(buf + pos, req->key, PROTO_MAX_KEY_LEN);
     pos += PROTO_MAX_KEY_LEN;

    put_int(buf, &pos, req->partitions);
    put_int(buf, &pos, req->replication_factor);
    put_int(buf, &pos, req->broker_id);
    put_int(buf, &pos, req->partition_id);
    put_long(buf, &pos, req->offset);
    put_int(buf, &pos, (int)req->acks);

    buf[pos++] = req->from_beginning ? 1 : 0;
    
    put_int(buf, &pos, (int)req->value_len);
     memcpy(buf + pos, req->value, req->value_len);
     pos += req->value_len;

    return pos;
}


//Convierte los bytes recibidos en una solicitud
 int decode_request(const unsigned char *buf, size_t len, request_t *out_req)
 {
    size_t pos = 0;

    if (buf == NULL || out_req == NULL)
    return -1;

    if (len > PROTO_MAX_FRAME_BYTES)
    return -1;

    // Tamaño mínimo de una petición 
    size_t minimo = 4 + PROTO_MAX_TOPIC_LEN + PROTO_MAX_GROUP_LEN + PROTO_MAX_KEY_LEN + 4 * 4 + 8 + 4 + 1 + 4;
   
    if (len < minimo)
    return -1;

     memset(out_req, 0, sizeof(request_t));

    out_req->op = (proto_op_t)get_int(buf, &pos);

     memcpy(out_req->topic, buf + pos, PROTO_MAX_TOPIC_LEN);
     pos += PROTO_MAX_TOPIC_LEN;

     memcpy(out_req->group, buf + pos, PROTO_MAX_GROUP_LEN);
     pos += PROTO_MAX_GROUP_LEN;

     memcpy(out_req->key, buf + pos, PROTO_MAX_KEY_LEN);
     pos += PROTO_MAX_KEY_LEN;

    out_req->partitions = get_int(buf, &pos);
    out_req->replication_factor = get_int(buf, &pos);
    out_req->broker_id = get_int(buf, &pos);
    out_req->partition_id = get_int(buf, &pos);
    out_req->offset = get_long(buf, &pos);
    out_req->acks = (proto_acks_t)get_int(buf, &pos);
    out_req->from_beginning = buf[pos++] != 0;

     int value_len = get_int(buf, &pos);

     if (value_len < 0 || value_len > PROTO_MAX_PAYLOAD)
     return -1;

     if (len != pos + (size_t)value_len)
     return -1;

    out_req->value_len = (size_t)value_len;

     memcpy(out_req->value, buf + pos, out_req->value_len);

    if (!validate_request(out_req))
    return -1;


    return 0;
}


//Convierte una respuesta en bytes
size_t encode_response(const response_t *resp, unsigned char *buf, size_t buf_cap)
{
    size_t pos = 0;

    if (resp == NULL || buf == NULL)
    return 0;

    //se asegura que tenga terminador antes de enviarse
    if (memchr(resp->text_info, '\0', PROTO_MAX_TEXT_INFO) == NULL)
    return 0;

    if (resp->data_len > PROTO_MAX_PAYLOAD)
    return 0;

    // Calcula el tamaño total necesario para serializar la respuesta 
    size_t necesario = 4 + 4 + 8 + 4 + PROTO_MAX_TEXT_INFO + resp->data_len;

    if (buf_cap < necesario)
    return 0;

    put_int(buf, &pos, resp->status);
    put_int(buf, &pos, resp->partition_id);
    put_long(buf, &pos, resp->offset);
    put_int(buf, &pos, (int)resp->data_len);

     memcpy(buf + pos, resp->text_info, PROTO_MAX_TEXT_INFO);
     pos += PROTO_MAX_TEXT_INFO;

     memcpy(buf + pos, resp->payload, resp->data_len);
     pos += resp->data_len;


    return pos;

}


//Convierte los bytes recibidos en una respuesta
int decode_response(const unsigned char *buf, size_t len, response_t *out_resp)
{
    size_t pos = 0;

    if (buf == NULL || out_resp == NULL)
    return -1;

    // Tamaño mínimo de una respuesta
    size_t minimo = 4 + 4 + 8 + 4 + PROTO_MAX_TEXT_INFO;

    if (len < minimo || len > PROTO_MAX_FRAME_BYTES)
    return -1;

    memset(out_resp, 0, sizeof(response_t));

    out_resp->status = get_int(buf, &pos);
    out_resp->partition_id = get_int(buf, &pos);
    out_resp->offset = get_long(buf, &pos);

    int data_len = get_int(buf, &pos);

    if (data_len < 0 || data_len > PROTO_MAX_PAYLOAD)
    return -1;

    // Extrae text_info y adelantamos pos antes de verificar el final de trama 
    memcpy(out_resp->text_info, buf + pos, PROTO_MAX_TEXT_INFO);
    out_resp->text_info[PROTO_MAX_TEXT_INFO - 1] = '\0';
    pos += PROTO_MAX_TEXT_INFO;

    if (len != pos + (size_t)data_len)
    return -1;

    memcpy(out_resp->payload, buf + pos, (size_t)data_len);
    out_resp->data_len = (size_t)data_len;

    return 0;
}


// Establecer una conexion TCP con otros equipos
int proto_connect(const char *host, int port)
{
    struct addrinfo hints;
    struct addrinfo *result;
    struct addrinfo *actual;

    char puerto[67];

    if (host == NULL || port <= 0 || port > 65535)
    return -1;

    snprintf(puerto, sizeof(puerto), "%d", port);
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, puerto, &hints, &result) != 0)
    return -1;

    int fd = -1;

    for (actual = result; actual != NULL; actual = actual->ai_next) {

        fd = socket(actual->ai_family, actual->ai_socktype, actual->ai_protocol);

        if (fd < 0)
        continue;

        if (connect(fd, actual->ai_addr, actual->ai_addrlen) == 0)
        break;

        close(fd);
        fd = -1;
    }

    freeaddrinfo(result);


    return fd;
}


//Envia una solicitud por TCP
int proto_send_request(int fd, const request_t *req)
{
    unsigned char buf[PROTO_MAX_FRAME_BYTES];

    size_t len = encode_request(req, buf, sizeof(buf));

    if (len == 0)
    return -1;

    uint32_t tamano = htonl((uint32_t)len);

    if (send_all(fd, (unsigned char *)&tamano, sizeof(tamano)) < 0)
    return -1;

    if (send_all(fd, buf, len) < 0)
    return -1;


    return 0;
}


// Recibe una solicitud por TCP
 int proto_recv_request(int fd, request_t *out_req)
{
    uint32_t tamano;

    if (recv_all(fd, (unsigned char *)&tamano, sizeof(tamano)) < 0)
    return -1;

    size_t len = ntohl(tamano);

    if (len == 0 || len > PROTO_MAX_FRAME_BYTES)
    return -1;

    unsigned char buf[PROTO_MAX_FRAME_BYTES];

    if (recv_all(fd, buf, len) < 0)
    return -1;

    
    return decode_request(buf, len, out_req);
}


//Envia una respuesta por TCP
 int proto_send_response(int fd, const response_t *resp)
{
    unsigned char buf[PROTO_MAX_FRAME_BYTES];

    size_t len = encode_response(resp, buf, sizeof(buf));

    if (len == 0)
    return -1;

    uint32_t tamano = htonl((uint32_t)len);

    if (send_all(fd, (unsigned char *)&tamano, sizeof(tamano)) < 0)
    return -1;

    if (send_all(fd, buf, len) < 0)
    return -1;


    return 0;

}

// Recibe una respuesta por TCP
int proto_recv_response(int fd, response_t *out_resp)
{
    uint32_t tamano;

    if (recv_all(fd, (unsigned char *)&tamano, sizeof(tamano)) < 0)
    return -1;

    size_t len = ntohl(tamano);

    if (len == 0 || len > PROTO_MAX_FRAME_BYTES)
    return -1;

    unsigned char buf[PROTO_MAX_FRAME_BYTES];

    if (recv_all(fd, buf, len) < 0)
    return -1;

    return decode_response(buf, len, out_resp);
}