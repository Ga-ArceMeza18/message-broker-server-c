#define _GNU_SOURCE

#include "protocol.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Parametros enviables desde la linea de comandos
typedef struct {

    char host[128];
    int port;
    char cmd[64];
    char topic[PROTO_MAX_TOPIC_LEN];
    char group[PROTO_MAX_GROUP_LEN];
    char key[PROTO_MAX_KEY_LEN];
    char value[PROTO_MAX_PAYLOAD];
    int partitions;
    int replication_factor;
    int broker_id;
    int partition_id;
    long offset;
    proto_acks_t acks;
    bool from_beginning;
} 
cli_opts_t;

// Muestra el uso basico de las banderas 
static void print_usage(const char *prog)
{
fprintf(stderr,
        "Uso: %s [--host <host>] [--port <puerto>] <comando> [opciones]\n"
        "Comandos:\n"
        "  topic-create    --topic <nombre> --partitions <n> --replication <n>\n"
        "  topic-list\n"
        "  topic-describe  --topic <nombre>\n"
        "  produce         --topic <nombre> [--key <k>] --value <texto> [--acks 0|1|all]\n"
        "  consume-join    --topic <nombre> --group <grupo> [--from-beginning]\n"
        "  consume-fetch   --topic <nombre> --group <grupo> [--partition <id>] [--offset <n>]\n"
        "  consume-commit  --topic <nombre> --group <grupo> --partition <id> --offset <n>\n"
        "  group-describe  --group <grupo>\n"
        "  broker-pause    --broker-id <id>\n"
        "  broker-resume   --broker-id <id>\n"
        "  shutdown\n",
        prog);
}


static bool parse_acks(const char *s, proto_acks_t *out)
{
    if (strcmp(s, "0") == 0) { // Si es "0", no requiere confirmación
        *out = PROTO_ACKS_NONE;
    return true;
    }
    if (strcmp(s, "1") == 0) { // Si es "1", requiere confirmación del líder
        *out = PROTO_ACKS_LEADER;
    return true;
    }
    if (strcmp(s, "all") == 0 || strcmp(s, "-1") == 0) { // Si es "all" o "-1", requiere confirmación de todas
        *out = PROTO_ACKS_ALL;
    return true;
    }


    return false;
}

// Convierte una cadena a int
static bool parse_int_arg(const char *s, int *out)
{
if (s == NULL || s[0] == '\0')
return false;

char *end = NULL;
long v = strtol(s, &end, 10);
if (*end != '\0')
return false;
*out = (int)v;

return true;
}

// Convierte una cadena a long 
static bool parse_long_arg(const char *s, long *out)
{
if (s == NULL || s[0] == '\0')
return false;

char *end = NULL;
long v = strtol(s, &end, 10);
if (*end != '\0')
return false;
*out = v;

return true;
}


static bool parse_args(int argc, char **argv, cli_opts_t *opts)
{
memset(opts, 0, sizeof(*opts));
snprintf(opts->host, sizeof(opts->host), "127.0.0.1");
opts->port = 9092;
opts->acks = PROTO_ACKS_LEADER;
opts->partition_id = -1;
opts->offset = -1L;
opts->broker_id = -1;

for (int i = 1; i < argc; i++) {
    const char *arg = argv[i];

if (strcmp(arg, "--host") == 0 && i + 1 < argc) {
    snprintf(opts->host, sizeof(opts->host), "%s", argv[++i]);
} else if (strcmp(arg, "--port") == 0 && i + 1 < argc) {
    if (!parse_int_arg(argv[++i], &opts->port))
        return false;

} else if (strcmp(arg, "--topic") == 0 && i + 1 < argc) {
    snprintf(opts->topic, sizeof(opts->topic), "%s", argv[++i]);
} else if (strcmp(arg, "--group") == 0 && i + 1 < argc) {
    snprintf(opts->group, sizeof(opts->group), "%s", argv[++i]);
} else if (strcmp(arg, "--key") == 0 && i + 1 < argc) {
    snprintf(opts->key, sizeof(opts->key), "%s", argv[++i]);
} else if (strcmp(arg, "--value") == 0 && i + 1 < argc) {
    snprintf(opts->value, sizeof(opts->value), "%s", argv[++i]);
} else if (strcmp(arg, "--partitions") == 0 && i + 1 < argc) {
    if (!parse_int_arg(argv[++i], &opts->partitions))
        return false;

} else if (strcmp(arg, "--replication") == 0 && i + 1 < argc) {
    if (!parse_int_arg(argv[++i], &opts->replication_factor))
        return false;

} else if (strcmp(arg, "--broker-id") == 0 && i + 1 < argc) {
    if (!parse_int_arg(argv[++i], &opts->broker_id))
        return false;

} else if (strcmp(arg, "--partition") == 0 && i + 1 < argc) {
    if (!parse_int_arg(argv[++i], &opts->partition_id))
        return false;

} else if (strcmp(arg, "--offset") == 0 && i + 1 < argc) {
    if (!parse_long_arg(argv[++i], &opts->offset))
        return false;

} else if (strcmp(arg, "--acks") == 0 && i + 1 < argc) {
    if (!parse_acks(argv[++i], &opts->acks))
        return false;

} else if (strcmp(arg, "--from-beginning") == 0) {
    opts->from_beginning = true;
} else if (arg[0] == '-') {
    return false;

} else if (opts->cmd[0] == '\0') {
    snprintf(opts->cmd, sizeof(opts->cmd), "%s", arg);

} else {
    return false;

}

  }

    return opts->cmd[0] != '\0';
}

// Asigna el codigo de operacion proto_op_t segun el nombre del comando
static bool map_command_op(const char *cmd, proto_op_t *out_op)
{
if (strcmp(cmd, "topic-create") == 0) 
    *out_op = PROTO_OP_TOPIC_CREATE;

    else if (strcmp(cmd, "topic-list") == 0)
    *out_op = PROTO_OP_TOPIC_LIST;

    else if (strcmp(cmd, "topic-describe") == 0)
    *out_op = PROTO_OP_TOPIC_DESCRIBE;
        
    else if (strcmp(cmd, "produce") == 0)
    *out_op = PROTO_OP_PRODUCE;

    else if (strcmp(cmd, "consume-join") == 0)
    *out_op = PROTO_OP_CONSUME_JOIN;

    else if (strcmp(cmd, "consume-fetch") == 0)
    *out_op = PROTO_OP_CONSUME_FETCH;

    else if (strcmp(cmd, "consume-commit") == 0)
    *out_op = PROTO_OP_CONSUME_COMMIT;

    else if (strcmp(cmd, "group-describe") == 0)
    *out_op = PROTO_OP_GROUP_DESCRIBE;
        
    else if (strcmp(cmd, "broker-pause") == 0)
    *out_op = PROTO_OP_BROKER_PAUSE;

    else if (strcmp(cmd, "broker-resume") == 0)
    *out_op = PROTO_OP_BROKER_RESUME;

    else if (strcmp(cmd, "shutdown") == 0)
    *out_op = PROTO_OP_SHUTDOWN;

 else

    return false;

 return true;
}

// Arma y valida la estructura request_t a partir de las opciones
static bool build_request(const cli_opts_t *opts, request_t *req)
{
memset(req, 0, sizeof(*req));

if (!map_command_op(opts->cmd, &req->op)) {
    fprintf(stderr, "Comando desconocido: %s\n", opts->cmd);
    return false;
}

    snprintf(req->topic, sizeof(req->topic), "%s", opts->topic);
    snprintf(req->group, sizeof(req->group), "%s", opts->group);
    snprintf(req->key, sizeof(req->key), "%s", opts->key);

    req->partitions = opts->partitions;
    req->replication_factor = opts->replication_factor;
    req->broker_id = opts->broker_id;
    req->partition_id = opts->partition_id;
    req->offset = opts->offset;
    req->acks = opts->acks;
    req->from_beginning = opts->from_beginning;

size_t vlen = strlen(opts->value);
if (vlen > PROTO_MAX_PAYLOAD) {
    fprintf(stderr, "Error: --value excede %d bytes\n", PROTO_MAX_PAYLOAD);
    return false;
}

req->value_len = vlen;
if (vlen > 0) {
    memcpy(req->value, opts->value, vlen);
}

if (!validate_request(req)) {
    fprintf(stderr, "Comandos invalidos para el comando '%s'\n", opts->cmd);
   
    return false;
}

   
return true;
}

// Imprime los campos de la respuesta recibida del broker 
static void print_response(const response_t *resp)
{
printf("status=%d (%s) partition=%d offset=%ld\n",
    resp->status,
    resp->status == PROTO_STATUS_OK ? "OK" : "ERR",
    resp->partition_id,
    resp->offset);

if (resp->text_info[0] != '\0') {
  
 printf("info: %s\n", resp->text_info);
}

if (resp->data_len > 0) {
 printf("payload (%zu bytes): ", resp->data_len);
  for (size_t i = 0; i < resp->data_len; i++) {
     unsigned char c = resp->payload[i];
     putchar(isprint(c) ? (int)c : '.');
    }

    putchar('\n');

}
}

int main(int argc, char **argv)
{
cli_opts_t opts;
if (!parse_args(argc, argv, &opts)) {
  print_usage(argv[0]);

  return EXIT_FAILURE;
}

request_t req;
 if (!build_request(&opts, &req)) {
    print_usage(argv[0]);

return EXIT_FAILURE;
}

int fd = proto_connect(opts.host, opts.port);
if (fd < 0) {
  perror("proto_connect");

return EXIT_FAILURE;
    }

if (proto_send_request(fd, &req) < 0) {
 fprintf(stderr, "Error enviando solicitud al broker\n");
 close(fd);

return EXIT_FAILURE;
}

response_t resp;
if (proto_recv_response(fd, &resp) < 0) {
  fprintf(stderr, "Error recibiendo respuesta del broker\n");
 close(fd);

return EXIT_FAILURE;
}

close(fd);
print_response(&resp);

return (resp.status == PROTO_STATUS_OK) ? EXIT_SUCCESS : EXIT_FAILURE;
}