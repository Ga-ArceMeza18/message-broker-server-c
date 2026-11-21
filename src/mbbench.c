#define _GNU_SOURCE

#include "protocol.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

//Configuracion del benchmark
typedef struct {
    char host[128];
    int port;
    char topic[PROTO_MAX_TOPIC_LEN];
    char key_prefix[64];
    char csv_path[256];
    int threads;
    int messages_per_thread;
    int payload_size;
    proto_acks_t acks;

} 

bench_cfg_t;

// Estadisticas y muestras de latencia por hilo 
typedef struct {
    int thread_id;
    const bench_cfg_t *cfg;
    long ok_count;
    long err_count;
    double total_latency_ms;
    double max_latency_ms;
    double *latencies_ms;
    int recorded_count;

} 

worker_stats_t;

// metricas calculadas
typedef struct {
    long total_msgs;
    long total_ok;
    long total_err;
    double total_ms;
    double throughput_msg_s;
    double avg_latency_ms;
    double max_latency_ms;
    double p50_ms;
    double p99_ms;

} 
bench_summary_t;

//tiempo actual en milisegundos
static double now_ms(void){
 struct timespec ts;
 clock_gettime(CLOCK_MONOTONIC, &ts);

return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

/* Muestra como ejecutar mbbench */
static void print_usage(const char *prog){
fprintf(stderr,
        "Uso: %s [--host <host>] [--port <puerto>] --topic <nombre>\n"
        "        [--threads <n>] [--messages <n>] [--size <bytes>]\n"
        "        [--acks 0|1|all] [--csv <archivo.csv>]\n",
        prog);
}


static bool parse_acks(const char *s, proto_acks_t *out)
{
    if (strcmp(s, "0") == 0) {
        *out = PROTO_ACKS_NONE;
        return true;
    }
    if (strcmp(s, "1") == 0) {
        *out = PROTO_ACKS_LEADER;
        return true;
    }
    if (strcmp(s, "all") == 0 || strcmp(s, "-1") == 0) {
        *out = PROTO_ACKS_ALL;
        return true;
    }
    return false;
}

//Convierte una cadena a entero positivo 
static bool parse_pos_int(const char *s, int *out)
{
if (s == NULL || s[0] == '\0')
    return false;

    char *end = NULL;
    long v = strtol(s, &end, 10);

if (*end != '\0' || v <= 0 || v > 10000000)
    return false;

    *out = (int)v;

return true;
}

// Lee y valida los argumentos de linea de comandos
static bool parse_bench_args(int argc, char **argv, bench_cfg_t *cfg)
{
memset(cfg, 0, sizeof(*cfg));
snprintf(cfg->host, sizeof(cfg->host), "127.0.0.1");
cfg->port = 9092;
snprintf(cfg->key_prefix, sizeof(cfg->key_prefix), "k");
cfg->threads = 4;
cfg->messages_per_thread = 100;
cfg->payload_size = 64;
cfg->acks = PROTO_ACKS_LEADER;

for (int i = 1; i < argc; i++) {
    const char *arg = argv[i];

if (strcmp(arg, "--host") == 0 && i + 1 < argc) {
    snprintf(cfg->host, sizeof(cfg->host), "%s", argv[++i]);
} else if (strcmp(arg, "--port") == 0 && i + 1 < argc) {
    int p = 0;
    if (!parse_pos_int(argv[++i], &p) || p > 65535)
        return false;

    cfg->port = p;
} else if (strcmp(arg, "--topic") == 0 && i + 1 < argc) {
    snprintf(cfg->topic, sizeof(cfg->topic), "%s", argv[++i]);
} else if (strcmp(arg, "--threads") == 0 && i + 1 < argc) {
    if (!parse_pos_int(argv[++i], &cfg->threads))
        return false;

} else if (strcmp(arg, "--messages") == 0 && i + 1 < argc) {
    if (!parse_pos_int(argv[++i], &cfg->messages_per_thread))
        return false;

} else if (strcmp(arg, "--size") == 0 && i + 1 < argc) {
    int sz = 0;
    if (!parse_pos_int(argv[++i], &sz) || sz > PROTO_MAX_PAYLOAD)
        return false;

    cfg->payload_size = sz;
} else if (strcmp(arg, "--acks") == 0 && i + 1 < argc) {
    if (!parse_acks(argv[++i], &cfg->acks))
        return false;

} else if (strcmp(arg, "--csv") == 0 && i + 1 < argc) {
    snprintf(cfg->csv_path, sizeof(cfg->csv_path), "%s", argv[++i]);
} else {

    return false;
    
   }
 }

    return cfg->topic[0] != '\0';
}

// Prepara una peticion PROTO_OP_PRODUCE
static void fill_produce_request(request_t *req, const bench_cfg_t *cfg, int thread_id, int seq)
{
    memset(req, 0, sizeof(*req));
    req->op = PROTO_OP_PRODUCE;
    snprintf(req->topic, sizeof(req->topic), "%s", cfg->topic);
    snprintf(req->key, sizeof(req->key), "%s-%d-%d", cfg->key_prefix, thread_id, seq);
    req->acks = cfg->acks;
    req->value_len = (size_t)cfg->payload_size;

    for (int i = 0; i < cfg->payload_size; i++) {
        req->value[i] = (unsigned char)('a' + ((seq + i) % 26));
    }
}

// Rutina de cada hilo worker
// Función que ejecuta cada hilo creado en run_scenario()
static void *worker_routine(void *arg)
{
    // Convierte el puntero genérico arg a la estructura de este hilo
    worker_stats_t *st = (worker_stats_t *)arg;
    // Obtiene el puntero de solo lectura a la configuración general del benchmark
    const bench_cfg_t *cfg = st->cfg;

    // Abre una conexión TCP del hilo hacia el host y puerto del broker
    int fd = proto_connect(cfg->host, cfg->port);
    if (fd < 0) {
        st->err_count += cfg->messages_per_thread;
    return NULL;

    }

    // Bucle para enviar y recibir la cantidad de mensajes
    for (int i = 0; i < cfg->messages_per_thread; i++) {
        request_t req;
        response_t resp;
        // Arma el mensaje PROTO_OP_PRODUCE 
        fill_produce_request(&req, cfg, st->thread_id, i);

    // Guarda el tiempo inicial en microsegundos antes de enviar la petición
    double t0 = now_ms();

        // Envía la petición y espera la respuesta
        if (proto_send_request(fd, &req) < 0 || proto_recv_response(fd, &resp) < 0) {
            st->err_count++; //suma error
            
        continue;
        }

    // Calcula cuánto tardó el ciclo completo de envío + respuesta en milisegundos
    double elapsed = now_ms() - t0;

    // Acumula la latencia 
    st->total_latency_ms += elapsed;

     // Actualiza la latencia máxima
    if (elapsed > st->max_latency_ms) {
            st->max_latency_ms = elapsed;
    }
    // Guarda la muestra en el arreglo del hilo para calcular p50 y p99 después
    if (st->latencies_ms != NULL && st->recorded_count < cfg->messages_per_thread) {
        st->latencies_ms[st->recorded_count++] = elapsed;
    }

    // Revisa si el broker respondió OK o error 
    if (resp.status == PROTO_STATUS_OK) {
        st->ok_count++;
    } else {
        st->err_count++;
        }

    }

    // Cierra el socket TCP 
    close(fd);
    return NULL;

}

// Comparador para qsort para valores double 
static int cmp_double(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
if (da < db)
    return -1;
 if (da > db)
     return 1;

return 0;

}

// Ejecuta el escenario multihilo y devuelve el tiempo 
static double run_scenario(const bench_cfg_t *cfg, worker_stats_t *stats)
{
 pthread_t *threads = calloc((size_t)cfg->threads, sizeof(pthread_t));
if (threads == NULL) {
    for (int i = 0; i < cfg->threads; i++) {
        stats[i].err_count += cfg->messages_per_thread;
    }
    
return 0.0;
    }

double start_ms = now_ms();

for (int i = 0; i < cfg->threads; i++) {
    stats[i].thread_id = i;
    stats[i].cfg = cfg;
    if (pthread_create(&threads[i], NULL, worker_routine, &stats[i]) != 0) {
        stats[i].err_count += cfg->messages_per_thread;
        
    }
   
}

for (int i = 0; i < cfg->threads; i++) {
   pthread_join(threads[i], NULL);
    
}

double elapsed_total = now_ms() - start_ms;
free(threads);
    
return elapsed_total;
}

// Calcula throughput 
static double measure_throughput(long ok_count, double total_ms)
{
    double secs = total_ms / 1000.0;
    if (secs <= 0.0 || ok_count <= 0)
        return 0.0;
    return (double)ok_count / secs;
}

// Calcula percentiles p50 y p99 de latencia 
static void measure_latency_p50_p99(const worker_stats_t *stats, int threads, double *out_p50, double *out_p99)
{
size_t total_samples = 0;
for (int i = 0; i < threads; i++) {
    total_samples += (size_t)stats[i].recorded_count;
}

if (total_samples == 0) {
    *out_p50 = 0.0;
    *out_p99 = 0.0;
    return;
}

double *all = malloc(total_samples * sizeof(double));
if (all == NULL) {
    *out_p50 = 0.0;
    *out_p99 = 0.0;
    return;
}

size_t idx = 0;
for (int i = 0; i < threads; i++) {
    for (int j = 0; j < stats[i].recorded_count; j++) {
        all[idx++] = stats[i].latencies_ms[j];
    }
}

qsort(all, total_samples, sizeof(double), cmp_double);

 size_t idx_p50 = (size_t)(0.50 * (double)(total_samples - 1));
 size_t idx_p99 = (size_t)(0.99 * (double)(total_samples - 1));

 *out_p50 = all[idx_p50];
 *out_p99 = all[idx_p99];
 
 free(all);
}

//Crea archivo CSV con los resultados 
static bool export_csv(const char *path, const bench_cfg_t *cfg, const bench_summary_t *sum)
{
 if (path == NULL || path[0] == '\0')
      return true;

    FILE *fp = fopen(path, "w");
    if (fp == NULL) {
        perror("fopen csv");
     return false;
    }

fprintf(fp,
        "topic,threads,messages_per_thread,payload_size,acks,"
        "total_msgs,ok,err,total_ms,throughput_msg_s,avg_latency_ms,"
        "max_latency_ms,p50_ms,p99_ms\n");
fprintf(fp,
        "%s,%d,%d,%d,%d,%ld,%ld,%ld,%.3f,%.2f,%.3f,%.3f,%.3f,%.3f\n",
        cfg->topic,
        cfg->threads,
        cfg->messages_per_thread,
        cfg->payload_size,
        (int)cfg->acks,
        sum->total_msgs,
        sum->total_ok,
        sum->total_err,
        sum->total_ms,
        sum->throughput_msg_s,
        sum->avg_latency_ms,
        sum->max_latency_ms,
        sum->p50_ms,
        sum->p99_ms);

fclose(fp);

return true;

}

int main(int argc, char **argv)
{
    
bench_cfg_t cfg;
if (!parse_bench_args(argc, argv, &cfg)) {
     print_usage(argv[0]);
    
     return EXIT_FAILURE;
    }

worker_stats_t *stats = calloc((size_t)cfg.threads, sizeof(worker_stats_t));
if (stats == NULL) {
     fprintf(stderr, "Error reservando memoria para estadisticas\n");
    
     return EXIT_FAILURE;
    }

for (int i = 0; i < cfg.threads; i++) {
      stats[i].latencies_ms = calloc((size_t)cfg.messages_per_thread, sizeof(double));
 }

double total_ms = run_scenario(&cfg, stats);

long total_ok = 0;
long total_err = 0;
double sum_lat_ms = 0.0;
double max_lat_ms = 0.0;

for (int i = 0; i < cfg.threads; i++) {
    total_ok += stats[i].ok_count;
    total_err += stats[i].err_count;
    sum_lat_ms += stats[i].total_latency_ms;
    if (stats[i].max_latency_ms > max_lat_ms) {
        max_lat_ms = stats[i].max_latency_ms;
    }
    
}

double p50_ms = 0.0;
double p99_ms = 0.0;
measure_latency_p50_p99(stats, cfg.threads, &p50_ms, &p99_ms);

bench_summary_t sum = {
    .total_msgs = total_ok + total_err,
    .total_ok = total_ok,
    .total_err = total_err,
    .total_ms = total_ms,
    .throughput_msg_s = measure_throughput(total_ok, total_ms),
    .avg_latency_ms = (total_ok > 0) ? (sum_lat_ms / (double)total_ok) : 0.0,
    .max_latency_ms = max_lat_ms,
    .p50_ms = p50_ms,
    .p99_ms = p99_ms,
};

printf("Resultados benchmark:\n");
printf("  Hilos: %d | Mensajes/hilo: %d | Total: %ld (OK=%ld, ERR=%ld)\n",
        cfg.threads, cfg.messages_per_thread, sum.total_msgs, sum.total_ok, sum.total_err);
printf("  Tiempo total: %.2f ms (%.3f s)\n", sum.total_ms, sum.total_ms / 1000.0);
printf("  Throughput: %.2f msg/s\n", sum.throughput_msg_s);
printf("  Latencia prom: %.3f ms | p50: %.3f ms | p99: %.3f ms | max: %.3f ms\n",
        sum.avg_latency_ms, sum.p50_ms, sum.p99_ms, sum.max_latency_ms);

if (!export_csv(cfg.csv_path, &cfg, &sum)) {
    for (int i = 0; i < cfg.threads; i++) {
        free(stats[i].latencies_ms);
    }
    free(stats);
    return EXIT_FAILURE;
}

for (int i = 0; i < cfg.threads; i++) {
    free(stats[i].latencies_ms);
}
free(stats);

return (sum.total_err == 0 && sum.total_ok > 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
