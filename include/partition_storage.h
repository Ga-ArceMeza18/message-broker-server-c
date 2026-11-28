// partition_storage.h - Manejo del almacenamiento de particiones para los topics del broker
#ifndef PARTITION_STORAGE_H
#define PARTITION_STORAGE_H

#include "cluster_metadata.h"

/*
 * Abre los logs de las particiones donde local_rank
 * pertenece al conjunto de replicas.
 *
 * Retorna 0 si todo sale bien y -1 si ocurre un error.
 */
int partition_storage_open(cluster_metadata_t *metadata, const char *storage_path, int local_rank);

/*
 * Cierra todos los logs abiertos en los topics.
 * Debe ejecutarse cuando ya no existan operaciones
 * concurrentes sobre ellos.
 */
void partition_storage_close(cluster_metadata_t *metadata);

#endif
