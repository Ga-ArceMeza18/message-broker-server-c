#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

#include "broker.h"

/*            INSTRUCTIONS TO RUN THE BROKER            */    
/*  mpirun -np N ./brokerd [puerto_base] [directorio_datos] 
    - N: number of brokers to run
    - puerto_base: base port for brokers (default: 9000)
    - directorio_datos: directory to store broker data (default: "data")
*/ 


// main: temporal main function to run the broker
int main(int argc, char **argv) {
    int provided = 0;
    if (MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &provided) != MPI_SUCCESS) {
        fprintf(stderr, "brokerd: no se pudo inicializar MPI\n");
        return 1;
    }
    if (provided < MPI_THREAD_SERIALIZED) {
        fprintf(stderr, "brokerd: MPI no ofrece MPI_THREAD_SERIALIZED\n");
        MPI_Finalize();
        return 1;
    }

    int rank = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int base_port = (argc > 1) ? atoi(argv[1]) : 9000;
    const char *data_dir = (argc > 2) ? argv[2] : "data";

    char name[32];
    char path[512];
    snprintf(name, sizeof name, "broker%d", rank);
    snprintf(path, sizeof path, "%s/%s", data_dir, name);

    broker_t broker;
    if (broker_init(&broker, name, "localhost", base_port + rank, rank, path) != 0) {
        fprintf(stderr, "brokerd[%d]: fallo en broker_init\n", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int rc = broker_run(&broker);
    if (rc != 0) {
        fprintf(stderr, "brokerd[%d]: fallo en broker_run (puerto %d)\n", rank, broker.port);
    }

    broker_shutdown(&broker);
    broker_destroy(&broker);

    if (rc != 0) {
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    MPI_Finalize();
    return 0;
}