// Pruebas unitarias para la estructura consumer_group_t y sus funciones asociadas.

#include "consumer_group_t.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define RUN_TEST(test) do {                                      \
    printf("\n[TEST] %s\n", #test);                              \
    if (test() == 0) {                                           \
        printf("  [OK] Prueba completada correctamente.\n");  \
        tests_passed++;                                          \
    } else {                                                     \
        printf("  [FAIL] Prueba fallida.\n");                    \
        tests_failed++;                                          \
    }                                                            \
} while (0)

#define CHECK(condition) do {                                    \
    if (!(condition)) {                                          \
        printf("  [ERROR] Linea %d: %s\n",                       \
               __LINE__, #condition);                            \
        return 1;                                                \
    }                                                            \
} while (0)


// Comprueba la inicializacion del grupo.
int test_group_init(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar la inicializacion de un grupo.\n");
    printf("  - Inicializar un grupo con 5 particiones.\n");
    printf("  - Comprobar su identificador y generacion inicial.\n");
    printf("  - Verificar que no contiene miembros.\n");
    printf("  - Comprobar que las asignaciones y offsets inician en -1.\n");

    CHECK(group_init(&group, "test_group", 5) == 0);

    CHECK(strcmp(group.group_id, "test_group") == 0);
    CHECK(group.generation == 0);
    CHECK(group.partition_count == 5);
    CHECK(group.member_count == 0);
    CHECK(group.member_capacity == 0);

    for (int i = 0; i < 5; i++) {
        CHECK(group.assignment[i] == -1);
        CHECK(group.committed_offsets[i] == -1);
    }

    group_destroy(&group);

    return 0;
}


// Comprueba argumentos invalidos durante la inicializacion.
int test_invalid_init(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar el rechazo de parametros invalidos.\n");
    printf("  - Rechazar un identificador NULL.\n");
    printf("  - Rechazar un identificador vacio.\n");
    printf("  - Rechazar una cantidad de particiones igual a cero.\n");
    printf("  - Rechazar una cantidad de particiones negativa.\n");

    CHECK(group_init(&group, NULL, 5) == -1);
    CHECK(group_init(&group, "", 5) == -1);
    CHECK(group_init(&group, "test", 0) == -1);
    CHECK(group_init(&group, "test", -1) == -1);

    return 0;
}


// Comprueba la incorporacion de varios miembros.
int test_group_join(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar la incorporacion de miembros.\n");
    printf("  - Registrar tres miembros con descriptores distintos.\n");
    printf("  - Comprobar que cada miembro recibe un identificador positivo.\n");
    printf("  - Verificar que los identificadores son unicos.\n");
    printf("  - Comprobar que el grupo contiene tres miembros.\n");

    CHECK(group_init(&group, "join_test", 6) == 0);

    int member1;
    int member2;
    int member3;

    CHECK(group_join(&group, 10, &member1) == 0);
    CHECK(group_join(&group, 11, &member2) == 0);
    CHECK(group_join(&group, 12, &member3) == 0);

    CHECK(member1 > 0);
    CHECK(member2 > 0);
    CHECK(member3 > 0);

    CHECK(member1 != member2);
    CHECK(member1 != member3);
    CHECK(member2 != member3);

    CHECK(group.member_count == 3);

    group_destroy(&group);

    return 0;
}


// Comprueba argumentos invalidos al incorporar miembros.
int test_invalid_join(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar el rechazo de incorporaciones invalidas.\n");
    printf("  - Rechazar un descriptor de cliente negativo.\n");
    printf("  - Rechazar un puntero NULL para recibir el identificador.\n");
    printf("  - Comprobar que ningun miembro se registra.\n");

    CHECK(group_init(&group, "invalid_join", 3) == 0);

    int member_id;

    CHECK(group_join(&group, -1, &member_id) == -1);
    CHECK(group_join(&group, 10, NULL) == -1);

    CHECK(group.member_count == 0);

    group_destroy(&group);

    return 0;
}


// Comprueba la distribucion Round Robin.
int test_round_robin(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar la distribucion Round Robin.\n");
    printf("  - Crear un grupo con 7 particiones.\n");
    printf("  - Incorporar tres miembros.\n");
    printf("  - Comprobar que las particiones se distribuyen alternadamente.\n");
    printf("  - Verificar que los miembros reciben 3, 2 y 2 particiones.\n");

    CHECK(group_init(&group, "rebalance_test", 7) == 0);

    int member1;
    int member2;
    int member3;

    CHECK(group_join(&group, 10, &member1) == 0);
    CHECK(group_join(&group, 11, &member2) == 0);
    CHECK(group_join(&group, 12, &member3) == 0);

    CHECK(group.assignment[0] == member1);
    CHECK(group.assignment[1] == member2);
    CHECK(group.assignment[2] == member3);
    CHECK(group.assignment[3] == member1);
    CHECK(group.assignment[4] == member2);
    CHECK(group.assignment[5] == member3);
    CHECK(group.assignment[6] == member1);

    CHECK(group.members[0].owned_partition_count == 3);
    CHECK(group.members[1].owned_partition_count == 2);
    CHECK(group.members[2].owned_partition_count == 2);

    group_destroy(&group);

    return 0;
}


// Comprueba el rebalanceo cuando se elimina un miembro.
int test_rebalance_after_leave(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar el rebalanceo despues de eliminar un miembro.\n");
    printf("  - Crear un grupo con 6 particiones y tres miembros.\n");
    printf("  - Eliminar al segundo miembro.\n");
    printf("  - Comprobar que quedan dos miembros.\n");
    printf("  - Verificar que las seis particiones se redistribuyen entre ellos.\n");

    CHECK(group_init(&group, "leave_test", 6) == 0);

    int member1;
    int member2;
    int member3;

    CHECK(group_join(&group, 10, &member1) == 0);
    CHECK(group_join(&group, 11, &member2) == 0);
    CHECK(group_join(&group, 12, &member3) == 0);
    CHECK(group_leave(&group, member2) == 0);

    CHECK(group.member_count == 2);

    for (int i = 0; i < 6; i++) {
        int expected = (i % 2 == 0) ? member1 : member3;
        CHECK(group.assignment[i] == expected);
    }

    CHECK(group.members[0].member_id == member1);
    CHECK(group.members[1].member_id == member3);

    group_destroy(&group);

    return 0;
}


// Comprueba la eliminacion de miembros inexistentes.
int test_invalid_leave(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar el rechazo de solicitudes de eliminacion invalidas.\n");
    printf("  - Incorporar un miembro valido.\n");
    printf("  - Intentar eliminar un miembro inexistente.\n");
    printf("  - Intentar eliminar un identificador negativo.\n");
    printf("  - Comprobar que el miembro original permanece registrado.\n");

    CHECK(group_init(&group, "invalid_leave", 3) == 0);

    int member_id;

    CHECK(group_join(&group, 10, &member_id) == 0);
    CHECK(group_leave(&group, 999) == -1);
    CHECK(group_leave(&group, -1) == -1);
    CHECK(group.member_count == 1);

    group_destroy(&group);

    return 0;
}


// Comprueba la eliminacion del ultimo miembro.
int test_leave_last_member(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar la eliminacion del ultimo miembro.\n");
    printf("  - Incorporar un miembro al grupo.\n");
    printf("  - Eliminar dicho miembro.\n");
    printf("  - Comprobar que el grupo queda vacio.\n");
    printf("  - Verificar que todas las particiones quedan sin asignar (-1).\n");

    CHECK(group_init(&group, "last_member", 3) == 0);

    int member_id;

    CHECK(group_join(&group, 10, &member_id) == 0);
    CHECK(group_leave(&group, member_id) == 0);

    CHECK(group.member_count == 0);

    for (int i = 0; i < 3; i++) {
        CHECK(group.assignment[i] == -1);
    }

    group_destroy(&group);

    return 0;
}


// Comprueba la actualizacion del heartbeat.
int test_group_heartbeat(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar la actualizacion del heartbeat.\n");
    printf("  - Incorporar un miembro al grupo.\n");
    printf("  - Registrar el timestamp anterior.\n");
    printf("  - Actualizar el heartbeat y comprobar que no retrocede.\n");
    printf("  - Rechazar un identificador de miembro inexistente.\n");

    CHECK(group_init(&group, "heartbeat_test", 3) == 0);

    int member_id;

    CHECK(group_join(&group, 10, &member_id) == 0);

    long previous = group.members[0].last_heartbeat;

    CHECK(group_heartbeat(&group, member_id) == 0);
    CHECK(group.members[0].last_heartbeat >= previous);
    CHECK(group_heartbeat(&group, 999) == -1);

    group_destroy(&group);

    return 0;
}


// Comprueba confirmaciones de offsets validas.
int test_group_commit(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar la confirmacion de offsets validos.\n");
    printf("  - Incorporar dos miembros al grupo.\n");
    printf("  - Confirmar un offset para una particion de cada miembro.\n");
    printf("  - Comprobar que los offsets quedan almacenados correctamente.\n");

    CHECK(group_init(&group, "commit_test", 4) == 0);

    int member1;
    int member2;

    CHECK(group_join(&group, 10, &member1) == 0);
    CHECK(group_join(&group, 11, &member2) == 0);

    CHECK(group_commit(&group, member1, 0, 10) == 0);
    CHECK(group_commit(&group, member2, 1, 20) == 0);

    CHECK(group.committed_offsets[0] == 10);
    CHECK(group.committed_offsets[1] == 20);

    group_destroy(&group);

    return 0;
}


// Comprueba confirmaciones invalidas.
int test_invalid_commit(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar el rechazo de confirmaciones de offsets invalidas.\n");
    printf("  - Rechazar un offset negativo.\n");
    printf("  - Rechazar una particion inexistente.\n");
    printf("  - Rechazar un miembro inexistente.\n");
    printf("  - Rechazar a un miembro que no posee la particion.\n");
    printf("  - Rechazar un offset menor al previamente confirmado.\n");

    CHECK(group_init(&group, "invalid_commit", 4) == 0);

    int member1;
    int member2;

    CHECK(group_join(&group, 10, &member1) == 0);
    CHECK(group_join(&group, 11, &member2) == 0);

    // Offset negativo.
    CHECK(group_commit(&group, member1, 0, -1) == -1);

    // Particion inexistente.
    CHECK(group_commit(&group, member1, 4, 10) == -1);

    // Miembro inexistente.
    CHECK(group_commit(&group, 999, 0, 10) == -1);

    // Miembro sin propiedad de la particion.
    CHECK(group_commit(&group, member2, 0, 10) == -1);

    // Offset regresivo.
    CHECK(group_commit(&group, member1, 0, 20) == 0);
    CHECK(group_commit(&group, member1, 0, 15) == -1);

    CHECK(group.committed_offsets[0] == 20);

    group_destroy(&group);

    return 0;
}


// Comprueba la descripcion del grupo.
int test_group_describe(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar la generacion de la descripcion del grupo.\n");
    printf("  - Incorporar un miembro al grupo.\n");
    printf("  - Solicitar la descripcion en un buffer de 1024 bytes.\n");
    printf("  - Comprobar que contiene el identificador del grupo.\n");
    printf("  - Verificar que incluye generacion, miembros y particiones.\n");

    CHECK(group_init(&group, "describe_test", 3) == 0);

    int member_id;

    CHECK(group_join(&group, 10, &member_id) == 0);

    char buffer[1024];

    CHECK(group_describe(&group, buffer, sizeof(buffer)) == 0);

    CHECK(strstr(buffer, "describe_test") != NULL);
    CHECK(strstr(buffer, "Generation:") != NULL);
    CHECK(strstr(buffer, "Members: 1") != NULL);
    CHECK(strstr(buffer, "Partitions: 3") != NULL);

    printf("  - Descripcion generada:\n");
    printf("    %s\n", buffer);

    group_destroy(&group);

    return 0;
}


// Comprueba el manejo de buffers invalidos o insuficientes.
int test_invalid_describe(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar el manejo de buffers invalidos o insuficientes.\n");
    printf("  - Rechazar un puntero NULL.\n");
    printf("  - Rechazar un buffer de tamaño cero.\n");
    printf("  - Rechazar un buffer demasiado pequeño.\n");

    CHECK(group_init(&group, "describe_test", 3) == 0);

    char buffer[2];

    CHECK(group_describe(&group, NULL, 10) == -1);
    CHECK(group_describe(&group, buffer, 0) == -1);
    CHECK(group_describe(&group, buffer, sizeof(buffer)) == -1);

    group_destroy(&group);

    return 0;
}


// Comprueba mas miembros que particiones.
int test_more_members_than_partitions(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar el comportamiento cuando hay mas miembros que particiones.\n");
    printf("  - Crear un grupo con dos particiones.\n");
    printf("  - Incorporar cuatro miembros.\n");
    printf("  - Comprobar que los primeros dos reciben una particion.\n");
    printf("  - Verificar que los otros dos permanecen sin particiones.\n");

    CHECK(group_init(&group, "capacity_test", 2) == 0);

    int members[4];

    for (int i = 0; i < 4; i++) {
        CHECK(group_join(&group, 10 + i, &members[i]) == 0);
    }

    CHECK(group.member_count == 4);

    CHECK(group.members[0].owned_partition_count == 1);
    CHECK(group.members[1].owned_partition_count == 1);
    CHECK(group.members[2].owned_partition_count == 0);
    CHECK(group.members[3].owned_partition_count == 0);

    group_destroy(&group);

    return 0;
}


// Comprueba un grupo sin miembros.
int test_empty_group(void) {
    consumer_group_t group;

    printf("  Objetivo: Verificar el rebalanceo de un grupo sin miembros.\n");
    printf("  - Inicializar un grupo sin incorporar miembros.\n");
    printf("  - Ejecutar el rebalanceo manualmente.\n");
    printf("  - Comprobar que el grupo sigue vacio.\n");
    printf("  - Verificar que todas las particiones permanecen sin asignar.\n");

    CHECK(group_init(&group, "empty_test", 3) == 0);

    CHECK(group_rebalance(&group) == 0);
    CHECK(group.member_count == 0);

    for (int i = 0; i < 3; i++) {
        CHECK(group.assignment[i] == -1);
    }

    group_destroy(&group);

    return 0;
}


int main(void) {
    printf("====================================\n");
    printf("       CONSUMER GROUP UNIT TESTS\n");
    printf("====================================\n");

    RUN_TEST(test_group_init);
    RUN_TEST(test_invalid_init);
    RUN_TEST(test_group_join);
    RUN_TEST(test_invalid_join);
    RUN_TEST(test_round_robin);
    RUN_TEST(test_rebalance_after_leave);
    RUN_TEST(test_invalid_leave);
    RUN_TEST(test_leave_last_member);
    RUN_TEST(test_group_heartbeat);
    RUN_TEST(test_group_commit);
    RUN_TEST(test_invalid_commit);
    RUN_TEST(test_group_describe);
    RUN_TEST(test_invalid_describe);
    RUN_TEST(test_more_members_than_partitions);
    RUN_TEST(test_empty_group);

    printf("\n====================================\n");
    printf("RESULTADOS\n");
    printf("====================================\n");

    printf("Pruebas exitosas: %d\n", tests_passed);
    printf("Pruebas fallidas: %d\n", tests_failed);
    printf("Total: %d\n", tests_passed + tests_failed);

    if (tests_failed == 0) {
        printf("\nTodas las pruebas finalizaron correctamente.\n");
    } else {
        printf("\nSe encontraron errores durante las pruebas.\n");
    }

    return tests_failed == 0 ? 0 : 1;
}
