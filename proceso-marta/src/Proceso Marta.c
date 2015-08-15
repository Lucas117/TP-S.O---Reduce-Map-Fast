/*
 ============================================================================
 Name        : Proceso.c
 Author      : Leandro Wagner, Lucas Vergñory, Agustín Di Prinzio, Miguel Arrondo, Franco Vare
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <commons/collections/list.h>
#include "hSockets.h"
#include "FuncionesMARTA.h"

int main(void) {
	puts("!!!Hola MaRTA!!!"); /* prints !!!Hello World!!! */

	lectura* resultado_lectura;

	if ((resultado_lectura = leerConfiguracion()) == NULL) {
		return EXIT_FAILURE;
	}

	char* puerto_escucha = malloc(6);
	char* puerto_escucha_ActualizadorLista = malloc(6);

	strcpy(puerto_escucha,resultado_lectura->puertoPrincipal);
	strcpy(puerto_escucha_ActualizadorLista, resultado_lectura->puertoActualizacion);

	free(resultado_lectura);

	t_list* listaNodosEnElSitema = list_create();
	t_list* cargaNodos = list_create();
	t_list* jobs = list_create();
	t_list* jobsEnEsperaResultadoGuardado = list_create();

	escucharConexiones(puerto_escucha, puerto_escucha_ActualizadorLista, listaNodosEnElSitema, cargaNodos, jobs, jobsEnEsperaResultadoGuardado);

	return EXIT_SUCCESS;
}

