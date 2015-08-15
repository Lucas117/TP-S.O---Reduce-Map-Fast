/*
 ============================================================================
 Name        : Proceso.c

 Author      : Leandro Wagner, Lucas Vergñory, Agustín Di Prinzio, Miguel Arrondo, Franco Vare
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */
#include "funcionesJob.h"
#include <stdio.h>
#include <stdlib.h>

int main(void) {
	puts("!!!Hola Job!!!"); /* prints !!!Hola Job!!! */

	lectura* resultado_lectura;

			if ((resultado_lectura = leerConfiguracion()) == NULL) {
				return EXIT_FAILURE;}

	script* resultado = mapearScriptsAMemoria(resultado_lectura->mapper,
			resultado_lectura->reducer);

	conexionMarta* martu = malloc(sizeof(conexionMarta));
	martu->ip_Marta = malloc(16);
	strcpy(martu->ip_Marta,resultado_lectura->ipMarta);
	martu->puerto_Principal = malloc(6);
	strcpy(martu->puerto_Principal,resultado_lectura->puertoMarta);

	establecerConexiones(resultado_lectura->puertoSalida, martu,
			resultado_lectura->archivos, resultado_lectura->combiner,
			resultado, resultado_lectura->nombreJob, resultado_lectura->resultado);


	free(resultado);

	liberarLectura(resultado_lectura);
	free(resultado_lectura);

	return EXIT_SUCCESS;
}
