/*
 ============================================================================
 Name        : Proceso.c
 Author      : Leandro Wagner, Lucas Vergñory, Agustín Di Prinzio, Miguel Arrondo, Franco Vare 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

/*
 ============================================================================
 Name        : Proceso.c
 Author      : Leandro Wagner, Lucas Vergñory, Agustín Di Prinzio, Miguel Arrondo, Franco Vare
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */
#include "funcionesNodo.h"
#include "hSockets.h"


int main(void) {

	puts("!!!Hola Nodo!!!"); /* prints !!!Hello World!!! */

	loguearme(PATH_LOG,"Proceso Nodo",0,2,"Comienza la ejecucion del proceso.");

	lectura* resultado_lectura;

		if ((resultado_lectura = leerConfiguracion()) == NULL) {
			loguearme(PATH_LOG,"Proceso Nodo",0,4,"Se termino la ejecucion, no existe .config.");
			loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,"Proceso Terminado!");
			return EXIT_FAILURE;}
			//aca usando resultado_lectura sacas los valores, y en el free borra todo
			//se conecta al FS con su IP y Puerto y se queda escuchando

		inicializarNodo(resultado_lectura->tamanioNodo,resultado_lectura->dirTemp);

		crearArchivoData(resultado_lectura->nombreBin, resultado_lectura->tamanioNodo);//creo el archivo data solamente si no existe

		mapearDataBinAMemoria(resultado_lectura->nombreBin);

		escucharConexiones(resultado_lectura->ipFS, resultado_lectura->puertoFs, resultado_lectura->puertoEscucha, resultado_lectura->nombreNodo, resultado_lectura->tamanioNodo);

	return EXIT_SUCCESS;
}
