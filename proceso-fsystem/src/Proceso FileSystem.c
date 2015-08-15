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
#include <commons/string.h>
#include <commons/collections/list.h>
#include "FuncionesFS.h"
#include "EstructurasFS.h"
#include "hSockets.h"
#include <pthread.h>

void mostrarConsola();
void darMd5();
char* darPalabra(char*, int);
int contarPalabras(char*);
int contarLetras(char*);

pthread_mutex_t mutexListasNodos = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexArchivosYDirectorios = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexConexionMaRTA = PTHREAD_MUTEX_INITIALIZER;

conexionMarta* conexion_Marta;
int nodosMinimo;
char* puerto_escucha;

int main(void) {
	//puts("!!!Hola File System!!!"); /* prints !!!Hola File System!!! */
	loguearme(PATH_LOG, "Proceso File System", 0, 2,
			"Comienza la ejecucion del proceso.");

	lectura* resultado_lectura;

	if ((resultado_lectura = leerConfiguracion()) == NULL) {
		return EXIT_FAILURE;
	}

	puerto_escucha = malloc(6);
	strcpy(puerto_escucha, resultado_lectura->puertoListen);

	conexion_Marta = malloc(sizeof(conexionMarta));

	conexion_Marta->ip_Marta = malloc(16);
	strcpy(conexion_Marta->ip_Marta, resultado_lectura->ipMarta);

	conexion_Marta->puerto_Principal = malloc(6);
	strcpy(conexion_Marta->puerto_Principal,
			resultado_lectura->puertoMPrincipal);

	conexion_Marta->puerto_ActualizadorLista = malloc(6);
	strcpy(conexion_Marta->puerto_ActualizadorLista,
			resultado_lectura->puertoMAct);

	nodosMinimo = resultado_lectura->nodos_minimo;

	liberarLectura(resultado_lectura);
	free(resultado_lectura);

	t_list * directorios = leerDirectoriosPersistidos();

	t_list * archivos = leerArchivosPersistidos();

	t_list* nodosEnEspera = list_create();
	t_list* nodosEnSistema = leerNodosPersistidos();

	pthread_t hiloEscucha;

	void* argumentos[10];

	argumentos[0] = puerto_escucha;
	argumentos[1] = conexion_Marta;
	argumentos[2] = nodosEnSistema;
	argumentos[3] = nodosEnEspera;
	argumentos[4] = archivos;
	argumentos[5] = directorios;
	argumentos[6] = &nodosMinimo;
	argumentos[7] = &mutexListasNodos;
	argumentos[8] = &mutexArchivosYDirectorios;
	argumentos[9] = &mutexConexionMaRTA;

	pthread_mutex_lock(&mutexConexionMaRTA);

	pthread_create(&hiloEscucha, NULL, hiloParaEscuchar, argumentos);

	pthread_mutex_lock(&mutexConexionMaRTA);
	pthread_mutex_unlock(&mutexConexionMaRTA);
	pthread_mutex_destroy(&mutexConexionMaRTA);

	mostrarConsola(directorios, archivos, nodosEnEspera, nodosEnSistema);

	return EXIT_SUCCESS;
}

void mostrarConsola(t_list* directorios, t_list* archivos,
		t_list* nodosEnEspera, t_list* nodosEnSistema) {

	char* comando; //string_new() -> esto no funciona o no se hacerlo, el problema del strcmp que puse mas abajo se fue cuando cambie esto por un malloc(128)
	char** p;
	int x, i;
	system("clear");
	printf(
			"Para ver los todos los comandos use el comando ayuda\n****************************************************\n");
	while (1) {
		printf("> ");
		comando = malloc(130);
		scanf("%129[^\n]s", comando);
		getchar();
		if (contarLetras(comando) > 128) { // los comandos tienen limite de 128 caracteres (lo podemos agrandar)
			printf("el comando es muy largo\n");
			while (getchar() != '\n') { // este while consume caracteres que quedaron colgados en stdin, lo puse porque
			} //                           cuando ponia un string de mas de 128 caracteres y lo mandaba a escribir de nuevo
		} else { //                        en lugar de guardar lo que escrbia el usuario me guardaba los caracteres que se habian pasado de 128 antes
			x = contarPalabras(comando);
			p = malloc(x * (sizeof(char*))); //       creo un puntero a puntero para guardar los punteros a las palabras
			for (i = 0; i < x; i++) { //              que me crea darPalabra para poder hacerles free despues
				p[i] = darPalabra(comando, i + 1); // p[0] es el puntero a la primera palabra, p[1] a la segunda, y asi
			}
			//			for (i = 0; i < x; i++) {
			//				printf("%s\n",p[i]);
			//			}
			if (!strcmp("ayuda", p[0])) { //      cuando hace el strcmp me modifica el string
				printf("formatearMDFS\n"); //          original probar en debug comando: md5 /home/utnso/file1
				printf("mostrarArchivos\n");
				printf("borrarArchivo indiceDirectorio nombreArchivo \n"); //          (se fue cuando cambie string_new() por malloc(128)
				printf(
						"renombrarArchivo indiceDirectorio nombreActual nombreNuevo\n");
				printf(
						"moverArchivo indiceDirectorioActual NombreArchivo indiceDirectorioNuevo\n");
				printf("agregarArchivoAlMDFS rutaArchivo indiceDirectorio\n");
				printf(
						"recuperarArchivoDelMDFS nombreArchivo indiceDirectorio rutaDestino\n");
				printf("crearDirectorio indicePadre nombreDirectorio\n");
				printf(
						"moverDirectorio indicePadreActual indicePadreNuevo nombreDirectorio\n");
				printf("borrarDirectorio indicePadre nombreDirectorio\n");
				printf("mostrarDirectorios\n");
				printf(
						"renombrarDirectorio indicePadre nombreActual nombreNuevo\n");
				printf("md5 nombreArchivo indiceDirectorio\n");
				printf("verBloques indiceDirectorio nombreArchivo\n");
				printf("eliminarBloque nombreArchivo directorioArchivo numeroDeBloque nombreNodo\n");
				printf(
						"copiarBloque nombreArchivo directorioArchivo numeroDeBloque nombreNodo\n");
				printf("copiarArchivoANodo nombreArchivo directorioArchivo nombreNodo");
				printf("agregarNodo\n");
				printf("mostrarNodos\n");
				printf("eliminarNodo ipNodo nombreNodo\n");
				printf("espacioDisponible\n");
				printf("exit\n");
			} else if (!strcmp("exit", p[0])) {
				for (i = 0; i < x; i++) { // libera la memoria que aloco en darPalabra(2)
					free(p[i]); //           aca a veces tira el free(): invalid next size (fast)
				} //                lo pude reproducir una sola vez despues de tirarle
				free(p);                  // un monton de comandos
				free(comando);
				persistirDirectorios(directorios);
				persistirArchivos(archivos);
				persistirNodos(nodosEnSistema);
				list_destroy_and_destroy_elements(directorios,
						(void*) destruirDirectorio);
				list_destroy_and_destroy_elements(archivos,
						(void*) destruirArchivo);
				list_destroy_and_destroy_elements(nodosEnEspera,
						(void*) destruirNodo);
				list_destroy_and_destroy_elements(nodosEnSistema,
						(void*) destruirNodo);
				return;
			} else if (!strcmp(p[0], "formatearMDFS")) {
				if (x < 1) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					pthread_mutex_lock(&mutexArchivosYDirectorios);
					formatearMDFS(archivos, nodosEnSistema, directorios);
					pthread_mutex_unlock(&mutexArchivosYDirectorios);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "crearDirectorio")) {
				if (x < 3) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					pthread_mutex_lock(&mutexArchivosYDirectorios);
					crearDirectorio(p[1], p[2], directorios);
					pthread_mutex_unlock(&mutexArchivosYDirectorios);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "moverDirectorio")) {
				if (x < 4) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					pthread_mutex_lock(&mutexArchivosYDirectorios);
					moverDirectorio(p[1], p[2], p[3], directorios);
					pthread_mutex_unlock(&mutexArchivosYDirectorios);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "espacioDisponible")) {
				if (x < 1) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {
					espacioDisponibleDeMDFS(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "borrarDirectorio")) {
				if (x < 3) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					pthread_mutex_lock(&mutexArchivosYDirectorios);
					borrarDirectorio(p[1], p[2], directorios, archivos,
							nodosEnSistema);
					pthread_mutex_unlock(&mutexArchivosYDirectorios);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "mostrarDirectorios")) {
				if (x < 1) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					mostrarDirectorios(directorios);
				}
			} else if (!strcmp(p[0], "renombrarDirectorio")) {
				if (x < 4) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					pthread_mutex_lock(&mutexArchivosYDirectorios);
					renombrarDirectorio(p[1], p[2], p[3], directorios);
					pthread_mutex_unlock(&mutexArchivosYDirectorios);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "mostrarArchivos")) {
				if (x < 1) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					mostrarArchivos(archivos, nodosEnSistema);
				}
			} else if (!strcmp(p[0], "borrarArchivo")) {
				if (x < 3) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					pthread_mutex_lock(&mutexArchivosYDirectorios);
					borrarArchivo(p[1], p[2], archivos, nodosEnSistema);
					pthread_mutex_unlock(&mutexArchivosYDirectorios);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "renombrarArchivo")) {
				if (x < 4) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					pthread_mutex_lock(&mutexArchivosYDirectorios);
					renombrarArchivo(p[1], p[2], p[3], archivos);
					pthread_mutex_unlock(&mutexArchivosYDirectorios);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "moverArchivo")) {
				if (x < 4) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					pthread_mutex_lock(&mutexArchivosYDirectorios);
					moverArchivo(p[1], p[2], p[3], archivos, directorios);
					pthread_mutex_unlock(&mutexArchivosYDirectorios);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "agregarArchivoAlMDFS")) {
				if (x < 3) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					pthread_mutex_lock(&mutexArchivosYDirectorios);
					agregarArchivoAlMDFS(p[1], p[2], nodosEnSistema,
							&mutexListasNodos, archivos, directorios,
							nodosMinimo);
					pthread_mutex_unlock(&mutexArchivosYDirectorios);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "recuperarArchivoDelMDFS")) {
				if (x < 4) {
					printf("falta uno o mas parametros\n");
					continue;
				}
				recuperarArchivoDelMDFS(p[1], p[2], p[3], archivos, directorios,
						nodosEnSistema, conexion_Marta, nodosMinimo,
						nodosEnEspera, puerto_escucha);
			} else if (!strcmp(p[0], "md5")) {
				if (x < 3) {
					printf("falta uno o mas parametros\n");
					continue;
				}
				darMd5(p[1], p[2], archivos, directorios, nodosEnSistema,
						conexion_Marta, nodosMinimo, nodosEnEspera,
						puerto_escucha);
			} else if (!strcmp(p[0], "verBloques")) {
				if (x < 3) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					verBloques(p[1], p[2], archivos);
				}
			} else if (!strcmp(p[0], "eliminarBloque")) {
				if (x < 5) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

									eliminarBloque(p[1], p[2], p[3], p[4],archivos, nodosEnSistema, &mutexListasNodos);
									persistirDirectorios(directorios);
									persistirArchivos(archivos);
									persistirNodos(nodosEnSistema);
								}
			} else if (!strcmp(p[0], "copiarBloque")) {
				if (x < 5) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					copiarBloque(p[1], p[2], p[3], p[4], archivos, nodosEnSistema,
							puerto_escucha, &mutexListasNodos);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "agregarNodo")) {
				if (x < 1) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {
					agregarNodo(nodosEnEspera, nodosEnSistema, conexion_Marta,
							nodosMinimo, &mutexListasNodos);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else if (!strcmp(p[0], "mostrarNodos")) {
				if (x < 1) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {

					mostrarNodos(nodosEnSistema);

				}
			} else if (!strcmp(p[0], "eliminarNodo")) {
				if (x < 3) {
					printf("falta uno o mas parametros\n");
					continue;
				} else {
					eliminarNodo(p[1], p[2], nodosEnSistema, &mutexListasNodos);
					persistirDirectorios(directorios);
					persistirArchivos(archivos);
					persistirNodos(nodosEnSistema);
				}
			} else
				printf("no entiendo ese comando\n");
			for (i = 0; i < x; i++) { // libera la memoria que aloco en darPalabra(2)
				free(p[i]); //           aca a veces tira el free(): invalid next size (fast)
			}   //            lo pude reproducir una sola vez despues de tirarle
			free(p);                  // un monton de comandos
			free(comando);
		}

	}
	return;
}

// cuenta los espacios y le suma uno
// funciona bien si pones muchos espacios juntos por error
int contarPalabras(char* str) {
	int palabras = 1, i = 0;
	while (str[i] == '\ ') // se mueve hasta la primer letra si empieza con espacios
		i++;
	while (str[i] != '\0') {
		if (str[i] == '\ ' && str[i + 1] != '\ ' && str[i + 1] != '\0')
			palabras++;
		i++;
	}
	return palabras;
}

//cuenta las letras hasta el \0
int contarLetras(char* str) {
	int i = 0;
	while (str[i] != '\0') {
		i++;
	}
	return i;
}
//te devuelve la palabra n° pos del string str
//tambien funciona si pones espacios de mas
char* darPalabra(char* str, int pos) {
	int i = 0, j, k = 0;
	int primerLetra, largoPalabra;
	int ultimoEspacio = -1;
	for (j = 0; j < pos; j++) {
		while (str[i] == '\ ') {
			ultimoEspacio = i;
			i++;
		}
		while (str[i] != '\ ' && str[i] != '\0') {
			i++;
		}
	}
	primerLetra = ultimoEspacio + 1;
	largoPalabra = i - ultimoEspacio;
	char* palabra = malloc(largoPalabra * (sizeof(char)));
	while (k < largoPalabra) {
		palabra[k] = str[primerLetra + k];
		k++;
	}
	palabra[k - 1] = '\0';
	return palabra;
}
//printea el md5 del archivo por system call
void darMd5(char* nombreArchivo, char* directorioArchivo, t_list* archivos,
		t_list* directorios, t_list* nodosEnSistema,
		conexionMarta* conexionMarta, int nodosMinimos, t_list* nodosEnEspera,
		char* puerto_escucha) {
	recuperarArchivoDelMDFS(nombreArchivo, directorioArchivo, "/tmp", archivos,
			directorios, nodosEnSistema, conexionMarta, nodosMinimos,
			nodosEnEspera, puerto_escucha);
	char *md5 = malloc(128);
	strcpy(md5, "md5sum ");
	strcat(md5, "/tmp/");
	strcat(md5, nombreArchivo);
	system(md5);
	strcpy(md5, "/tmp/");
	strcat(md5, nombreArchivo);
	remove(md5);
	free(md5);
}

