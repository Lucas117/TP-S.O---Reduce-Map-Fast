/*
 * FuncionesFS.h
 *
 *  Created on: 4/6/2015
 *      Author: utnso
 */
#include <stdio.h>
#include "EstructurasFS.h"
#include <commons/collections/list.h>
#include <commons/log.h>
#include <commons/txt.h>
#ifndef FUNCIONESFS_H_
#define FUNCIONESFS_H_


#define PATH "archivoFS.conf"
#define PATH_ARCHIVOS "archivos.dat"
#define PATH_DIRECTORIOS "directorios.dat"
#define PATH_LOG "LogFS.log"
#define PATH_NODOS "nodos.dat"
#define TAMANIO_BLOQUE 20*1024*1024

void loguearme(char *ruta, char *nombre_proceso,bool mostrar_por_consola ,t_log_level nivel, const char *mensaje);
void espacioDisponibleDeMDFS(t_list* nodosEnSistema);
void mostrarNodos(t_list* nodos);
void recuperarArchivoDelMDFS(char*,char*,char*,t_list* archivos,t_list* directorios,t_list* nodosEnSistema, conexionMarta*, int, t_list*, char*);
void persistirNodos(t_list* nodos);
t_list* leerNodosPersistidos();
int armarBloque(char *bloque_completo,int tamanio_aux);
void armarBloqueFinal(char *bloque_final, int cantidad_escrita);
void agregarArchivoAlMDFS(char *archivo_a_agregar, char* indice, t_list *nodos_sistema, pthread_mutex_t *mutexFD, t_list * archivos,t_list* directorios, int nodosMinimo);
lectura* leerConfiguracion();
void liberarLectura(lectura*);
void destruirDirectorio(directorio*);
void eliminarBloque(char* nombreArchivo, char* directorioArchivo,
		char* numeroBloque, char* nombreNodo, t_list* archivos,
		t_list* nodosEnSistema, pthread_mutex_t* mutexListasNodos);
void copiarBloque(char* nombreArchivo, char* directorioArchivo, char* numeroBloque, char* nombreNodo, t_list* archivos, t_list* nodosEnSistema, char* puerto_escucha, pthread_mutex_t* mutexNodos);
void destruirNodo(datosNodo*);
void crearDirectorio(char*,char*,t_list*);
void moverDirtectorio(char*, char*, char*, t_list*);
void borrarDirectorio(char*, char*, t_list*, t_list*, t_list*);
void mostrarDirectorios(t_list*);
void renombrarDirectorio(char*,char*,char*, t_list*);
void mostrarArchivos(t_list*,t_list*);
void borrarArchivo(char*, char*, t_list*, t_list*);
void renombrarArchivo(char*, char*, char*, t_list*);
void moverArchivo(char*, char*, char*, t_list*, t_list*);
void persistirUnDirectorio(directorio*);
t_list* leerDirectoriosPersistidos();
void  persistirDirectorios(t_list*);
void destruirArchivo(archivo*);
void verBloques(char*, char*, t_list*);
t_list* leerArchivosPersistidos();
void persistirArchivos(t_list*);
void agregarNodo(t_list* espera, t_list* sistema, conexionMarta* conexion_Marta, int nodosMinimo, pthread_mutex_t *mutexListasNodos);
void eliminarNodo(char* ip, char* nombreNodo, t_list* sistema, pthread_mutex_t *mutexListasNodos);
void formatearMDFS(t_list*, t_list*, t_list*);
infoCopia* enviarBloqueCortado(char* datos, t_list* nodosEnSistema,pthread_mutex_t* mutexFD);



#endif /* FUNCIONESFS_H_ */
