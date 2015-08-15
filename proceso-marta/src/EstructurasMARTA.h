/*
 * EstructurasMARTA.h
 *
 *  Created on: 3/6/2015
 *      Author: utnso
 */

#include <stdio.h>
#include <commons/collections/list.h>

#ifndef ESTRUCTURASMARTA_H_
#define ESTRUCTURASMARTA_H_

typedef struct {
	char* ip;
	char* puerto;
	int nodosMinimo;
	int fd_FileSystem;
	int disponible;
} fileSystem;

typedef struct {
	char* puertoPrincipal;
	char* puertoActualizacion;
} lectura;

typedef struct {
	char* ip;
	char* puerto;
	int nroBloque;
	int nombreNodo;
} infoCopia; //Estructura utilizada para documentar las copias de cada bloque, dentro de la estructura datoBloque

typedef struct {
	int nrobloque;
	t_list* copias;
	int numeroDeCopias;
} datoBloque; //Estructura utilizada para la lista enlazada de ubicacion de copias del archivo en los nodos dentro de la estructura archivo

typedef struct {
	char* ipJob;
	char* puertoJob;
	char* direccionArchivo;
	t_list* listaBloques;
} archivo; //Estructura utilizada para la lista enlazada con informacion de los archivos del FS

typedef struct {
	int nombreNodo;
	char* ip;
	char* puerto;
	int disponible;
} datosNodo; //Estructura utilizada para lista enlazada de nodos disponible que tiene tanto FileSystem como MARTA

typedef struct {
	int nombreNodo;
	char* ip;
	char* puerto;
	int carga;
} cargaNodo;

typedef struct {
	char* ipNodo;
	char* puertoNodo;
	int bloqueNodo;
	int terminado;

} instruccionMapReduce;

typedef struct {

	char* ipNodoPrincipal;
	char* puertoNodoPrincipal;
	t_list* instruccionesMap;
	t_list* instruccionesReduce;

} instrucciones;

typedef struct {
	char* ip;
	char* puerto;
	char* direccionArchivo;
	int combiner;
	t_list* datosBloque;
	instrucciones* instrucciones;
} job;

typedef struct {
	char* ipJob;
	char* puertoJob;
	char* ipNodo;
	char* puertoNodo;
	char* archivoTrabajo;
	int bloqueNodo;
	int resultado;

} replanificacionMap;

typedef struct {
	int combiner;
	t_list* archivosTrabajo;
} caracteristicasJob;

typedef struct {
	int nombreJob;
	char* ipJob;
	char* puertoJob;
	char* resultadoOperacion;
	char* ipNodo;
	char* puertoNodo;
} trabajoJobTerminado;

typedef struct {
	int resultadoGuardado;
	char* ipJob;
	char* puertoJob;
} resultado_fileSystem;

typedef struct {
	char* ipNodo;
	char* puertoNodo;
} reducesParcialesParaJob;

typedef struct {
	char* ipNodo;
	char* puertoNodo;

}falloNodo;

#endif /* ESTRUCTURASMARTA_H_ */
