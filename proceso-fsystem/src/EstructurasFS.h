/*
 * EstructurasFS.h
 *
 *  Created on: 3/6/2015
 *      Author: utnso
 */

#include <stdio.h>
#include <commons/collections/list.h>
#include <pthread.h>



#ifndef ESTRUCTURASFS_H_
#define ESTRUCTURASFS_H_

typedef struct {
	char* ip_Marta;
	char* puerto_ActualizadorLista;
	char* puerto_Principal;
} conexionMarta;

typedef struct {
	char* puertoListen;
	int nodos_minimo;
	char* ipMarta;
	char* puertoMPrincipal;
	char* puertoMAct;
} lectura;

typedef struct {
	int index;
	int padre;
	char* nombre;
} directorio; //Estructura utilizada para la lista de directorios

typedef struct {
	char* ip;
	char* puerto;
	char* puerto_escucha;
	int nroBloque;
	int nombreNodo;

} infoCopia; //Estructura utilizada para documentar las copias de cada bloque, dentro de la estructura datoBloque

typedef struct {
	int nrobloque;
	t_list* copias;
	int numeroDeCopias;
} datoBloque; //Estructura utilizada para la lista enlazada de ubicacion de copias del archivo en los nodos dentro de la estructura archivo

typedef struct {
	char* nombre;
	int tamanioEnBytes;
	int directorio;
	int estado;
	t_list* listaBloques;
} archivo; //Estructura utilizada para la lista enlazada con informacion de los archivos del FS

typedef struct {
	int nombreNodo;
	char* ip;
	char* puerto;
	char* puerto_escucha;
	int *bloquesOcupados;
	int capacidadNodo;
	int disponible;
} datosNodo; //Estructura utilizada para lista enlazada de nodos disponible que tiene tanto FileSystem como MARTA

typedef struct {
	int nombreNodo;
	int capacidadNodo;
	char* puerto_escucha;
} caracteristicasNodo; //Estructura utilizada cuadno se conexta un nuevo nodo

typedef struct{
	int tamanio_bloque;
	char *bloque;
}bloqueMapeado;

typedef struct{
	char* ipJob;
	char* puertoJob;
	char* archivoBuscado;
}pedidoArchivo;

typedef struct {
	int nombreJob;
	char* ipJob;
	char* puertoJob;
	char* resultadoOperacion;
	char* ipNodo;
	char* puertoNodo;
}trabajoJobTerminado;

#endif /* ESTRUCTURASFS_H_ */
