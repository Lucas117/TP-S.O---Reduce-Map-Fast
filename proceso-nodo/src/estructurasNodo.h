/*
 * estructurasNodo.h

 *
 *  Created on: 4/6/2015
 *      Author: utnso
 */

#ifndef ESTRUCTURASNODO_H_
#define ESTRUCTURASNODO_H_

#include <stdio.h>
#include <commons/collections/list.h>
#include <pthread.h>

typedef struct{
	char** trabajo_bloques;
	int recibido;
	int nombre_job;
	int ya_mostre_que_estoy;
	int script_map_bajado;
	int script_reduce_bajado;
}trabajoPorJob;

/* Estructura que es devuelta una vez que se lee el archivo de configuración*/
typedef struct{
	char* puertoFs;
	char* ipFS;
	char* nombreBin;
	char* dirTemp;
	char* puertoEscucha;
	int nombreNodo;
	long tamanioNodo;

} lectura;

typedef struct{

	int tamanio_bloque;
	char *bloque;

}bloque_mapeado;

typedef struct {
	int nombreJob;
	int nroBloque;
	char* instruccionMap;
	int tamanioScriptMap;
}trabajoMap;


typedef struct {
	char* ipNodo;
	char* puertoNodo;
	int nroBloque;
}datoBloqueArchivo;

typedef struct {
	int nombreJob;
	char* ipLocal;
	char* puertoLocal;
	t_list* datosBloqueArchivo;
	char* instruccionReduce;
	int tamanioScriptReduce;
}trabajoReduce;

typedef struct {
	int nombreJob;
	int nroBloque;
}pedidoMap;

typedef struct{
	char *ruta_temp;
	char *ruta_recibido;
}rutas_temporales;

#endif /* ESTRUCTURASNODO_H_ */
