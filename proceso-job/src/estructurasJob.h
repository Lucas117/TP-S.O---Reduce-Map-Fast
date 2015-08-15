/*
 * estructurasJob.h
 *
 *  Created on: 1/7/2015
 *      Author: utnso
 */

#ifndef ESTRUCTURASJOB_H_
#define ESTRUCTURASJOB_H_

#include <stdio.h>
#include <commons/collections/list.h>

typedef struct{

	char* ipMarta;
	char* puertoMarta;
	char* mapper;
	char* reducer;
	int combiner;
	char* puertoSalida;
	char** archivos;
	char* resultado;
	int nombreJob;
} lectura;


typedef struct {
	char* ip_Marta;
	char* puerto_Principal;

} conexionMarta;

typedef struct {
	char* ipNodo;
	char* puertoNodo;
	int bloqueNodo;

} instruccionMap;

typedef struct {
	char* ipNodo;
	char* puertoNodo;
	int bloqueNodo;

} datosInstruccionReduce;

typedef struct {
	char* ipNodoPrincipal;
	char* puertoNodoPrincipal;
	t_list* instruccionesReduce;

} instruccionReduce;

typedef struct {
	char* instruccionMap;
	char* insruccionReduce;
	int tamanioScriptReduce;
	int tamanioScriptMap;
}script;

typedef struct {
	char* ipNodo;
	char* puertoNodo;

}falloNodo;

typedef struct {
	char* ipNodo;
	char* puertoNodo;
} reducesParcialesParaJob;


#endif /* ESTRUCTURASJOB_H_ */
