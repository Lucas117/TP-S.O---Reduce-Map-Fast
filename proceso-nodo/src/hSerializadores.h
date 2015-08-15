/*
 * hSerializadores.h
 *
 *  Created on: 7/6/2015
 *      Author: utnso
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <stdint.h>

#include"funcionesNodo.h"

#ifndef HSERIALIZADORES_H_
#define HSERIALIZADORES_H_

typedef struct _Paquete{
	uint32_t longitud_bloque;
	void *bloque;

} paquete;

typedef struct _Bloque{
	uint32_t longitud_datos;
	void *datos;
} bloque;

paquete *pack(bloque *block);

bloque* serializarNombreYCapacidadNodo(int nombreNodo, int capacidadNodo,char* puerto_local);

void deserializarBloqueNodo (bloque *block);

bloque* serializarResultadoMap(int resultado);

void deserializarNombreNodo(bloque* block);

void deserializarMapNodo(bloque* block, trabajoMap* trabajo);

bloque_mapeado* deserializarSolicitarBloqueArchivo(bloque* block);

bloque* serializarBloqueArchivoSolicitado(bloque_mapeado* bloqueMapeado);

bloque* serializarResultadoReduceFinal(int resultado);

bloque* serializarPedidoMapRealizado(int nombreJob, int nroBloque);

void deserializarPedidoMapRealizado(bloque* block, pedidoMap* pedido_map);

bloque_mapeado* deserializarBloqueArchivoSolicitado(bloque* block);

bloque* serializarPedidoReduceRealizado(int nombreJob);

int deserializarPedidoReduceRealizado(bloque* block);

bloque* serializarResultadoReduceParcialConCombiner(int resultado);

int deserializarPedidoOperacionJobFinalizada(bloque* block);

void deserializarReduceParcialConCombinerNodo(bloque* block, trabajoReduce* trabajo);

void deserializarReduceFinal(bloque* block, trabajoReduce* trabajo);

void deserializarReduceSinCombinerNodo(bloque* block, trabajoReduce* trabajo);

#endif /* HSERIALIZADORES_H_ */
