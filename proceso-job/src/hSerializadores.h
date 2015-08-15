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

#include"estructurasJob.h"

#ifndef HSERIALIZADORES_H_
#define HSERIALIZADORES_H_

typedef struct _Paquete {
	uint32_t longitud_bloque;
	void *bloque;

} paquete;

typedef struct _Bloque {
	uint32_t longitud_datos;
	void *datos;
} bloque;

paquete *pack(bloque *block);

char* deserializarMapJob(bloque* block, t_list* instruccionesMap);

bloque *serializarBloqueNodo(char* bloqueNodo, int nroBloque);

bloque* serializarPedirTrabajoDeArchivoAMarta(char** direccionesArchivo,
		int combiner);

bloque* serializarMapNodo(script* datos_script, int nroBloque, int nombreJob);

int deserializarResultadoMapNodo(bloque* block);

char* deserializarMapJobReplan(bloque* block, instruccionMap* instruccion);

bloque* serializarResultadoMapMarta(instruccionMap* instruccion_Map,
		int resultado, char* archivoTrabajo);

char* deserializarReduceJobSinCombiner(bloque* block,
		instruccionReduce* instrucciones_Reduce);

char* deserializarReduceParcialJobConCombiner(bloque* block,
		instruccionReduce* instrucciones_Reduce);

bloque* serializarReduceSinCombinerNodo(script* datos_script,
		instruccionReduce* instruccion_reduce, int nombreJob);

bloque* serializarReduceParcialConCombinerNodo(script* datos_script,
		instruccionReduce* instruccion_reduce, int nombreJob);

bloque* serializarReduceFinalConCombinerNodo(script* datos_script,
		instruccionReduce* instruccion_reduce, int nombreJob);

int deserializarResultadoReduceParcialConCombiner(bloque* block);

bloque* serializarResultadoReduceSinCombinerMarta(int resultado);

bloque* serializarResultadoReduceExitoso(int nombreJob,
		char *resultado_operacion, char* ipNodo, char* puertoNodo);

bloque* serializarResultadoReduceSinCombinerFallo(char* ipNodoPrincipal,
		char* puertoNodoPrincipal);

bloque* serializarResultadoReduceParcial(int resultado,
		instruccionReduce* instruccion_reduce);

falloNodo* deserializarFalloOperacion(bloque* block);

bloque* serializarResultadoReduceConCombinerFallo(char* ipNodoPrincipal,
		char* puertoNodoPrincipal);

char* deserializarReduceFinalJobConCombiner(bloque* block,
		instruccionReduce* instrucciones_Reduce);

char* deserializarNoExisteArchivoPedido(bloque* block);

#endif /* HSERIALIZADORES_H_ */
