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

#include"EstructurasMARTA.h"

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

archivo* deserializarBloquesParaMarta(bloque *block);

caracteristicasJob* deserializarPedirTrabajoDeArchivoAMarta(bloque *block);

void deserializarActualizacionNodosDisponibles(bloque* block, t_list* listaNodosEnElSitema, fileSystem* file_system);

bloque* serializarMapJob(t_list* instruccionesMap, char* archivoTrabajo);

replanificacionMap* deserializarResultadoMapMarta(bloque* block);

bloque* serializarPedidoBloquesDeArchivo(job* datos_job);

bloque* serializarMapJobReplan(instruccionMapReduce* nuevaInstruccionMap, char* archivoTrabajo);

bloque* serializarReduceJobSinCombiner(char* ipNodoPrincipal, char* puertoNodoPrincipal, t_list* instrucciones, char* archivoTrabajo);

bloque* serializarReduceParcialJobConCombiner(char* ipNodo, char* puertoNodo, int* bloques, int tamanioBloque, char* archivoTrabajo);

bloque* serializarFileSystemNoDisponible();

bloque* serializarFileSystemNoConectado();

void deserializarArchivoPedidoNoExiste(bloque* block, t_list* jobs);

bloque* serializarNoExisteArchivoPedido(char* archivo_buscado);

t_list* deserializarResultadoReduceParcial(bloque* block);

bloque* serializarFalloOperacion(char* ipNodo, char* puertoNodo);

bloque* serializarFalloOperacionJob();

bloque* serializarOperacionJobExitosa();

bloque* serializarGuardarResultadoFileSystem(trabajoJobTerminado* trabajo_terminado);

resultado_fileSystem* deserializarGuardadoResultadoFileSystem(bloque* block);

bloque* serializarReduceFinalJobConCombiner(char* ipNodoPrincipal, char* puertoNodoPrincipal, t_list* instrucciones, char* archivoTrabajo);

bloque* serializarNoSePuedeReplanificarMap();

falloNodo* deserializarResultadoReducerFallo(bloque* block);

#endif /* HSERIALIZADORES_H_ */
