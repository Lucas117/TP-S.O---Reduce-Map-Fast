/*
 * hSockets.h
 *
 *  Created on: 1/6/2015
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
#include <commons/string.h>
#include <pthread.h>

#include "hSerializadores.h"
#include "funcionesJob.h"

#ifndef HSOCKETS_H_
#define HSOCKETS_H_

#define BACKLOG 200			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo

void* hiloParaReduceSinCombiner(void* foo);

void* hiloParaReduceParcialConCombiner(void* foo);

void* hiloParaReduceFinalConCombiner(void* foo);

void* hiloParaMapear(void* foo);

void crearHilosReduce(bloque* block);

void crearHilosMaper(bloque* block);

void rutinaReduceSinCombiner();

void rutinaReduceParcialConCombiner();

void rutinaReduceFinalConCombiner();

void rutinaMapper();

void establecerConexionConMarta(conexionMarta* conexion_Marta);

int crearConexion (char* ip_destino, char* puerto_destino);

int sendAll(int fd, paquete *package);

int obtenerDescriptorDeFichero(char* ip_destino, char* puerto_destino);

int enviarDatos(char* ip_destino, char* puerto_destino, bloque *block);

void establecerConexiones(char* puerto_escucha, conexionMarta* conexion_Marta, char** direccionesArchivo, int combiner, script *mapper_reducer, int nombre_Job, char* resultadoOperacion);

int receive_and_unpack(bloque *block, int clientSocket);

void crearHiloMaperReplan(bloque* block);

void crearHiloReducerSinCombiner(bloque* block) ;

void crearHiloReducerParcialConCombiner(bloque* block);

void crearHiloReducerFinalConCombiner(bloque* block);

int enviarDatosNodo(int fd_destino, bloque *block);

void* accionesJob(void* fd_Marta);


#endif /* HSOCKETS_H_ */
