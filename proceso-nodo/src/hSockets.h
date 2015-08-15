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

#include "hSerializadores.h"

#ifndef HSOCKETS_H_
#define HSOCKETS_H_

#define BACKLOG 200			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo

void deciHola();
void escucharFileSystem(int fdDestino);

void* conexionFileSystem(void** args);

void crearConexionConFileSystem(char* ip_destino, char* puerto_destino, void* nombre_Nodo, void* capacidad_Nodo);

int crearConexion (char* ip_destino, char* puerto_destino);

int sendAll(int fd, paquete *package);

int enviarDatos(int fd_destino, bloque *block);

void escucharConexiones(char* ip_destino, char* puerto_destino,	char* puerto_local, int nombreNodo, int capacidadNodo);

void* trabajoNodo(void* new_fd);

void trabajarMap(int fd_job, bloque *block);

void trabajarReduceSinCombiner(int fd_job, bloque *block);

void trabajarReduceParcialConCombiner(int fd_job, bloque *block);

void trabajarReduceFinalConCombiner(int fd_job, bloque *block);

void realizarPedidoMap(int fd_Nodo, bloque* block);

void realizarPedidoReduceParciales(int fd_Nodo, bloque* block);

int receive_and_unpack(bloque *block, int clientSocket);

void limpiarTrabajoReduce(trabajoReduce* recibido);

void realizarSolicitudBloqueArchivo(int fd_filesystem, bloque* block);

void realizarPedidoOperacionJobFinalizada(int fd_filesystem, bloque* block);

#endif /* HSOCKETS_H_ */
