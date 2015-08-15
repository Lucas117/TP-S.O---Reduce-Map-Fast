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

#include "hSerializadores.h"

#ifndef HSOCKETS_H_
#define HSOCKETS_H_

#define BACKLOG 200			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo

void* hiloParaEscuchar(void** args);

void establecerConexionConMarta(conexionMarta* conexion_Marta,
		char* puerto_escucha);

int crearConexion(char* ip_destino, char* puerto_destino, char* puerto_escucha);

int sendAll(int fd, paquete *package);

int obtenerDescriptorDeFichero(char* ip_destino, char* puerto_destino);

int enviarDatos(char* ip_destino, char* puerto_destino, bloque *block);

void escucharConexiones(char* puerto_escucha, conexionMarta* conexion_Marta,
		t_list* listaNodosEnElSitema, t_list* listaNodosEnEspera,
		t_list* archivos, t_list* directorios, void* nodos_Minimo,
		pthread_mutex_t* mutexListasNodos,
		pthread_mutex_t* mutexArchivosYDirectorios, pthread_mutex_t* mutexConexionMaRTA);

int receive_and_unpack(bloque *block, int clientSocket);

void eliminarSocketNodo(datosNodo* nodo);

void conexionNodo(caracteristicasNodo* nodoNuevo, int fdNodo,
		t_list* listaNodosEnElSitema, t_list* listaNodosEnEspera,
		conexionMarta* conexion_Marta, int nodosMinimo);

void desconexionNodo(int fdNodo, t_list* listaNodosEnElSitema,
		t_list* listaNodosEnEspera, conexionMarta* conexion_Marta,
		int nodosMinimo);

void actualizarMarta(t_list* listaNodosEnElSitema,
		conexionMarta* conexion_Marta, int nodosMinimo);

int enviarDatosParaObtenerBloque(int fd_destino, bloque *block);

void buscarListaDeBloquesDeArchivoYEnviar(pedidoArchivo* archivo_buscado,
		t_list* archivos, t_list* directorios, t_list* nodos, char* ipMarta,
		char* puertoMarta);

t_list* listaDeBloquesDelArchivo(char* archivo_buscado, t_list* archivos,
		t_list* directorios, t_list* nodos);

int indiceDelDirectorio(char* ruta, t_list* directorios);

int archivoEstaDisponible(archivo* arch, t_list* nodosEnSistema);

char* bajarResultadoAlSistema(bloqueMapeado* bloque_mapeado,char* dondeLoGuardoEnMDFS);

void guardarResultadoOperacionJobFileSystem(
		trabajoJobTerminado* trabajo_terminado, t_list* archivos,
		t_list* directorios, t_list* nodosSistema, pthread_mutex_t* mutex,
		int nodosMinimos,char* puerto_escucha,conexionMarta* marta);

#endif /* HSOCKETS_H_ */
