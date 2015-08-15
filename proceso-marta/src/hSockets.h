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
#include <pthread.h>

#include "hSerializadores.h"

#ifndef HSOCKETS_H_
#define HSOCKETS_H_

#define BACKLOG 200			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo


void* hiloParaActualizaListaNodos(void** args);

void agregarConexionFileSystemAlSistema(fileSystem* file_system);

void escucharConexiones(char* puerto_escucha, char* puerto_escucha_ActualizadorLista, t_list* listaNodosEnElSitema,	t_list* cargaNodos, t_list* jobs, t_list* jobsEnEsperaResultadoGuardado);

void actualizardorListaNodos(char* puerto_actualizador, fileSystem* file_system, t_list* listaNodosEnElSitema, t_list* cargaNodos);

int sendAll(int fd, paquete *package);

int obtenerDescriptorDeFichero(char* ip_destino, char* puerto_destino);

int enviarDatos(int fd_destino, bloque *block);

void esperarPorActualizaciones(int fileSystem_fd, fileSystem* file_system, t_list* listaNodosEnElSitema, t_list* cargaNodos);

int receive_and_unpack(bloque *block, int clientSocket);

void agregarAddressJob(job* datos_job, int fdJob);

void agregarJobAlSistema(job* datos_job, int fd_job, t_list* jobs);

void determinarResultadoOperacionMap(replanificacionMap* resultadoMap, t_list* jobs, t_list * nodosConCarga, t_list* listaNodosEnElSistema, int fdJob);

void agregarAddressResultadoJob(replanificacionMap* resultadoMap, int fdJob);

void pedirBloquesArchivoFileSystem(caracteristicasJob* caracteristicas_job,	int fd_Job, fileSystem* file_system, t_list* jobs);

void resultadoOperacionMapSinCombiner(replanificacionMap* resultadoMap, job* jobBuscado, t_list* jobs, t_list * nodosConCarga, t_list* listaNodosEnElSistema, int fdJob);

void resultadoOperacionMapCombiner(replanificacionMap* resultadoMap, job* jobBuscado, t_list* jobs, t_list * nodosConCarga, t_list* listaNodosEnElSistema, int fdJob);

void avisarNoExisteArchivoPedidoJob(int fdJob, t_list* jobs, char* archivo_buscado);

void* hiloPlanificacionMarta(void** args);

void planificacionMarta(void* new_fd, fileSystem* file_system, t_list* listaNodosEnElSistema, t_list* cargaNodos, t_list* jobs, t_list* jobsEnEsperaResultadoGuardado);

void agregarAddresResultadoReduce(t_list* listaResultadoReduceParcial, int fdJob);

void destruirJob(job* job);

void destruirMapReduce(instruccionMapReduce* mapreduce);

void destruirInstruccion(instrucciones* instr);

void falloOperacion(int fdJob, falloNodo* fallo_nodo);

void terminarTrabajoJob(int fdJob, trabajoJobTerminado* trabajo_terminado,	fileSystem* file_system, t_list* jobsEnEsperaResultadoGuardado);

void fileSystemNoDisponible(int fdJob);

void asignarBloquesAJobYPlanificar(archivo* archivoTrabajoJob, t_list* jobs, t_list* nodosEnSistema, t_list* nodosCarga);

void informarJobResultadoGuardado(resultado_fileSystem* resultadoFS, t_list* jobsEnEsperaResultadoGuardado);

void avisarJobsEnEspera(t_list* jobsEnEsperaResultadoGuardado);

void destruirJobEnEspera(trabajoJobTerminado* jobEnEspera);

void controlarListaNodosCarga(t_list* nodosSistema, t_list* nodosConCarga);

void fileSystemNoConectadaDisponible(int fdJob);

#endif /* HSOCKETS_H_ */
