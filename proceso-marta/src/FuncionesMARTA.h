/*
 * FuncionesMARTA.h
 *
 *  Created on: 7/7/2015
 *      Author: utnso
 */

#ifndef FUNCIONESMARTA_H_
#define FUNCIONESMARTA_H_
#include <stdio.h>
#include <stdlib.h>
#include <commons/collections/list.h>
#include <commons/config.h>
#include<commons/string.h>
#include "hSockets.h"
#include <commons/log.h>
#include <commons/txt.h>

#define PATH "archivoMARTA.conf"
#define PATH_LOG "LogMARTA.log"

void crearMappersParaJob(job* job, t_list* nodosDisponibles,
		t_list* nodosConCarga, int fdJob, pthread_mutex_t* mutexSistema,
		pthread_mutex_t* mutexListasNodos);
void crearReducersParaJobsSinCombiner(t_list* jobs, t_list * nodosConCarga,
		int fdJob, pthread_mutex_t* mutexSistema,
		pthread_mutex_t* mutexListasNodos);
void crearReducersParaJobConCombiner(t_list* jobs, t_list* nodosCarga,
		t_list* mapersAReducir, int fdJob, pthread_mutex_t* mutexSistema,
		pthread_mutex_t* mutexListasNodos);
void terminaronTodasLasInstruccionesConCombiner(t_list* listaResultado,
		t_list* jobs, t_list * nodosConCarga, t_list* listaNodosEnElSistema,
		int fdJob, pthread_mutex_t* mutexSistema,
		pthread_mutex_t* mutexListasNodos);
void replanificarMapEnJob(replanificacionMap* replan, t_list* jobs,
		t_list * nodosDisponibles, t_list* nodosConCarga, int fdJob,
		pthread_mutex_t* mutexSistema, pthread_mutex_t* mutexListasNodos);

t_list* obtenerReducesPurificadosParaJob(t_list* reducesParcialesChotos);

void abortarJobReduceParcial(int fdJob, char* ipNodo, char* puertoNodo);

void borrarJobsDeMARTA(char* ipJob, char* puertoJob, t_list* jobs, t_list* cargaNodos);

lectura* leerConfiguracion();

void liberarLectura(lectura* leido);

#endif /* FUNCIONESMARTA_H_ */
