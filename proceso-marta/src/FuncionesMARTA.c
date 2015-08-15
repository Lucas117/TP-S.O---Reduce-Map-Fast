/*
 * FuncionesMARTA.c
 *
 *  Created on: 7/7/2015
 *      Author: utnso
 */

#include <stdio.h>
#include <stdlib.h>
#include <commons/collections/list.h>
#include "hSockets.h"
#include "hSerializadores.h"
#include "FuncionesMARTA.h"
#include "EstructurasMARTA.h"

pthread_mutex_t mutexLogMARTA = PTHREAD_MUTEX_INITIALIZER;

short copiaDisponible(infoCopia* copia, t_list* nodosDisponibles) {

	bool nodoBuscado(cargaNodo* nodo) {
		return nodo->nombreNodo == copia->nombreNodo;
	}

	datosNodo* nodo = list_find(nodosDisponibles, (void*) nodoBuscado);

	return nodo->disponible;
}

int obtenerCargaNodo(infoCopia* copia, t_list* nodosConCarga) {

	bool nodoBuscado(cargaNodo* nodo) {
		return nodo->nombreNodo == copia->nombreNodo;
	}

	cargaNodo* nodo = list_find(nodosConCarga, (void*) nodoBuscado);

	return nodo->carga;

}

void sumarCargaANodo(infoCopia* copiaPosta, t_list* nodosConCarga) {

	bool nodoBuscado(cargaNodo* nodo) {
		return nodo->nombreNodo == copiaPosta->nombreNodo;
	}

	cargaNodo* nodo = list_find(nodosConCarga, (void*) nodoBuscado);

	nodo->carga = nodo->carga + 1;

	printf("Nodo %d -> Carga %d\n",nodo->nombreNodo, nodo->carga);

}

void crearMappersParaJob(job* job, t_list* nodosDisponibles,
		t_list* nodosConCarga, int fdJob, pthread_mutex_t* mutexSistema,
		pthread_mutex_t* mutexListasNodos) {

	char* msg_log;

	pthread_mutex_lock(mutexSistema);
	msg_log = malloc(
			strlen("Planificando Maps -> Job - IP: ") + 1 + 16
					+ strlen(" PUERTO: ") + 1 + 6);

	strcpy(msg_log, "Planificando Maps -> Job - IP: ");
	strcat(msg_log, job->ip);
	strcat(msg_log, " PUERTO: ");
	strcat(msg_log, job->puerto);

	loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_TRACE, msg_log);

	free(msg_log);

	bloque *bloqueAEnviar;
	instrucciones* instruccionesJob = malloc(sizeof(instrucciones));
	instruccionesJob->ipNodoPrincipal = malloc(16);
	instruccionesJob->puertoNodoPrincipal = malloc(6);
	instruccionesJob->instruccionesMap = list_create();
	instruccionesJob->instruccionesReduce = list_create();

	void seleccionarCopia(datoBloque* bloque) {
		pthread_mutex_lock(mutexListasNodos);

		int i;
		int carga[bloque->numeroDeCopias];
		int indexMenorCarga = -1;
		int nodoSeleccionado = -1;

		for (i = 0; i < bloque->numeroDeCopias; i++) {
			infoCopia* copia2 = list_get(bloque->copias, i);
			carga[i] = obtenerCargaNodo(copia2, nodosConCarga);

			if (nodoSeleccionado != -1) {
				if (carga[i] < carga[nodoSeleccionado]) {
					if (copiaDisponible(copia2, nodosDisponibles)) {
						indexMenorCarga = i;
						nodoSeleccionado = i;
					}
				}
			} else {
				if (copiaDisponible(copia2, nodosDisponibles)) {
					indexMenorCarga = i;
					nodoSeleccionado = i;
				}

			}
		}

		if (indexMenorCarga == -1) {
			//TODO ERROR BLOQUE NO DISPONIBLE
			return;
		}

		infoCopia* copiaPosta = list_get(bloque->copias, indexMenorCarga);

		bool nodoBusquete(datosNodo* nodito) {
			return nodito->nombreNodo == copiaPosta->nombreNodo;
		}

		datosNodo* nodoAuxx = list_find(nodosDisponibles, (void*) nodoBusquete);

		instruccionMapReduce* instruccionMap = malloc(
				sizeof(instruccionMapReduce));
		instruccionMap->ipNodo = malloc(16);
		strcpy(instruccionMap->ipNodo, nodoAuxx->ip);
		instruccionMap->puertoNodo = malloc(6);
		strcpy(instruccionMap->puertoNodo, nodoAuxx->puerto);
		instruccionMap->bloqueNodo = copiaPosta->nroBloque;
		instruccionMap->terminado = 0;

		sumarCargaANodo(copiaPosta, nodosConCarga);

		list_add(instruccionesJob->instruccionesMap, instruccionMap);

		bool obtenerNodoConCarga(cargaNodo* carga_nod) {
			return !strcmp(carga_nod->ip, instruccionMap->ipNodo)
					&& !strcmp(carga_nod->puerto, instruccionMap->puertoNodo);
		}

		cargaNodo* carga_nodo_msg = list_find(nodosConCarga,
				(void*) obtenerNodoConCarga);

		char* carga_nodo_msg_log = string_itoa(carga_nodo_msg->carga);
		char* bloque_nodo_log = string_itoa(instruccionMap->bloqueNodo);

		msg_log = malloc(
				strlen("Map -> Nodo - IP: ") + 1 + 16 + strlen(" PUERTO: ") + 1
						+ 6 + strlen(" BLOQUE: ") + 1 + 4 + strlen(" CARGA: ")
						+ 1 + 4);

		strcpy(msg_log, "Map -> Nodo - IP: ");
		strcat(msg_log, instruccionMap->ipNodo);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, instruccionMap->puertoNodo);
		strcat(msg_log, " BLOQUE: ");
		strcat(msg_log, bloque_nodo_log);
		strcat(msg_log, " CARGA: ");
		strcat(msg_log, carga_nodo_msg_log);

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO, msg_log);

		free(msg_log);
		free(carga_nodo_msg_log);
		free(bloque_nodo_log);

		pthread_mutex_unlock(mutexListasNodos);

	}

	list_iterate(job->datosBloque, (void*) seleccionarCopia);

	job->instrucciones = instruccionesJob;

	bloqueAEnviar = serializarMapJob(instruccionesJob->instruccionesMap,
			job->direccionArchivo);
	enviarDatos(fdJob, bloqueAEnviar);

	pthread_mutex_unlock(mutexSistema);

}

void crearReducersParaJobsSinCombiner(t_list* jobs, t_list * nodosConCarga,
		int fdJob, pthread_mutex_t* mutexSistema,
		pthread_mutex_t* mutexListasNodos) {

	pthread_mutex_lock(mutexSistema);
	pthread_mutex_lock(mutexListasNodos);

	t_list* nodosReducidos = list_create();

	t_list* contadorNodos = list_create();
	int contadorCarga = 0;
	cargaNodo* nodoPrincipal = malloc(sizeof(cargaNodo));
	nodoPrincipal->ip = malloc(16);
	nodoPrincipal->puerto = malloc(6);

	void vecesQueSeRepiteNodo(instruccionMapReduce* instruccion) {

		bool esElMismoNodo(datosNodo* nodoaux) {
			return !strcmp(instruccion->ipNodo, nodoaux->ip)
					&& !strcmp(instruccion->puertoNodo, nodoaux->puerto);
		}

		if (list_is_empty(contadorNodos)) {
			datosNodo* nodo = malloc(sizeof(datosNodo));
			nodo->ip = malloc(16);
			nodo->puerto = malloc(6);
			strcpy(nodo->ip, instruccion->ipNodo);
			strcpy(nodo->puerto, instruccion->puertoNodo);
			nodo->nombreNodo = 0;
			list_add(contadorNodos, nodo);
		} else {
			if (list_find(contadorNodos, (void*) esElMismoNodo)) {

			} else {
				datosNodo* nodo = malloc(sizeof(datosNodo));
				nodo->ip = malloc(16);
				nodo->puerto = malloc(6);
				strcpy(nodo->ip, instruccion->ipNodo);
				strcpy(nodo->puerto, instruccion->puertoNodo);
				nodo->nombreNodo = 0;
				list_add(contadorNodos, nodo);
			}
		}

	}

	void buscarNodoPrincipal(cargaNodo* nodoIterado) {
		if (nodoIterado->nombreNodo > contadorCarga) {
			nodoPrincipal = nodoIterado;
			contadorCarga = nodoIterado->nombreNodo;

		}
	}

	void vecesQueSeRepiteInstruccion(instruccionMapReduce* instruccionaux) {
		bool esElMismoNodo(datosNodo* nodoaux) {
			return !strcmp(instruccionaux->ipNodo, nodoaux->ip)
					&& !strcmp(instruccionaux->puertoNodo, nodoaux->puerto);
		}
		cargaNodo* nodoB = list_find(contadorNodos, (void*) esElMismoNodo);

		nodoB->nombreNodo = nodoB->nombreNodo + 1;

	}

	void repeticionInstruccionEnJobs(job* jobss) {
		list_iterate(jobss->instrucciones->instruccionesMap,
				(void*) vecesQueSeRepiteInstruccion);
	}

	void contarRepeticionesVariosJobs(job* job) {
		list_iterate(job->instrucciones->instruccionesMap,
				(void*) vecesQueSeRepiteNodo);
	}

	list_iterate(jobs, (void*) contarRepeticionesVariosJobs);

	list_iterate(jobs, (void*) repeticionInstruccionEnJobs);

	list_iterate(contadorNodos, (void*) buscarNodoPrincipal);

	void asignarNodoPrincipal(job* job) {

		strcpy(job->instrucciones->ipNodoPrincipal, nodoPrincipal->ip);
		strcpy(job->instrucciones->puertoNodoPrincipal, nodoPrincipal->puerto);
	}
	list_iterate(jobs, (void*) asignarNodoPrincipal);

	t_list* mapersReunidos = list_create();

	void reunirMapers(job* jobx) {
		list_add_all(mapersReunidos, jobx->instrucciones->instruccionesMap);
	}

	list_iterate(jobs, (void*) reunirMapers);

	job* jobGato = list_get(jobs, 0);

	char* msg_log = malloc(
			strlen("Planificando Reduce Final Sin Combiner -> Job - IP: ") + 1
					+ 16 + strlen(" PUERTO: ") + 1 + 6);

	strcpy(msg_log, "Planificando Reduce Final Sin Combiner -> Job - IP: ");
	strcat(msg_log, jobGato->ip);
	strcat(msg_log, " PUERTO: ");
	strcat(msg_log, jobGato->puerto);

	loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_TRACE, msg_log);

	free(msg_log);

	bool obtenerNodoConCarga(cargaNodo* carga_nod) {
		return !strcmp(carga_nod->ip, nodoPrincipal->ip)
				&& !strcmp(carga_nod->puerto, nodoPrincipal->puerto);
	}

	cargaNodo* carga_nodo_msg = list_find(nodosConCarga,
			(void*) obtenerNodoConCarga);

	char* carga_nodo_msg_log = string_itoa(carga_nodo_msg->carga);

	msg_log = malloc(
			strlen("Reduce Final Sin Combiner -> Nodo Principal - IP: ") + 1
					+ 16 + strlen(" PUERTO: ") + 1 + 6 + strlen(" BLOQUE: ") + 1
					+ 4 + strlen(" CARGA: ") + 1 + 4);

	strcpy(msg_log, "Reduce Final Sin Combiner -> Nodo Principal - IP: ");
	strcat(msg_log, nodoPrincipal->ip);
	strcat(msg_log, " PUERTO: ");
	strcat(msg_log, nodoPrincipal->puerto);
	strcat(msg_log, " CARGA: ");
	strcat(msg_log, carga_nodo_msg_log);

	loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO, msg_log);

	free(msg_log);
	free(carga_nodo_msg_log);

	//Aca se envia al nodo la ip y puerto del nodoPrincipal (job->instrucciones->ipNodoPrincipal y job->instrucciones->puertoNodoPrincipal)
	bloque* bloqueAEnviar;
	bloqueAEnviar = serializarReduceJobSinCombiner(nodoPrincipal->ip,
			nodoPrincipal->puerto, mapersReunidos, jobGato->direccionArchivo);
	enviarDatos(fdJob, bloqueAEnviar);

	pthread_mutex_unlock(mutexListasNodos);
	pthread_mutex_unlock(mutexSistema);

}

void crearReducersParaJobConCombiner(t_list* jobs, t_list* nodosCarga,
		t_list* mapersAReducir, int fdJob, pthread_mutex_t* mutexSistema,
		pthread_mutex_t* mutexListasNodos) {

	pthread_mutex_lock(mutexSistema);
	pthread_mutex_lock(mutexListasNodos);

	t_list* reducesAEnviar = list_create();
	char* ipNodo = malloc(16);
	char* puertoNodo = malloc(6);
	int tamanioBloque = list_size(mapersAReducir);
	int bloques[list_size(mapersAReducir)];
	int i = 0;
	int bloqueNodo;

	void crearReduce(instruccionMapReduce* instruccion) {
		instruccionMapReduce* reduce = malloc(sizeof(instruccionMapReduce));
		reduce->ipNodo = malloc(16);
		reduce->puertoNodo = malloc(6);
		reduce->bloqueNodo = instruccion->bloqueNodo;
		strcpy(reduce->ipNodo, instruccion->ipNodo);
		strcpy(reduce->puertoNodo, instruccion->puertoNodo);
		ipNodo = instruccion->ipNodo;
		puertoNodo = instruccion->puertoNodo;
		bloqueNodo = instruccion->bloqueNodo;
		reduce->terminado = 0;
		bloques[i] = instruccion->bloqueNodo;
		i++;

		bool nodoBuscado(cargaNodo* nodo) {
			return (!strcmp(nodo->ip, reduce->ipNodo))
					&& (!strcmp(nodo->puerto, reduce->puertoNodo));
		}

		cargaNodo* nodoCarg = list_find(nodosCarga, (void*) nodoBuscado);
		nodoCarg->carga = nodoCarg->carga + 1;

		printf("Nodo %d -> Carga %d\n",nodoCarg->nombreNodo, nodoCarg->carga);

		void agregarReduceAJob(job* job) {
			list_add(job->instrucciones->instruccionesReduce, reduce);
		}
		list_iterate(jobs, (void*) agregarReduceAJob);

	}

	list_iterate(mapersAReducir, (void*) crearReduce);

	job* jobToga = list_get(jobs, 0);

	char* msg_log = malloc(
			strlen("Planificando Reduce Parcial Con Combiner -> Job - IP: ") + 1
					+ 16 + strlen(" PUERTO: ") + 1 + 6);

	strcpy(msg_log, "Planificando Reduce Parcial Con Combiner -> Job - IP: ");
	strcat(msg_log, jobToga->ip);
	strcat(msg_log, " PUERTO: ");
	strcat(msg_log, jobToga->puerto);

	loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_TRACE, msg_log);

	free(msg_log);

	bool obtenerNodoConCarga(cargaNodo* carga_nod) {
		return !strcmp(carga_nod->ip, ipNodo)
				&& !strcmp(carga_nod->puerto, puertoNodo);
	}

	cargaNodo* carga_nodo_msg = list_find(nodosCarga,
			(void*) obtenerNodoConCarga);

	char* carga_nodo_msg_log = string_itoa(carga_nodo_msg->carga);

	msg_log = malloc(
			strlen("Reduce Parcial Con Combiner -> Nodo - IP: ") + 1 + 16
					+ strlen(" PUERTO: ") + 1 + 6 + strlen(" CARGA: ") + 1 + 4);

	strcpy(msg_log, "Reduce Parcial Con Combiner -> Nodo - IP: ");
	strcat(msg_log, ipNodo);
	strcat(msg_log, " PUERTO: ");
	strcat(msg_log, puertoNodo);
	strcat(msg_log, " CARGA: ");
	strcat(msg_log, carga_nodo_msg_log);

	loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO, msg_log);

	free(msg_log);
	free(carga_nodo_msg_log);

	//enviar ipNodo ipPuerto y bloque
	bloque* bloqueAEnviar;
	bloqueAEnviar = serializarReduceParcialJobConCombiner(ipNodo, puertoNodo,
			bloques, tamanioBloque, jobToga->direccionArchivo);
	enviarDatos(fdJob, bloqueAEnviar);

	pthread_mutex_unlock(mutexListasNodos);
	pthread_mutex_unlock(mutexSistema);

}

t_list* obtenerReducesPurificadosParaJob(t_list* reducesParcialesChotos) {

	t_list* reducesListos = list_create();

	void purificarReduce(instruccionMapReduce* reduce) {

		bool existeYaEnLaLista(reducesParcialesParaJob* reducePurificado) {
			return !strcmp(reduce->ipNodo, reducePurificado->ipNodo)
					&& !strcmp(reduce->puertoNodo, reducePurificado->puertoNodo);
		}

		if (list_find(reducesListos, (void*) existeYaEnLaLista)) {
			return;
		} else {
			reducesParcialesParaJob* reducePAux = malloc(
					sizeof(reducesParcialesParaJob));
			reducePAux->ipNodo = malloc(16);
			reducePAux->puertoNodo = malloc(6);
			strcpy(reducePAux->ipNodo, reduce->ipNodo);
			strcpy(reducePAux->puertoNodo, reduce->puertoNodo);

			list_add(reducesListos, reducePAux);
		}

	}

	list_iterate(reducesParcialesChotos, (void*) purificarReduce);

	return reducesListos;
}

void terminaronTodasLasInstruccionesConCombiner(t_list* listaResultado,
		t_list* jobs, t_list * nodosConCarga, t_list* listaNodosEnElSistema,
		int fdJob, pthread_mutex_t* mutexSistema,
		pthread_mutex_t* mutexListasNodos) {

	pthread_mutex_lock(mutexSistema);
	pthread_mutex_lock(mutexListasNodos);

	replanificacionMap* instrucAux = list_get(listaResultado, 0);

	bool buscarJob(job* job) {
		return !strcmp(job->ip, instrucAux->ipJob)
				&& !strcmp(job->puerto, instrucAux->puertoJob);
	}

	job* jobBuscado = list_find(jobs, (void*) buscarJob);

	bool jobIgualAlOtro(job* jobAux) {
		return !strcmp(jobAux->ip, jobBuscado->ip)
				&& !strcmp(jobAux->puerto, jobBuscado->puerto);
	}

	t_list* jobsFiltrados = list_filter(jobs, (void*) jobIgualAlOtro);

	int pararIteracion = 1;

	if (jobBuscado->combiner) {

		bool instruccionTerminada(instruccionMapReduce* instruccion) {
			return instruccion->terminado;
		}

		void terminoReduce(replanificacionMap* reduce) {
			if (reduce->resultado && pararIteracion) {

				bool buscarNodo(instruccionMapReduce* instruccion) {
					return !strcmp(instruccion->ipNodo, reduce->ipNodo)
							&& !strcmp(instruccion->puertoNodo,
									reduce->puertoNodo)
							&& instruccion->bloqueNodo == reduce->bloqueNodo;
				}

				instruccionMapReduce* instruccionBuscada = list_find(
						jobBuscado->instrucciones->instruccionesReduce,
						(void*) buscarNodo);
				instruccionBuscada->terminado = reduce->resultado;

				bool nodoDeInstruccion(cargaNodo* nodo) {
					return !strcmp(nodo->ip, reduce->ipNodo)
							&& !strcmp(nodo->puerto, reduce->puertoNodo);
				}

				cargaNodo* nodoAux = list_find(nodosConCarga,
						nodoDeInstruccion);
				nodoAux->carga = nodoAux->carga - 1;

				printf("Nodo %d -> Carga %d\n",nodoAux->nombreNodo, nodoAux->carga);

			} else {
				//replanificacion??
				pararIteracion = 0;
				abortarJobReduceParcial(fdJob, reduce->ipNodo,
						reduce->puertoNodo);

			}
		}
		list_iterate(listaResultado, (void*) terminoReduce);

		t_list* contadorNodos = list_create();
		int contadorCarga = 0;
		cargaNodo* nodoPrincipal = malloc(sizeof(cargaNodo));
		nodoPrincipal->ip = malloc(16);
		nodoPrincipal->puerto = malloc(6);

		void vecesQueSeRepiteNodo(instruccionMapReduce* instruccion) {

			bool esElMismoNodo(datosNodo* nodoaux) {
				return !strcmp(instruccion->ipNodo, nodoaux->ip)
						&& !strcmp(instruccion->puertoNodo, nodoaux->puerto);
			}

			if (list_is_empty(contadorNodos)) {
				datosNodo* nodo = malloc(sizeof(datosNodo));
				nodo->ip = malloc(16);
				nodo->puerto = malloc(6);
				strcpy(nodo->ip, instruccion->ipNodo);
				strcpy(nodo->puerto, instruccion->puertoNodo);
				nodo->nombreNodo = 0;
				list_add(contadorNodos, nodo);
			} else {
				if (list_find(contadorNodos, (void*) esElMismoNodo)) {

				} else {
					datosNodo* nodo = malloc(sizeof(datosNodo));
					nodo->ip = malloc(16);
					nodo->puerto = malloc(6);
					strcpy(nodo->ip, instruccion->ipNodo);
					strcpy(nodo->puerto, instruccion->puertoNodo);
					nodo->nombreNodo = 0;
					list_add(contadorNodos, nodo);
				}
			}

		}

		void buscarNodoPrincipal(cargaNodo* nodoIterado) {
			if (nodoIterado->nombreNodo > contadorCarga) {
				nodoPrincipal = nodoIterado;
				contadorCarga = nodoIterado->nombreNodo;

			}
		}

		bool terminaronTodo(job* jobF) {
			return list_all_satisfy(
					jobF->instrucciones->instruccionesReduce,
					(void*) instruccionTerminada)
					&& list_all_satisfy(
							jobF->instrucciones->instruccionesMap,
							(void*) instruccionTerminada);
		}

		job * jobAux;

		if (list_all_satisfy(jobsFiltrados, (void*) terminaronTodo)) {

			void contarRepeticionesVariosJobs(job* job) {
				list_iterate(job->instrucciones->instruccionesMap,
						(void*) vecesQueSeRepiteNodo);
			}

			list_iterate(jobsFiltrados, (void*) contarRepeticionesVariosJobs);

			void vecesQueSeRepiteInstruccion(
					instruccionMapReduce* instruccionaux) {
				bool esElMismoNodo(datosNodo* nodoaux) {
					return !strcmp(instruccionaux->ipNodo, nodoaux->ip)
							&& !strcmp(instruccionaux->puertoNodo,
									nodoaux->puerto);
				}
				cargaNodo* nodoB = list_find(contadorNodos,
						(void*) esElMismoNodo);

				nodoB->nombreNodo = nodoB->nombreNodo + 1;

			}

			void repeticionInstruccionEnJobs(job* jobss) {
				list_iterate(jobss->instrucciones->instruccionesMap,
						(void*) vecesQueSeRepiteInstruccion);
			}

			list_iterate(jobsFiltrados, (void*) repeticionInstruccionEnJobs);

			list_iterate(contadorNodos, (void*) buscarNodoPrincipal);

			void asignarNodoPrincipal(job* job) {

				strcpy(job->instrucciones->ipNodoPrincipal, nodoPrincipal->ip);
				strcpy(job->instrucciones->puertoNodoPrincipal,
						nodoPrincipal->puerto);

				jobAux = job;
			}
			list_iterate(jobsFiltrados, (void*) asignarNodoPrincipal);

			char* msg_log =
					malloc(
							strlen(
									"Planificando Reduce Parcial Con Combiner -> Job - IP: ")
									+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log,
					"Planificando Reduce Parcial Con Combiner -> Job - IP: ");
			strcat(msg_log, jobAux->ip);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, jobAux->puerto);

			loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_TRACE, msg_log);

			free(msg_log);

			bool obtenerNodoConCarga(cargaNodo* carga_nod) {
				return !strcmp(carga_nod->ip,
						jobAux->instrucciones->ipNodoPrincipal)
						&& !strcmp(carga_nod->puerto,
								jobAux->instrucciones->puertoNodoPrincipal);
			}

			cargaNodo* carga_nodo_msg = list_find(nodosConCarga,
					(void*) obtenerNodoConCarga);

			char* carga_nodo_msg_log = string_itoa(carga_nodo_msg->carga);

			msg_log = malloc(
					strlen("Reduce Parcial Con Combiner -> Nodo - IP: ") + 1
							+ 16 + strlen(" PUERTO: ") + 1 + 6
							+ strlen(" CARGA: ") + 1 + 4);

			strcpy(msg_log, "Reduce Parcial Con Combiner -> Nodo - IP: ");
			strcat(msg_log, jobAux->instrucciones->ipNodoPrincipal);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, jobAux->instrucciones->puertoNodoPrincipal);
			strcat(msg_log, " CARGA: ");
			strcat(msg_log, carga_nodo_msg_log);

			loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(carga_nodo_msg_log);

			t_list* listaNodosReduce = obtenerReducesPurificadosParaJob(
					jobAux->instrucciones->instruccionesReduce);
			//Aca se envia al nodo la ip y puerto del nodoPrincipal (job->instrucciones->ipNodoPrincipal y job->instrucciones->puertoNodoPrincipal)
			bloque* bloqueAEnviar;
			bloqueAEnviar = serializarReduceFinalJobConCombiner(
					jobAux->instrucciones->ipNodoPrincipal,
					jobAux->instrucciones->puertoNodoPrincipal,
					listaNodosReduce, jobAux->direccionArchivo);



			enviarDatos(fdJob, bloqueAEnviar);
		}

	}

	pthread_mutex_unlock(mutexListasNodos);
	pthread_mutex_unlock(mutexSistema);

}

void replanificarMapEnJob(replanificacionMap* replan, t_list* jobs,
		t_list * nodosDisponibles, t_list* nodosConCarga, int fdJob,
		pthread_mutex_t* mutexSistema, pthread_mutex_t* mutexListasNodos) {

	pthread_mutex_lock(mutexSistema);
	pthread_mutex_lock(mutexListasNodos);

	bool buscarJob(job* job) {
		return !strcmp(job->ip, replan->ipJob)
				&& !strcmp(job->puerto, replan->puertoJob)
				&& !strcmp(job->direccionArchivo, replan->archivoTrabajo);
	}

	bool buscarBloqueNecesitado(datoBloque* bloque) {

		int resultado = 0;
		int i;

		for (i = 0; i < bloque->numeroDeCopias; i++) {
			infoCopia* copia = list_get(bloque->copias, i);

			if (copia->nroBloque == replan->bloqueNodo) {

				bool mismoNodo(datosNodo* nodi) {
					return !strcmp(nodi->ip, replan->ipNodo)
							&& !strcmp(nodi->puerto, replan->puertoNodo);
				}

				datosNodo* nodoAuxxx = list_find(nodosDisponibles,
						(void*) mismoNodo);

				if (copia->nombreNodo == nodoAuxxx->nombreNodo) {

					resultado = 1;

				}
			}

		}
		return resultado;
	}

	infoCopia* buscarCopiaDisponible(datoBloque* bloque) {

		int i;
		int carga[bloque->numeroDeCopias];
		int indexMenorCarga = -1;
		int nodoSeleccionado = -1;

		for (i = 0; i < bloque->numeroDeCopias; i++) {
			infoCopia* copia2 = list_get(bloque->copias, i);
			carga[i] = obtenerCargaNodo(copia2, nodosConCarga);

			if (nodoSeleccionado != -1) {
				if (carga[i] < carga[nodoSeleccionado]) {
					if (copiaDisponible(copia2, nodosDisponibles)) {
						indexMenorCarga = i;
						nodoSeleccionado = i;
					}
				}
			} else {
				if (copiaDisponible(copia2, nodosDisponibles)) {
					indexMenorCarga = i;
					nodoSeleccionado = i;
				}

			}
		}

		if (indexMenorCarga == -1) {
			//TODO ERROR BLOQUE NO DISPONIBLE
			return NULL;
		}

		infoCopia* copiaPosta = list_get(bloque->copias, indexMenorCarga);

		return copiaPosta;

	}

	job* jobBuscado = list_find(jobs, (void*) buscarJob);

	datoBloque* bloqueNecesitado = list_find(jobBuscado->datosBloque,
			(void*) buscarBloqueNecesitado);

	infoCopia* copiaParaHacerMap = buscarCopiaDisponible(bloqueNecesitado);

	if (copiaParaHacerMap != NULL) {

		instruccionMapReduce* nuevaInstruccionMap = malloc(
				sizeof(instruccionMapReduce));
		nuevaInstruccionMap->ipNodo = malloc(16);
		nuevaInstruccionMap->puertoNodo = malloc(6);

		void destruirInstruccion(instruccionMapReduce* instruccion) {
			free(instruccion->ipNodo);
			free(instruccion->puertoNodo);
			free(instruccion);

		}

		bool instruccionFallida(instruccionMapReduce* instruccion) {
			return !strcmp(instruccion->ipNodo, replan->ipNodo)
					&& !strcmp(instruccion->puertoNodo, replan->puertoNodo)
					&& instruccion->bloqueNodo == replan->bloqueNodo;
		}

		instruccionMapReduce* instruccionABorrar = list_remove_by_condition(
				jobBuscado->instrucciones->instruccionesMap,
				(void*) instruccionFallida);

		bool nodoDeInstruccion(cargaNodo* nodo) {
			return !strcmp(nodo->ip, instruccionABorrar->ipNodo)
					&& !strcmp(nodo->puerto, instruccionABorrar->puertoNodo);
		}

		cargaNodo* nodoAux = list_find(nodosConCarga,
				(void*) nodoDeInstruccion);
		nodoAux->carga = nodoAux->carga - 1;

		printf("Nodo %d -> Carga %d\n",nodoAux->nombreNodo, nodoAux->carga);

		destruirInstruccion(instruccionABorrar);

		bool mismoNodo(datosNodo* nodiii) {
			return nodiii->nombreNodo == copiaParaHacerMap->nombreNodo;
		}

		datosNodo* nodoAuxxxxx = list_find(nodosDisponibles, (void*) mismoNodo);

		strcpy(nuevaInstruccionMap->ipNodo, nodoAuxxxxx->ip);
		strcpy(nuevaInstruccionMap->puertoNodo, nodoAuxxxxx->puerto);
		nuevaInstruccionMap->bloqueNodo = copiaParaHacerMap->nroBloque;
		nuevaInstruccionMap->terminado = 0;

		sumarCargaANodo(copiaParaHacerMap, nodosConCarga);

		list_add(jobBuscado->instrucciones->instruccionesMap,
				nuevaInstruccionMap);

		bloque* bloqueAEnviar;
		bloqueAEnviar = serializarMapJobReplan(nuevaInstruccionMap,
				jobBuscado->direccionArchivo);
		enviarDatos(fdJob, bloqueAEnviar);

	} else {
		//no se puede replanificar map, se tiene que caer el job
		bloque* bloqueAEnviar;
		bloqueAEnviar = serializarNoSePuedeReplanificarMap();
		enviarDatos(fdJob, bloqueAEnviar);
	}

	pthread_mutex_unlock(mutexListasNodos);
	pthread_mutex_unlock(mutexSistema);

	void terminadaYMismoNodo(instruccionMapReduce* instruccionBuscada) {
		if (instruccionBuscada->terminado
				&& !strcmp(instruccionBuscada->ipNodo, replan->ipNodo)
				&& !strcmp(instruccionBuscada->puertoNodo,
						replan->puertoNodo)) {

			replanificacionMap* replan2 = malloc(sizeof(replanificacionMap));
			replan2->archivoTrabajo = malloc(
					strlen(replan->archivoTrabajo) + 1);
			strcpy(replan2->archivoTrabajo, replan->archivoTrabajo);
			replan2->bloqueNodo = instruccionBuscada->bloqueNodo;
			replan2->ipJob = malloc(16);
			strcpy(replan2->ipJob, replan->ipJob);
			replan2->ipNodo = malloc(16);
			strcpy(replan2->ipNodo, instruccionBuscada->ipNodo);
			replan2->puertoJob = malloc(6);
			strcpy(replan2->puertoJob, replan->puertoJob);
			replan2->puertoNodo = malloc(6);
			strcpy(replan2->puertoNodo, instruccionBuscada->puertoNodo);
			replan2->resultado = 0;

			replanificarMapEnJob(replan2, jobs, nodosDisponibles, nodosConCarga,
					fdJob, mutexSistema, mutexListasNodos);

		}

	}

	list_iterate(jobBuscado->instrucciones->instruccionesMap,
			(void*) terminadaYMismoNodo);

}

lectura* leerConfiguracion() {

	FILE *fp;

	fp = fopen(PATH, "r");
	lectura* a;
	if (fp) {
		a = malloc(sizeof(lectura));
		unsigned int tam;
		fclose(fp);
		t_config *archivo;
		archivo = config_create(PATH);

		tam = string_length(
				config_get_string_value(archivo, "PUERTO_PRINCIPAL"));
		a->puertoPrincipal = malloc(tam + 1);
		strcpy(a->puertoPrincipal,
				config_get_string_value(archivo, "PUERTO_PRINCIPAL"));

		tam = string_length(
				config_get_string_value(archivo, "PUERTO_ACTUALIZACION"));
		a->puertoActualizacion = malloc(tam + 1);
		strcpy(a->puertoActualizacion,
				config_get_string_value(archivo, "PUERTO_ACTUALIZACION"));

		config_destroy(archivo);
		return a;
	} else {
		printf("El archivo no existe\n");
	}

	return NULL;
}

void liberarLectura(lectura* leido) {
	free(leido->puertoActualizacion);
	free(leido->puertoPrincipal);

}

void abortarJobReduceParcial(int fdJob, char* ipNodo, char* puertoNodo) {
	bloque* bloqueAEnviar;
	bloqueAEnviar = serializarFalloOperacion(ipNodo, puertoNodo);
	enviarDatos(fdJob, bloqueAEnviar);

}

void borrarJobsDeMARTA(char* ipJob, char* puertoJob, t_list* jobs,
		t_list* nodosCarga) {

	bool jobBuscado(job* job) {
		return !strcmp(job->ip, ipJob) && !strcmp(job->puerto, puertoJob);

	}

	void eliminarCargas(job* jobi) {

		void sacarCargaSiNoTermino(instruccionMapReduce* instr) {
			if (!instr->terminado) {

				bool nodoIndicado(cargaNodo* nodd) {
					return !strcmp(nodd->ip, instr->ipNodo)
							&& !strcmp(nodd->puerto, instr->puertoNodo);
				}

				cargaNodo* nodoAux = list_find(nodosCarga,
						(void*) nodoIndicado);

				nodoAux->carga--;

				printf("Nodo %d -> Carga %d\n",nodoAux->nombreNodo, nodoAux->carga);
			}
		}

		if (!strcmp(jobi->ip, ipJob) && !strcmp(jobi->puerto, puertoJob)) {
			list_iterate(jobi->instrucciones->instruccionesMap,
					(void*) sacarCargaSiNoTermino);
			list_iterate(jobi->instrucciones->instruccionesReduce,
					(void*) sacarCargaSiNoTermino);
		}

	}

	list_iterate(jobs, (void*) eliminarCargas);

	list_remove_and_destroy_by_condition(jobs, (void*) jobBuscado,
			(void*) destruirJob);
}

void loguearme(char *ruta, char *nombre_proceso, bool mostrar_por_consola,
		t_log_level nivel, const char *mensaje) {

	pthread_mutex_lock(&mutexLogMARTA);
	t_log *log;

	switch (nivel) {
	case (LOG_LEVEL_ERROR):
		log = log_create(ruta, nombre_proceso, mostrar_por_consola, nivel);
		log_error(log, mensaje);
		log_destroy(log);
		break;

	case (LOG_LEVEL_DEBUG):
		log = log_create(ruta, nombre_proceso, mostrar_por_consola, nivel);
		log_debug(log, mensaje);
		log_destroy(log);
		break;

	case (LOG_LEVEL_INFO):
		log = log_create(ruta, nombre_proceso, mostrar_por_consola, nivel);
		log_info(log, mensaje);
		log_destroy(log);
		break;

	case (LOG_LEVEL_TRACE):
		log = log_create(ruta, nombre_proceso, mostrar_por_consola, nivel);
		log_trace(log, mensaje);
		log_destroy(log);
		break;

	case (LOG_LEVEL_WARNING):
		log = log_create(ruta, nombre_proceso, mostrar_por_consola, nivel);
		log_warning(log, mensaje);
		log_destroy(log);
		break;
	}
	pthread_mutex_unlock(&mutexLogMARTA);
}
