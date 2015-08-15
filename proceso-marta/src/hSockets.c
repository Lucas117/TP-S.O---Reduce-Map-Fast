#include "hSockets.h"
#include "hSerializadores.h"
#include "EstructurasMARTA.h"
#include "FuncionesMARTA.h"

//variables globales -----------------------

pthread_mutex_t mutexListasNodos = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexSistema = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexFD = PTHREAD_MUTEX_INITIALIZER;

// ------------------------------------------

void* hiloParaActualizaListaNodos(void** args) {
	actualizardorListaNodos(args[0], args[1], args[2], args[3]);

	return 0;
}

void actualizardorListaNodos(char* puerto_actualizador, fileSystem* file_system,
		t_list* listaNodosEnElSitema, t_list* cargaNodos) {

	struct addrinfo hints;
	struct addrinfo *serverInfo;
	struct sockaddr_in client_addr;

	int fileSystem_fd;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;		// No importa si uso IPv4 o IPv6
	hints.ai_flags = AI_PASSIVE;// Asigna el address del localhost: 127.0.0.1
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, puerto_actualizador, &hints, &serverInfo);

	int listenerSockfd;
	if ((listenerSockfd = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol)) == -1) {
		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR, "socket");

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		exit(1);
	}

	int yes; // un inicador que lo utiliza el setsockpot
	if ((setsockopt(listenerSockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)))
			== -1) {

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR, "setsockopt");

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		exit(1);
	}

	if ((bind(listenerSockfd, serverInfo->ai_addr, serverInfo->ai_addrlen))
			== -1) {

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR, "bind");

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		exit(1);
	}

	freeaddrinfo(serverInfo);

	int sin_size;

	if ((listen(listenerSockfd, BACKLOG)) == -1) {
		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR, "listen");

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		exit(1);
	}

	while (1) {

		sin_size = sizeof(client_addr);
		if ((fileSystem_fd = accept(listenerSockfd,
				(struct sockaddr *) &client_addr, &sin_size)) == -1) {

			loguearme(PATH_LOG, "Proceso MaRTA", 0, LOG_LEVEL_ERROR, "accept");
			continue;
		}

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO,
				"Actualizaciones del FileSystem disponibles");

		esperarPorActualizaciones(fileSystem_fd, file_system,
				listaNodosEnElSitema, cargaNodos);

	}

}

void destruirDatosNodo(datosNodo* nodo) {
	free(nodo->ip);
	free(nodo->puerto);
	free(nodo);
}

void esperarPorActualizaciones(int fileSystem_fd, fileSystem* file_system,
		t_list* listaNodosEnElSitema, t_list* cargaNodos) {

	int accion;

	while (1) {
		int nodos_Disponibles = 0;
		bloque *block = malloc(sizeof(bloque));
		accion = receive_and_unpack(block, fileSystem_fd);

		switch (accion) {
		case -1:
			perror("recv");
			loguearme(PATH_LOG, "Proceso MaRTA", 0, LOG_LEVEL_ERROR,
					"recv actualizador");
			close(fileSystem_fd); //cerrar el descriptor
			break;
		case 0:
			printf("FileSystem se desconecto");
			loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO,
					"Actualizaciones del FileSystem no disponibles");
			close(fileSystem_fd); //cerrar el descriptor
			break;
		case 1:
			pthread_mutex_lock(&mutexListasNodos);

			if (list_size(listaNodosEnElSitema)) {
				list_clean_and_destroy_elements(listaNodosEnElSitema,
						(void*) destruirDatosNodo);
			}

			deserializarActualizacionNodosDisponibles(block,
					listaNodosEnElSitema, file_system);

			void disponibilidadNodo(datosNodo* nodo) {
				nodos_Disponibles += nodo->disponible;
			}

			list_iterate(listaNodosEnElSitema, (void*) disponibilidadNodo);

			if (nodos_Disponibles < file_system->nodosMinimo) {
				file_system->disponible = 0;
			} else {
				file_system->disponible = 1;
			}

			controlarListaNodosCarga(listaNodosEnElSitema, cargaNodos);

			pthread_mutex_unlock(&mutexListasNodos);
		}

		if (accion <= 0) {
			break;
		}
	}

}

int obtenerDescriptorDeFichero(char* ip_destino, char* puerto_destino) {
	struct sockaddr_in socketAddr;  // direccion ip del socket
	int socketAddr_len;

	int fd; // descriptor de fichero de la ip_destinio
	int fd_buscado = -1;

	socketAddr_len = sizeof(socketAddr);
	for (fd = 0; fd <= 100; fd++) {

		getpeername(fd, (struct sockaddr *) &socketAddr, &socketAddr_len);

		if (!strcmp((inet_ntoa(socketAddr.sin_addr)), ip_destino)
				&& !strcmp(string_itoa(ntohs(socketAddr.sin_port)),
						puerto_destino)) {
			fd_buscado = fd;
			break;
		}
	}

	return fd_buscado;

}

int sendAll(int fd, paquete *package) {
	int total = 0;        // cuántos bytes hemos enviado
	uint32_t bytesPendientes = package->longitud_bloque; // cuántos se han quedado pendientes
	int bytesEnviados;
	int package_enviado = 1;
	while (total < package->longitud_bloque) {

		bytesEnviados = send(fd, package->bloque + total, bytesPendientes, 0);
		if (bytesEnviados == -1) {
			loguearme(PATH_LOG, "Proceso MaRTA", 0, LOG_LEVEL_ERROR, "send");
			package_enviado = 0;
			break;
		}

		total += bytesEnviados;
		bytesPendientes -= bytesEnviados;

	}

	return package_enviado;

}

int enviarDatos(int fd_destino, bloque *block) {

	int datosEnviados = 0;
	paquete *package;

	package = pack(block);

	datosEnviados = sendAll(fd_destino, package);

	free(package->bloque);
	free(package);

	return datosEnviados;

}

void escucharConexiones(char* puerto_escucha,
		char* puerto_escucha_ActualizadorLista, t_list* listaNodosEnElSistema,
		t_list* cargaNodos, t_list* jobs, t_list* jobsEnEsperaResultadoGuardado) {

	struct addrinfo hints;
	struct addrinfo *serverInfo;
	struct sockaddr_in client_addr;

	pthread_t hiloActualizadorListaNodo;
	pthread_t hiloPlanificacion;

	fileSystem* file_system = malloc(sizeof(fileSystem));
	file_system->disponible = 0; //inicializa el valor
	file_system->ip = malloc(16);
	file_system->puerto = malloc(6);
	file_system->fd_FileSystem = 0;

	int new_fd; //descriptor de fichero del cliente

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;		// No importa si uso IPv4 o IPv6
	hints.ai_flags = AI_PASSIVE;// Asigna el address del localhost: 127.0.0.1
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, puerto_escucha, &hints, &serverInfo);

	int listenerSockfd;
	if ((listenerSockfd = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol)) == -1) {

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR, "socket");

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		exit(1);
	}

	int yes; // un inicador que lo utiliza el setsockpot
	if ((setsockopt(listenerSockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)))
			== -1) {

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR, "setsockopt");

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		exit(1);
	}

	if ((bind(listenerSockfd, serverInfo->ai_addr, serverInfo->ai_addrlen))
			== -1) {

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR, "bind");

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		exit(1);
	}
	freeaddrinfo(serverInfo);

	int sin_size;

	if ((listen(listenerSockfd, BACKLOG)) == -1) {

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR, "listen");

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		exit(1);
	}

	void* argumentos[4];
	argumentos[0] = puerto_escucha_ActualizadorLista;
	argumentos[1] = file_system;
	argumentos[2] = listaNodosEnElSistema;
	argumentos[3] = cargaNodos;

	// crea hilo para la actualizacion de la lista de nodos
	pthread_create(&hiloActualizadorListaNodo, NULL,
			hiloParaActualizaListaNodos, argumentos);

	void* argumentosPlanificacion[6];

	while (1) {

		pthread_mutex_lock(&mutexFD);

		if ((new_fd = accept(listenerSockfd, (struct sockaddr *) &client_addr,
				&sin_size)) != -1) {

			argumentosPlanificacion[0] = &new_fd;
			argumentosPlanificacion[1] = file_system;
			argumentosPlanificacion[2] = listaNodosEnElSistema;
			argumentosPlanificacion[3] = cargaNodos;
			argumentosPlanificacion[4] = jobs;
			argumentosPlanificacion[5] = jobsEnEsperaResultadoGuardado;

			pthread_create(&hiloPlanificacion, NULL, hiloPlanificacionMarta,
					argumentosPlanificacion);

		} else {
			pthread_mutex_unlock(&mutexFD);
		}
	}

}

void* hiloPlanificacionMarta(void** args) {
	planificacionMarta(args[0], args[1], args[2], args[3], args[4], args[5]);

	return 0;
}

void planificacionMarta(void* new_fd, fileSystem* file_system,
		t_list* listaNodosEnElSistema, t_list* cargaNodos, t_list* jobs,
		t_list* jobsEnEsperaResultadoGuardado) {

	int fd_Conexion = *(int*) new_fd;
	pthread_mutex_unlock(&mutexFD);

	int accion;

	while (1) {
		replanificacionMap* resultadoMap;
		archivo* archivoTrabajoJob;
		caracteristicasJob* caracteristicas_job;
		t_list* listaResultadoReduceParcial;
		trabajoJobTerminado* trabajo_terminado;
		resultado_fileSystem* resultadoFS;
		falloNodo* fallo_nodo;

		bloque *block = malloc(sizeof(bloque));
		accion = receive_and_unpack(block, fd_Conexion); // recive el paquete y retorna una accion
		char* msg_log;

		switch (accion) {
		case -2:
			file_system->fd_FileSystem = fd_Conexion;
			agregarConexionFileSystemAlSistema(file_system);
			break;
		case -1:
		case 0:
			if (file_system->fd_FileSystem == fd_Conexion) {
				pthread_mutex_lock(&mutexSistema);

				msg_log = malloc(
						strlen("FileSYstem no se encuentra disponible - IP: ")
								+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

				strcpy(msg_log, "FileSYstem no se encuentra disponible - IP: ");
				strcat(msg_log, file_system->ip);
				strcat(msg_log, " PUERTO: ");
				strcat(msg_log, file_system->puerto);

				loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_ERROR,
						msg_log);

				free(msg_log);

				file_system->fd_FileSystem = 0;
				avisarJobsEnEspera(jobsEnEsperaResultadoGuardado);

				close(fd_Conexion); //cerrar el descriptor

				pthread_mutex_unlock(&mutexSistema);
			} else {

				pthread_mutex_lock(&mutexSistema);
				if (list_size(jobs)) {
					eliminarJobDelSistem(fd_Conexion, jobs, cargaNodos);
				}
				pthread_mutex_unlock(&mutexSistema);
				close(fd_Conexion); //cerrar el descriptor
			}

			break;
		case 1:
			caracteristicas_job = deserializarPedirTrabajoDeArchivoAMarta(
					block);

			pedirBloquesArchivoFileSystem(caracteristicas_job, fd_Conexion,
					file_system, jobs);
			break;
		case 2:
			//tabla de archivos filesystem-marta
			archivoTrabajoJob = deserializarBloquesParaMarta(block);
			asignarBloquesAJobYPlanificar(archivoTrabajoJob, jobs,
					listaNodosEnElSistema, cargaNodos);

			break;
		case 3:
			//resultado de un trabajo map
			if (file_system->fd_FileSystem) {
				if (file_system->disponible) {
					resultadoMap = deserializarResultadoMapMarta(block);
					agregarAddressResultadoJob(resultadoMap, fd_Conexion);
					determinarResultadoOperacionMap(resultadoMap, jobs,
							cargaNodos, listaNodosEnElSistema, fd_Conexion);
				} else {
					fileSystemNoDisponible(fd_Conexion);
				}
			} else {
				fileSystemNoConectadaDisponible(fd_Conexion);
			}
			break;
		case 4:
			//resultado reduce parcial
			if (file_system->fd_FileSystem) {
				if (file_system->disponible) {
					listaResultadoReduceParcial =
							deserializarResultadoReduceParcial(block);
					agregarAddresResultadoReduce(listaResultadoReduceParcial,
							fd_Conexion);
					terminaronTodasLasInstruccionesConCombiner(
							listaResultadoReduceParcial, jobs, cargaNodos,
							listaNodosEnElSistema, fd_Conexion, &mutexSistema,
							&mutexListasNodos);
				} else {
					fileSystemNoDisponible(fd_Conexion);
				}

			} else {
				fileSystemNoConectadaDisponible(fd_Conexion);
			}
			break;
		case 5:
			//resultado Exitoso de trabajo job
			trabajo_terminado = deserializarResultadoReduceExitoso(block);

			terminarTrabajoJob(fd_Conexion, trabajo_terminado, file_system,
					jobsEnEsperaResultadoGuardado);

			break;
		case 6:
			//no existe archivo pedido
			deserializarArchivoPedidoNoExiste(block, jobs);
			break;
		case 7:
			//resultado fallido reduce sin combiner
			fallo_nodo = deserializarResultadoReducerFallo(block);
			falloOperacion(fd_Conexion, fallo_nodo);
			break;
		case 8:
			//resultado fallido reduce con combiner
			fallo_nodo = deserializarResultadoReducerFallo(block);
			falloOperacion(fd_Conexion, fallo_nodo);
			break;
		case 9:
			resultadoFS = deserializarGuardadoResultadoFileSystem(block);
			informarJobResultadoGuardado(resultadoFS,
					jobsEnEsperaResultadoGuardado);
			break;
		}

		if (accion <= 0 && accion != -2) {
			break;
		}
	}

	return;
}

void agregarAddresResultadoReduce(t_list* listaResultadoReduceParcial,
		int fdJob) {
	void agregarAddres(replanificacionMap* instruccion) {
		agregarAddressResultadoJob(instruccion, fdJob);
	}

	list_iterate(listaResultadoReduceParcial, (void*) agregarAddres);
}

int receive_and_unpack(bloque *block, int clientSocket) {

	int accion; // devuelve una accion para el manejo de los datos

	int total = 0;        // cuántos bytes hemos enviado
	uint32_t bytesPendientes = sizeof(block->longitud_datos); // cuántos se han quedado pendientes

	while (total < sizeof(block->longitud_datos)) {
		if ((accion = recv(clientSocket, &block->longitud_datos + total,
				bytesPendientes, 0)) <= 0) {
			return accion; // un error
			break;
		}

		total += accion;
		bytesPendientes -= accion;

	}

	block->datos = malloc(block->longitud_datos); //reserva memoria para los datos

	accion = 0;
	total = 0;
	bytesPendientes = block->longitud_datos;

	while (total < block->longitud_datos) {
		if ((accion = recv(clientSocket, block->datos + total, bytesPendientes,
				0)) <= 0) {
			return accion; // un error
			break;
		}

		total += accion;
		bytesPendientes -= accion;

	}

	memcpy(&(accion), block->datos, sizeof(uint32_t));
	return accion;

}

void agregarConexionFileSystemAlSistema(fileSystem* file_system) {

	struct sockaddr_in socketAddr;  // direccion ip del socket
	int socketAddr_len;
	char* msg_log;

	socketAddr_len = sizeof(socketAddr);
	getpeername(file_system->fd_FileSystem, (struct sockaddr *) &socketAddr,
			&socketAddr_len); // sacar la direccion y puerto

	strcpy(file_system->ip, inet_ntoa(socketAddr.sin_addr));
	strcpy(file_system->puerto, string_itoa(ntohs(socketAddr.sin_port)));

	msg_log = malloc(
			strlen("Conexion FileSystem - IP: ") + 1 + 16 + strlen(" PUERTO: ") + 1
					+ 6);

	strcpy(msg_log, "Conexion FileSystem - IP: ");
	strcat(msg_log, file_system->ip);
	strcat(msg_log, " PUERTO: ");
	strcat(msg_log, file_system->puerto);

	loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO, msg_log);

	free(msg_log);

}

void agregarAddressResultadoJob(replanificacionMap* resultadoMap, int fdJob) {
	struct sockaddr_in socketAddr;  // direccion ip del socket
	int socketAddr_len;

	socketAddr_len = sizeof(socketAddr);
	getpeername(fdJob, (struct sockaddr *) &socketAddr, &socketAddr_len); // sacar la direccion y puerto

	resultadoMap->ipJob = malloc(16);
	resultadoMap->puertoJob = malloc(6);

	strcpy(resultadoMap->ipJob, inet_ntoa(socketAddr.sin_addr));
	strcpy(resultadoMap->puertoJob, string_itoa(ntohs(socketAddr.sin_port)));
}

void agregarAddressJob(job* datos_job, int fdJob) {
	struct sockaddr_in socketAddr;  // direccion ip del socket
	int socketAddr_len;

	socketAddr_len = sizeof(socketAddr);
	getpeername(fdJob, (struct sockaddr *) &socketAddr, &socketAddr_len); // sacar la direccion y puerto

	datos_job->ip = malloc(16);
	datos_job->puerto = malloc(6);
	datos_job->instrucciones = NULL;
	datos_job->datosBloque = NULL;

	strcpy(datos_job->ip, inet_ntoa(socketAddr.sin_addr));
	strcpy(datos_job->puerto, string_itoa(ntohs(socketAddr.sin_port)));
}

void agregarJobAlSistema(job* datos_job, int fd_job, t_list* jobs) {

	agregarAddressJob(datos_job, fd_job);

	list_add(jobs, datos_job);

}

void resultadoOperacionMapConCombiner(replanificacionMap* resultadoMap,
		job* jobBuscado, t_list* jobs, t_list * nodosConCarga,
		t_list* listaNodosEnElSistema, int fdJob) {

	pthread_mutex_lock(&mutexSistema);

	if (resultadoMap->resultado) {

		bool buscarNodo(instruccionMapReduce* instruccion) {
			return !strcmp(instruccion->ipNodo, resultadoMap->ipNodo)
					&& !strcmp(instruccion->puertoNodo,
							resultadoMap->puertoNodo)
					&& instruccion->bloqueNodo == resultadoMap->bloqueNodo;
		}

		bool mapTerminado(instruccionMapReduce* instruccion) {
			return instruccion->terminado;
		}

		instruccionMapReduce* instruccionBuscada = list_find(
				jobBuscado->instrucciones->instruccionesMap,
				(void*) buscarNodo);
		instruccionBuscada->terminado = resultadoMap->resultado;

		bool nodoDeInstruccion(cargaNodo* nodo) {
			return !strcmp(nodo->ip, resultadoMap->ipNodo)
					&& !strcmp(nodo->puerto, resultadoMap->puertoNodo);
		}

		cargaNodo* nodoAux = list_find(nodosConCarga, nodoDeInstruccion);
		nodoAux->carga = nodoAux->carga - 1;

		printf("Nodo %d -> Carga %d\n",nodoAux->nombreNodo, nodoAux->carga);

		bool maperEnMismoNodo(instruccionMapReduce* maper) {
			return !strcmp(maper->ipNodo, instruccionBuscada->ipNodo)
					&& !strcmp(maper->puertoNodo,
							instruccionBuscada->puertoNodo);
		}

		bool jobIgualAlOtro(job* jobAux) {
			return !strcmp(jobAux->ip, jobBuscado->ip)
					&& !strcmp(jobAux->puerto, jobBuscado->puerto);
		}

		t_list* jobsFiltrados = list_filter(jobs, (void*) jobIgualAlOtro);

		t_list* mapersEnElMismoNodo = list_create();

		void filtrarMapers(job* jobAuxx) {
			list_add_all(mapersEnElMismoNodo,
					list_filter(jobAuxx->instrucciones->instruccionesMap,
							(void*) maperEnMismoNodo));
		}

		list_iterate(jobsFiltrados, (void*) filtrarMapers);

		char* msg_log;
		msg_log = malloc(
				strlen("Tarea Map: Job - IP: ") + 1 + 16 + strlen(" PUERTO: ")
						+ 1 + 6);

		strcpy(msg_log, "Tarea Map: Job - IP: ");
		strcat(msg_log, jobBuscado->ip);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, jobBuscado->puerto);

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_TRACE, msg_log);

		free(msg_log);

//		void mostrar_tareas_pendientes(instruccionMapReduce * instruccion_log) {

			bool obtenerNodoConCarga(cargaNodo* carga_nod) {
				return !strcmp(carga_nod->ip, instruccionBuscada->ipNodo)
						&& !strcmp(carga_nod->puerto,
								instruccionBuscada->puertoNodo);
			}

//			if (instruccion_log->terminado) {

				cargaNodo* carga_nodo_msg = list_find(nodosConCarga,
						(void*) obtenerNodoConCarga);
				char* carga_nodo_msg_log = string_itoa(carga_nodo_msg->carga);

				msg_log = malloc(
						strlen("Map TERMINADO -> Nodo - IP: ") + 1 + 16
								+ strlen(" PUERTO: ") + 1 + 6
								+ strlen(" CARGA: ") + 1 + 4);

				strcpy(msg_log, "Map TERMINADO -> Nodo - IP: ");
				strcat(msg_log, instruccionBuscada->ipNodo);
				strcat(msg_log, " PUERTO: ");
				strcat(msg_log, instruccionBuscada->puertoNodo);
				strcat(msg_log, " CARGA: ");
				strcat(msg_log, carga_nodo_msg_log);

				loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO,
						msg_log);

				free(msg_log);
				free(carga_nodo_msg_log);

				printf("////////////////////////////////////////////\n");

//			} else {
//				cargaNodo* carga_nodo_msg = list_find(nodosConCarga,
//						(void*) obtenerNodoConCarga);
//				char* carga_nodo_msg_log = string_itoa(carga_nodo_msg->carga);
//				char* bloque_nodo_log = string_itoa(
//						instruccion_log->bloqueNodo);
//
//				msg_log = malloc(
//						strlen("Map PENDIENTE -> Nodo - IP: ") + 1 + 16
//								+ strlen(" PUERTO: ") + 1 + 6
//								+ strlen(" BLOQUE: ") + 1 + 4
//								+ strlen(" CARGA: ") + 1 + 4);
//
//				strcpy(msg_log, "Map PENDIENTE -> Nodo - IP: ");
//				strcat(msg_log, instruccion_log->ipNodo);
//				strcat(msg_log, " PUERTO: ");
//				strcat(msg_log, instruccion_log->puertoNodo);
//				strcat(msg_log, " BLOQUE: ");
//				strcat(msg_log, bloque_nodo_log);
//				strcat(msg_log, " CARGA: ");
//				strcat(msg_log, carga_nodo_msg_log);
//
//				loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO,
//						msg_log);
//
//				free(msg_log);
//				free(carga_nodo_msg_log);
//				free(bloque_nodo_log);

//			}
//
//		}

//		list_iterate(jobBuscado->instrucciones->instruccionesMap,
//				(void*) mostrar_tareas_pendientes);

		if (list_all_satisfy(mapersEnElMismoNodo, (void*) mapTerminado)) {
			pthread_mutex_unlock(&mutexSistema);
			crearReducersParaJobConCombiner(jobsFiltrados, nodosConCarga,
					mapersEnElMismoNodo, fdJob, &mutexSistema,
					&mutexListasNodos); // crea los reduce
		}

		pthread_mutex_unlock(&mutexSistema);

	} else {
		pthread_mutex_unlock(&mutexSistema);
		replanificarMapEnJob(resultadoMap, jobs, listaNodosEnElSistema,
				nodosConCarga, fdJob, &mutexSistema, &mutexListasNodos);
	}

}

void resultadoOperacionMapSinCombiner(replanificacionMap* resultadoMap,
		job* jobBuscado, t_list* jobs, t_list * nodosConCarga,
		t_list* listaNodosEnElSistema, int fdJob) {

	pthread_mutex_lock(&mutexSistema);

	if (resultadoMap->resultado) {

		bool buscarNodo(instruccionMapReduce* instruccion) {
			return !strcmp(instruccion->ipNodo, resultadoMap->ipNodo)
					&& !strcmp(instruccion->puertoNodo,
							resultadoMap->puertoNodo)
					&& instruccion->bloqueNodo == resultadoMap->bloqueNodo;
		}

		bool mapTerminado(instruccionMapReduce* instruccion) {
			return instruccion->terminado;
		}

		instruccionMapReduce* instruccionBuscada = list_find(
				jobBuscado->instrucciones->instruccionesMap,
				(void*) buscarNodo);
		instruccionBuscada->terminado = resultadoMap->resultado;

		bool nodoDeInstruccion(cargaNodo* nodo) {
			return !strcmp(nodo->ip, resultadoMap->ipNodo)
					&& !strcmp(nodo->puerto, resultadoMap->puertoNodo);
		}

		cargaNodo* nodoAux = list_find(nodosConCarga, nodoDeInstruccion);
		nodoAux->carga = nodoAux->carga - 1;

		printf("Nodo %d -> Carga %d\n",nodoAux->nombreNodo, nodoAux->carga);

		char* msg_log;
		msg_log = malloc(
				strlen("Tarea Map: Job - IP: ") + 1 + 16 + strlen(" PUERTO: ")
						+ 1 + 6);

		strcpy(msg_log, "Tarea Map: Job - IP: ");
		strcat(msg_log, jobBuscado->ip);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, jobBuscado->puerto);

		loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_TRACE, msg_log);

		free(msg_log);

		void mostrar_tareas_pendientes(instruccionMapReduce * instruccion_log) {

			bool obtenerNodoConCarga(cargaNodo* carga_nod) {
				return !strcmp(carga_nod->ip, instruccion_log->ipNodo)
						&& !strcmp(carga_nod->puerto,
								instruccion_log->puertoNodo);
			}

			if (instruccion_log->terminado) {

				cargaNodo* carga_nodo_msg = list_find(nodosConCarga,
						(void*) obtenerNodoConCarga);
				char* carga_nodo_msg_log = string_itoa(carga_nodo_msg->carga);

				msg_log = malloc(
						strlen("Map TERMINADO -> Nodo - IP: ") + 1 + 16
								+ strlen(" PUERTO: ") + 1 + 6
								+ strlen(" CARGA: ") + 1 + 4);

				strcpy(msg_log, "Map TERMINADO -> Nodo - IP: ");
				strcat(msg_log, instruccion_log->ipNodo);
				strcat(msg_log, " PUERTO: ");
				strcat(msg_log, instruccion_log->puertoNodo);
				strcat(msg_log, " CARGA: ");
				strcat(msg_log, carga_nodo_msg_log);

				loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO,
						msg_log);

				free(msg_log);
				free(carga_nodo_msg_log);

			} else {
				cargaNodo* carga_nodo_msg = list_find(nodosConCarga,
						(void*) obtenerNodoConCarga);
				char* carga_nodo_msg_log = string_itoa(carga_nodo_msg->carga);
				char* bloque_nodo_log = string_itoa(
						instruccion_log->bloqueNodo);

				msg_log = malloc(
						strlen("Map PENDIENTE -> Nodo - IP: ") + 1 + 16
								+ strlen(" PUERTO: ") + 1 + 6
								+ strlen(" BLOQUE: ") + 1 + 4
								+ strlen(" CARGA: ") + 1 + 4);

				strcpy(msg_log, "Map TERMINADO -> Nodo - IP: ");
				strcat(msg_log, instruccion_log->ipNodo);
				strcat(msg_log, " PUERTO: ");
				strcat(msg_log, instruccion_log->puertoNodo);
				strcat(msg_log, " BLOQUE: ");
				strcat(msg_log, bloque_nodo_log);
				strcat(msg_log, " CARGA: ");
				strcat(msg_log, carga_nodo_msg_log);

				loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO,
						msg_log);

				free(msg_log);
				free(carga_nodo_msg_log);
				free(bloque_nodo_log);

			}

		}

		list_iterate(jobBuscado->instrucciones->instruccionesMap,
				(void*) mostrar_tareas_pendientes);

		bool todosJobsTerminados(job* job) {

			if (!strcmp(job->ip, jobBuscado->ip)
					&& !strcmp(job->puerto, jobBuscado->puerto)) {

				return list_all_satisfy(job->instrucciones->instruccionesMap,
						(void*) mapTerminado);
			} else
				return 1;
		}

		if (list_all_satisfy(jobs, (void*) todosJobsTerminados)) {

			bool jobIgualAlOtro(job* jobAux) {
				return !strcmp(jobAux->ip, jobBuscado->ip)
						&& !strcmp(jobAux->puerto, jobBuscado->puerto);
			}

			t_list* jobsFiltrados = list_filter(jobs, (void*) jobIgualAlOtro);

			pthread_mutex_unlock(&mutexSistema);
			crearReducersParaJobsSinCombiner(jobsFiltrados, nodosConCarga,
					fdJob, &mutexSistema, &mutexListasNodos); // crea los reduce
		}

	} else {
		pthread_mutex_unlock(&mutexSistema);
		replanificarMapEnJob(resultadoMap, jobs, listaNodosEnElSistema,
				nodosConCarga, fdJob, &mutexSistema, &mutexListasNodos);
	}

	pthread_mutex_unlock(&mutexSistema);

}

void determinarResultadoOperacionMap(replanificacionMap* resultadoMap,
		t_list* jobs, t_list * nodosConCarga, t_list* listaNodosEnElSistema,
		int fdJob) {

	pthread_mutex_lock(&mutexSistema);

	bool buscarJob(job* job) {
		return !strcmp(job->ip, resultadoMap->ipJob)

		&& !strcmp(job->puerto, resultadoMap->puertoJob)
				&& !strcmp(job->direccionArchivo, resultadoMap->archivoTrabajo);
	}

	job* jobBuscado = list_find(jobs, (void*) buscarJob);

	if (jobBuscado->combiner) {
		pthread_mutex_unlock(&mutexSistema);
		resultadoOperacionMapConCombiner(resultadoMap, jobBuscado, jobs,
				nodosConCarga, listaNodosEnElSistema, fdJob);
	} else {
		pthread_mutex_unlock(&mutexSistema);
		resultadoOperacionMapSinCombiner(resultadoMap, jobBuscado, jobs,
				nodosConCarga, listaNodosEnElSistema, fdJob);
	}

}

void destruirMapReduce(instruccionMapReduce* mapreduce) {
	free(mapreduce->ipNodo);
	free(mapreduce->puertoNodo);
	free(mapreduce);
}

void destruirInstruccion(instrucciones* instr) {
	free(instr->ipNodoPrincipal);
	free(instr->puertoNodoPrincipal);
	if (instr->instruccionesMap != NULL) {
		list_destroy_and_destroy_elements(instr->instruccionesMap,
				(void*) destruirMapReduce);
	}
	if (instr->instruccionesReduce != NULL) {
		list_destroy_and_destroy_elements(instr->instruccionesReduce,
				(void*) destruirMapReduce);
	}
	free(instr);
}

void destruirJob(job* job) {

	void destruirCopia(infoCopia* copia) {
		free(copia->ip);
		free(copia->puerto);
		free(copia);
	}

	void destruirBloque(datoBloque* bloque) {
		list_destroy_and_destroy_elements(bloque->copias,
				(void*) destruirCopia);
		free(bloque);
	}
	if (job->instrucciones != NULL) {
		destruirInstruccion(job->instrucciones);
	}
	if (job->datosBloque != NULL) {
		list_destroy_and_destroy_elements(job->datosBloque,
				(void*) destruirBloque);
	}
	free(job->direccionArchivo);
	free(job->ip);
	free(job->puerto);
	free(job);
}

void pedirBloquesArchivoFileSystem(caracteristicasJob* caracteristicas_job,
		int fd_Job, fileSystem* file_system, t_list* jobs) {

	int accion;
	int realizarPlanificacion = 1;
	int fileSystemConectado = 1;
	bloque* bloqueAEnviar;
	bloque* block = malloc(sizeof(bloque));
	job* datos_job = malloc(sizeof(job));
	char* msg_log;

	void obtenerBloqueFileSystem(char* archivo) {

		pthread_mutex_lock(&mutexSistema);

		if (file_system->fd_FileSystem && realizarPlanificacion
				&& fileSystemConectado) {
			if (file_system->disponible && realizarPlanificacion) {
				job* datos_job = malloc(sizeof(job));
				datos_job->combiner = caracteristicas_job->combiner;
				datos_job->direccionArchivo = malloc(strlen(archivo) + 1);
				strcpy(datos_job->direccionArchivo, archivo);
				agregarJobAlSistema(datos_job, fd_Job, jobs);

				msg_log = malloc(
						strlen("Conexion Job - IP: ") + 1 + 16
								+ strlen(" PUERTO: ") + 1 + 6);

				strcpy(msg_log, "Conexion Job - IP: ");
				strcat(msg_log, datos_job->ip);
				strcat(msg_log, " PUERTO: ");
				strcat(msg_log, datos_job->puerto);

				loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO, msg_log);

				free(msg_log);

				bloqueAEnviar = serializarPedidoBloquesDeArchivo(datos_job);
				enviarDatos(file_system->fd_FileSystem, bloqueAEnviar);

			} else {
				realizarPlanificacion = 0;
			}

		} else {
			fileSystemConectado = 0;
		}

		pthread_mutex_unlock(&mutexSistema);

	}

	list_iterate(caracteristicas_job->archivosTrabajo,
			(void*) obtenerBloqueFileSystem);

	if (!fileSystemConectado) {
		bool jobConIpYPuertoIgual(job* job) {
			return !strcmp(job->ip, datos_job->ip)
					&& !strcmp(job->puerto, datos_job->puerto);
		}

		pthread_mutex_lock(&mutexSistema);
		bloque* bloqueAEnviar;
		bloqueAEnviar = serializarFileSystemNoConectado();
		enviarDatos(fd_Job, bloqueAEnviar);

		if (list_size(jobs)) {
			list_remove_and_destroy_by_condition(jobs,
					(void*) jobConIpYPuertoIgual, (void*) destruirJob);
		}
		pthread_mutex_unlock(&mutexSistema);

	} else {

		if (!realizarPlanificacion) {

			bool jobConIpYPuertoIgual(job* job) {
				return !strcmp(job->ip, datos_job->ip)
						&& !strcmp(job->puerto, datos_job->puerto);
			}

			pthread_mutex_lock(&mutexSistema);
			bloque* bloqueAEnviar;
			bloqueAEnviar = serializarFileSystemNoDisponible();
			enviarDatos(fd_Job, bloqueAEnviar);

			if (list_size(jobs)) {
				list_remove_and_destroy_by_condition(jobs,
						(void*) jobConIpYPuertoIgual, (void*) destruirJob);
			}
			pthread_mutex_unlock(&mutexSistema);

		}
	}

}

void avisarNoExisteArchivoPedidoJob(int fdJob, t_list* jobs,
		char* archivo_buscado) {
	struct sockaddr_in socketAddr;  // direccion ip del socket
	int socketAddr_len;

	char* ipJob = malloc(16);
	char* puertoJob = malloc(6);

	socketAddr_len = sizeof(socketAddr);
	getpeername(fdJob, (struct sockaddr *) &socketAddr, &socketAddr_len); // sacar la direccion y puerto

	strcpy(ipJob, inet_ntoa(socketAddr.sin_addr));
	strcpy(puertoJob, string_itoa(ntohs(socketAddr.sin_port)));

	bool jobConIpYPuertoIgual(job* job) {
		return !strcmp(job->ip, ipJob) && !strcmp(job->puerto, puertoJob);
	}

	pthread_mutex_lock(&mutexSistema);
	bloque* bloqueAEnviar;
	bloqueAEnviar = serializarNoExisteArchivoPedido(archivo_buscado);
	enviarDatos(fdJob, bloqueAEnviar);

	if (list_size(jobs)) {
		list_remove_and_destroy_by_condition(jobs, (void*) jobConIpYPuertoIgual,
				(void*) destruirJob);
	}
	pthread_mutex_unlock(&mutexSistema);
}

void eliminarJobDelSistem(int fdJob, t_list* jobs, t_list* nodosCarga) {
	struct sockaddr_in socketAddr;  // direccion ip del socket
	int socketAddr_len;
	char* ipJob = malloc(16);
	char* puertoJob = malloc(6);
	char* msg_log;

	socketAddr_len = sizeof(socketAddr);
	getpeername(fdJob, (struct sockaddr *) &socketAddr, &socketAddr_len); // sacar la direccion y puerto

	strcpy(ipJob, inet_ntoa(socketAddr.sin_addr));
	strcpy(puertoJob, string_itoa(ntohs(socketAddr.sin_port)));

	msg_log = malloc(
			strlen("Desconexion Job - IP: ") + 1 + 16 + strlen(" PUERTO: ") + 1
					+ 6);

	strcpy(msg_log, "Desconexion Job - IP: ");
	strcat(msg_log, ipJob);
	strcat(msg_log, " PUERTO: ");
	strcat(msg_log, puertoJob);

	loguearme(PATH_LOG, "Proceso MaRTA", 1, LOG_LEVEL_INFO, msg_log);

	free(msg_log);

	borrarJobsDeMARTA(ipJob, puertoJob, jobs, nodosCarga);

	free(ipJob);
	free(puertoJob);
}

void falloOperacion(int fdJob, falloNodo* fallo_nodo) {
	bloque* bloqueAEnviar;

	bloqueAEnviar = serializarFalloOperacionJob(fallo_nodo);
	enviarDatos(fdJob, bloqueAEnviar);
}

void terminarTrabajoJob(int fdJob, trabajoJobTerminado* trabajo_terminado,
		fileSystem* file_system, t_list* jobsEnEsperaResultadoGuardado) {

	struct sockaddr_in socketAddr;  // direccion ip del socket
	int socketAddr_len;

	bloque* bloqueAEnviar;

	if (file_system->disponible) {

		pthread_mutex_lock(&mutexSistema);

		socketAddr_len = sizeof(socketAddr);
		getpeername(fdJob, (struct sockaddr *) &socketAddr, &socketAddr_len); // sacar la direccion y puerto

		trabajo_terminado->ipJob = malloc(16);
		trabajo_terminado->puertoJob = malloc(6);

		strcpy(trabajo_terminado->ipJob, inet_ntoa(socketAddr.sin_addr));
		strcpy(trabajo_terminado->puertoJob,
				string_itoa(ntohs(socketAddr.sin_port)));

		bloqueAEnviar = serializarGuardarResultadoFileSystem(trabajo_terminado);
		enviarDatos(file_system->fd_FileSystem, bloqueAEnviar);

		list_add(jobsEnEsperaResultadoGuardado, trabajo_terminado);

		pthread_mutex_unlock(&mutexSistema);

	} else {

		pthread_mutex_lock(&mutexSistema);
		bloqueAEnviar = serializarFileSystemNoDisponible();
		enviarDatos(fdJob, bloqueAEnviar);
		pthread_mutex_unlock(&mutexSistema);
	}
}

void fileSystemNoDisponible(int fdJob) {
	bloque* bloqueAEnviar;

	bloqueAEnviar = serializarFileSystemNoDisponible();
	enviarDatos(fdJob, bloqueAEnviar);
}

void fileSystemNoConectadaDisponible(int fdJob) {
	bloque* bloqueAEnviar;

	bloqueAEnviar = serializarFileSystemNoConectado();
	enviarDatos(fdJob, bloqueAEnviar);
}

void asignarBloquesAJobYPlanificar(archivo* archivoTrabajoJob, t_list* jobs,
		t_list* nodosEnSistema, t_list* nodosCarga) {

	pthread_mutex_lock(&mutexSistema);

	int fdJob;

	bool jobBuscado(job* job) {
		return !strcmp(job->direccionArchivo,
				archivoTrabajoJob->direccionArchivo)
				&& !strcmp(job->ip, archivoTrabajoJob->ipJob)
				&& !strcmp(job->puerto, archivoTrabajoJob->puertoJob);
	}

	if (list_any_satisfy(jobs, (void*) jobBuscado)) {

		job* jobPosta = list_find(jobs, (void*) jobBuscado);

		pthread_mutex_unlock(&mutexSistema);

		jobPosta->datosBloque = archivoTrabajoJob->listaBloques;

		fdJob = obtenerDescriptorDeFichero(archivoTrabajoJob->ipJob,
				archivoTrabajoJob->puertoJob);

		crearMappersParaJob(jobPosta, nodosEnSistema, nodosCarga, fdJob,
				&mutexSistema, &mutexListasNodos);
	}

	pthread_mutex_unlock(&mutexSistema);

}

void destruirJobEnEspera(trabajoJobTerminado* jobEnEspera) {
	free(jobEnEspera->ipJob);
	free(jobEnEspera->puertoJob);
	free(jobEnEspera->ipNodo);
	free(jobEnEspera->puertoNodo);
	free(jobEnEspera->resultadoOperacion);
	free(jobEnEspera);

}

void informarJobResultadoGuardado(resultado_fileSystem* resultadoFS,
		t_list* jobsEnEsperaResultadoGuardado) {

	pthread_mutex_lock(&mutexSistema);

	int fdJob;
	bloque* bloqueAEnviar;

	bool existeJobEnLaLista(trabajoJobTerminado* job) {
		return !strcmp(job->ipJob, resultadoFS->ipJob)
				&& !strcmp(job->puertoJob, resultadoFS->puertoJob);
	}

	if (list_size(jobsEnEsperaResultadoGuardado)) {
		if (list_any_satisfy(jobsEnEsperaResultadoGuardado,
				(void*) existeJobEnLaLista)) {

			fdJob = obtenerDescriptorDeFichero(resultadoFS->ipJob,
					resultadoFS->puertoJob);
			bloqueAEnviar = serializarOperacionJobExitosa();
			enviarDatos(fdJob, bloqueAEnviar);

			list_remove_and_destroy_by_condition(jobsEnEsperaResultadoGuardado,
					(void*) existeJobEnLaLista, (void*) destruirJobEnEspera);
		}
	}

	pthread_mutex_unlock(&mutexSistema);

}

void avisarJobsEnEspera(t_list* jobsEnEsperaResultadoGuardado) {

	void avisarJob(trabajoJobTerminado* job) {
		bloque* bloqueAEnviar;
		int fdJob;

		fdJob = obtenerDescriptorDeFichero(job->ipJob, job->puertoJob);
		bloqueAEnviar = serializarFileSystemNoConectado();
		enviarDatos(fdJob, bloqueAEnviar);
	}

	if (list_size(jobsEnEsperaResultadoGuardado)) {
		list_iterate(jobsEnEsperaResultadoGuardado, (void*) avisarJob);
	}

	list_clean_and_destroy_elements(jobsEnEsperaResultadoGuardado,
			(void*) destruirJobEnEspera);

}

void controlarListaNodosCarga(t_list* nodosSistema, t_list* nodosConCarga) {

	void agregarEstructuraCarga(datosNodo* nodo) {

		bool estaEnLaLista(cargaNodo* nodoCarga) {
			return nodo->nombreNodo == nodoCarga->nombreNodo;
		}

		if (list_any_satisfy(nodosConCarga, (void*) estaEnLaLista)) {
			return;
		} else {
			cargaNodo* nodoNuevo = malloc(sizeof(cargaNodo));
			nodoNuevo->nombreNodo = nodo->nombreNodo;
			nodoNuevo->ip = malloc(16);
			strcpy(nodoNuevo->ip, nodo->ip);
			nodoNuevo->puerto = malloc(6);
			strcpy(nodoNuevo->puerto, nodo->puerto);
			nodoNuevo->carga = 0;
			list_add(nodosConCarga, nodoNuevo);

		}
	}

	list_iterate(nodosSistema, (void*) agregarEstructuraCarga);
}
