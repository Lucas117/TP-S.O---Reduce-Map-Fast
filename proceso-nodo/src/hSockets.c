#include "hSockets.h"
#include "hSerializadores.h"
#include "funcionesNodo.h"
#include "estructurasNodo.h"

//variables globales -----------------------

pthread_mutex_t mutexConexiones = PTHREAD_MUTEX_INITIALIZER;

char* puerto_escucha;

// ------------------------------------------

int crearConexion(char* ip_destino, char* puerto_destino) {

	struct addrinfo hints_Local;
	struct addrinfo *serverInfo_Local;

	//Obtiene los datos de la direccion de red y lo guarda en serverInfo_Remoto.
	struct addrinfo hints_Remoto;
	struct addrinfo *serverInfo_Remoto;

	int fd_conexionRemota;

	memset(&hints_Local, 0, sizeof(hints_Local));
	hints_Local.ai_family = AF_UNSPEC;		// No importa si uso IPv4 o IPv6
	hints_Local.ai_flags = AI_PASSIVE;// Asigna el address del localhost: 127.0.0.1
	hints_Local.ai_socktype = SOCK_STREAM;// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, puerto_escucha, &hints_Local, &serverInfo_Local);

	fd_conexionRemota = socket(serverInfo_Local->ai_family,
			serverInfo_Local->ai_socktype, serverInfo_Local->ai_protocol);

	// para setear los valores de la conexion remota
	memset(&hints_Remoto, 0, sizeof(hints_Remoto));
	hints_Remoto.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
	hints_Remoto.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(ip_destino, puerto_destino, &hints_Remoto, &serverInfo_Remoto);

	if ((connect(fd_conexionRemota, serverInfo_Remoto->ai_addr,
			serverInfo_Remoto->ai_addrlen)) == -1) {

		freeaddrinfo(serverInfo_Remoto);

		return 0;

	}

	freeaddrinfo(serverInfo_Remoto);

	return fd_conexionRemota;

}

void* conexionFileSystem(void** args) {
	crearConexionConFileSystem(args[0], args[1], args[2], args[3]);

	return 0;
}

void crearConexionConFileSystem(char* ip_destino, char* puerto_destino,
		void* nombre_Nodo, void* capacidad_Nodo) {

	bloque* bloqueAEnviar;

	int nombreNodo = *(int*) nombre_Nodo;
	int capacidadNodo = *(int*) capacidad_Nodo;
	int fd_conexionRemota;

	fd_conexionRemota = crearConexion(ip_destino, puerto_destino);

	if (!fd_conexionRemota) {
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"No se puede establecer conexion con FileSystem");
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		//destruirListaGlobal();
		unmapearDataBinAMemoria();
		exit(1);
	}
	char * me_conecte_fs = malloc(
			strlen("Conexion establecida con el FileSystem IP: ")
					+ strlen(ip_destino) + strlen(" PUERTO: ")
					+ strlen(puerto_destino) + 1);
	strcpy(me_conecte_fs, "Conexion establecida con el FileSytem IP: ");
	strcat(me_conecte_fs, ip_destino);
	strcat(me_conecte_fs, " PUERTO: ");
	strcat(me_conecte_fs, puerto_destino);

	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO, me_conecte_fs);

	free(me_conecte_fs);

	bloqueAEnviar = serializarNombreYCapacidadNodo(nombreNodo, capacidadNodo,
			puerto_escucha);

	enviarDatos(fd_conexionRemota, bloqueAEnviar);

	escucharFileSystem(fd_conexionRemota);

}

int sendAll(int fd, paquete *package) {
	int total = 0;        // cuántos bytes hemos enviado
	uint32_t bytesPendientes = package->longitud_bloque; // cuántos se han quedado pendientes
	int bytesEnviados;
	int package_enviado = 1;
	while (total < package->longitud_bloque) {

		bytesEnviados = send(fd, package->bloque + total, bytesPendientes, 0);
		if (bytesEnviados == -1) {
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

void escucharFileSystem(int fdDestino) {
	int accion;

	while (1) {

		bloque *block = malloc(sizeof(bloque));
		accion = receive_and_unpack(block, fdDestino); // recive el paquete y retorna una accion
		switch (accion) {
		case -1:
			loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
					"Perror recv");
			close(fdDestino); //cerrar el descriptor
			loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
					"Proceso Terminado!");
			//destruirListaGlobal();
			unmapearDataBinAMemoria();
			exit(1);
			break;
		case 0:
			loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
					"Conexion perdida con el FileSystem");
			close(fdDestino); //cerrar el descriptor
			loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
					"Proceso Terminado!");
			//destruirListaGlobal();
			unmapearDataBinAMemoria();
			exit(1);
			break;
		case 1:
			//guardar el nuevo nombre del nodo en el conf
			deserializarNombreNodo(block);
			break;
		case 2:
			// recibe bloque del nodo
			deserializarBloqueNodo(block);
			break;

		}
	}
}

void escucharConexiones(char* ip_destino, char* puerto_destino,
		char* puerto_local, int nombreNodo, int capacidadNodo) {

	struct addrinfo hints;
	struct addrinfo *serverInfo;
	struct sockaddr_in client_addr;

	pthread_t hiloFileSystem;
	pthread_t hiloTrabajo;

	fd_set master;

	int new_fd; //descriptor de fichero del cliente

	puerto_escucha = puerto_local;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;		// No importa si uso IPv4 o IPv6
	hints.ai_flags = AI_PASSIVE;// Asigna el address del localhost: 127.0.0.1
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, puerto_escucha, &hints, &serverInfo);

	int listenerSockfd;
	if ((listenerSockfd = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol)) == -1) {
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"Perror socket");
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		//destruirListaGlobal();
		unmapearDataBinAMemoria();
		exit(1);
	}

	int yes; // un inicador que lo utiliza el setsockpot
	if ((setsockopt(listenerSockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)))
			== -1) {
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"Perror setsockopt");
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		//destruirListaGlobal();
		unmapearDataBinAMemoria();
		exit(1);
	}

	if ((bind(listenerSockfd, serverInfo->ai_addr, serverInfo->ai_addrlen))
			== -1) {
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR, "Perror bind");
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		//destruirListaGlobal();
		unmapearDataBinAMemoria();
		exit(1);
	}
	freeaddrinfo(serverInfo);

	int sin_size;

	if ((listen(listenerSockfd, BACKLOG)) == -1) {
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"Perror listen");
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		//destruirListaGlobal();
		unmapearDataBinAMemoria();
		exit(1);
	}

	void* argumentos[4];

	argumentos[0] = ip_destino;
	argumentos[1] = puerto_destino;
	argumentos[2] = &nombreNodo;
	argumentos[3] = &capacidadNodo;

	pthread_create(&hiloFileSystem, NULL, conexionFileSystem,
			(void*) argumentos);

	FD_SET(listenerSockfd, &master); // agrega listenerSockfd al conjunto maestro

	while (1) {

		if (select(listenerSockfd + 1, &master, NULL, NULL, NULL) == -1) {

			loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
					"Perror select");
			break;
		}

		pthread_mutex_lock(&mutexConexiones);

		if ((new_fd = accept(listenerSockfd, (struct sockaddr *) &client_addr,
				&sin_size)) != -1) {

			pthread_create(&hiloTrabajo, NULL, trabajoNodo, (void*) &new_fd);

		} else {
			pthread_mutex_unlock(&mutexConexiones);
			break;
		}
	}

	close(listenerSockfd);
	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
			"Proceso Terminado!");

	unmapearDataBinAMemoria();

	exit(1);
}

void* trabajoNodo(void* new_fd) {

	int fd_Conexion = *(int*) new_fd;

	pthread_mutex_unlock(&mutexConexiones);

	int accion;

	bloque *block = malloc(sizeof(bloque));
	accion = receive_and_unpack(block, fd_Conexion); // recive el paquete y retorna una accion

	switch (accion) {
	case -2:
		realizarSolicitudBloqueArchivo(fd_Conexion, block);
		pthread_mutex_lock(&mutexConexiones);
		close(fd_Conexion);
		pthread_mutex_unlock(&mutexConexiones);
		break;
	case -1:
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"Error en el recv");
		pthread_mutex_lock(&mutexConexiones);
		close(fd_Conexion);
		pthread_mutex_unlock(&mutexConexiones);
		break;
	case 0:
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"Desconexion durante un pedido");
		pthread_mutex_lock(&mutexConexiones);
		close(fd_Conexion);
		pthread_mutex_unlock(&mutexConexiones); //cerrar el descriptor
		break;
	case 1:
		//map
		trabajarMap(fd_Conexion, block);
		pthread_mutex_lock(&mutexConexiones);
		close(fd_Conexion);
		pthread_mutex_unlock(&mutexConexiones);
		//LISTOOOO
		break;
	case 2:
		//reduce
		trabajarReduceSinCombiner(fd_Conexion, block);
		pthread_mutex_lock(&mutexConexiones);
		close(fd_Conexion);
		pthread_mutex_unlock(&mutexConexiones);
		break;
		//listooo
	case 3:
		//reduce
		trabajarReduceParcialConCombiner(fd_Conexion, block);
		pthread_mutex_lock(&mutexConexiones);
		close(fd_Conexion);
		pthread_mutex_unlock(&mutexConexiones);
		// LISTOOO
		break;
	case 4:
		//reduce
		trabajarReduceFinalConCombiner(fd_Conexion, block);
		pthread_mutex_lock(&mutexConexiones);
		close(fd_Conexion);
		pthread_mutex_unlock(&mutexConexiones);
		break;
	case 5:
		//pedido map
		realizarPedidoMap(fd_Conexion, block);
		pthread_mutex_lock(&mutexConexiones);
		close(fd_Conexion);
		pthread_mutex_unlock(&mutexConexiones);
		//LISTOOO SAPEE
		break;
	case 6:
		//pedido reduce parcial
		realizarPedidoReduceParciales(fd_Conexion, block);
		pthread_mutex_lock(&mutexConexiones);
		close(fd_Conexion);
		pthread_mutex_unlock(&mutexConexiones);
		//LISTOOO
		break;
	case 7:
		realizarPedidoOperacionJobFinalizada(fd_Conexion, block);
		pthread_mutex_lock(&mutexConexiones);
		close(fd_Conexion);
		pthread_mutex_unlock(&mutexConexiones);
		//LISTOOO
		break;
	}

	return 0;

}

void trabajarMap(int fd_job, bloque *block) {
	int resultado;
	bloque *bloqueAEnviar;

	trabajoMap* trabajo_Map = malloc(sizeof(trabajoMap));

	deserializarMapNodo(block, trabajo_Map);

	// mapea y retorna un valor de exito o fallo y se lo envia al job
	//ACA VA TODA MI MAGIA

	resultado = ejecutarMap(trabajo_Map);

	bloqueAEnviar = serializarResultadoMap(resultado);

	enviarDatos(fd_job, bloqueAEnviar);

}

void trabajarReduceSinCombiner(int fd_job, bloque *block) {

	bloque* bloqueAEnviar;
	int resultado;
	int fdNodoRemoto;
	int realizarTrabajo = 1;

	trabajoReduce* trabajo = malloc(sizeof(trabajoReduce));

	deserializarReduceSinCombinerNodo(block, trabajo);

	void establecerConexionConNodos(datoBloqueArchivo* nodo) {

		if (realizarTrabajo) {

			if (!strcmp(nodo->ipNodo, trabajo->ipLocal)
					&& !strcmp(nodo->puertoNodo, trabajo->puertoLocal)) {
				// me llamo a mi mismo, no tomo en cuenta mis maps
			} else {

				char* mensaje_conexion = malloc(
						strlen("Conexion con Nodo IP: ") + strlen(nodo->ipNodo)
								+ 1);
				strcpy(mensaje_conexion, "Conexion con Nodo IP: ");
				strcat(mensaje_conexion, nodo->ipNodo);
				loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO,
						mensaje_conexion);
				free(mensaje_conexion);

				pthread_mutex_lock(&mutexConexiones);
				fdNodoRemoto = crearConexion(nodo->ipNodo, nodo->puertoNodo);
				pthread_mutex_unlock(&mutexConexiones);

				if (!fdNodoRemoto) {
					realizarTrabajo = 0; // frenar trabajo
					resultado = 0;
					bloqueAEnviar = serializarResultadoReduceFinal(resultado);

					enviarDatos(fd_job, bloqueAEnviar);

				} else {
					bloqueAEnviar = serializarPedidoMapRealizado(
							trabajo->nombreJob, nodo->nroBloque);

					enviarDatos(fdNodoRemoto, bloqueAEnviar);

					bloque* bloqueARecibir = malloc(sizeof(bloque));
					if ((receive_and_unpack(bloqueARecibir, fdNodoRemoto))
							<= 0) {

						realizarTrabajo = 0; // frenar trabajo
						resultado = 0;
						bloqueAEnviar = serializarResultadoReduceFinal(
								resultado);

						enviarDatos(fd_job, bloqueAEnviar);
						pthread_mutex_lock(&mutexConexiones);
						close(fdNodoRemoto);
						pthread_mutex_unlock(&mutexConexiones);

					} else {
						bloque_mapeado* bloque_recibido =
								deserializarBloqueArchivoSolicitado(
										bloqueARecibir);
						guardarRecibido(bloque_recibido, trabajo->nombreJob);
						char* mensaje_desconexion = malloc(
								strlen("Desconexion con Nodo IP: ")
										+ strlen(nodo->ipNodo) + 1);
						strcpy(mensaje_desconexion,
								"Desconexion con Nodo IP: ");
						strcat(mensaje_desconexion, nodo->ipNodo);
						loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO,
								mensaje_desconexion);
						free(mensaje_desconexion);

						pthread_mutex_lock(&mutexConexiones);
						close(fdNodoRemoto);
						pthread_mutex_unlock(&mutexConexiones);
					}
				}

			}

		}
	}

	list_iterate(trabajo->datosBloqueArchivo,
			(void*) establecerConexionConNodos);

// llamar al que hace el reduce de los m

	if (realizarTrabajo) {
		resultado = hacerReduceTotalSinCombiner(trabajo->instruccionReduce,
				trabajo->nombreJob, trabajo->tamanioScriptReduce);
		bloqueAEnviar = serializarResultadoReduceFinal(resultado);

		enviarDatos(fd_job, bloqueAEnviar);


	}

	limpiarTrabajoReduce(trabajo);

}

void trabajarReduceParcialConCombiner(int fd_job, bloque *block) {

	bloque* bloqueAEnviar;
	int resultado;

	trabajoReduce* job_reduce = malloc(sizeof(trabajoReduce));

	deserializarReduceParcialConCombinerNodo(block, job_reduce);

	resultado = llamameParaEjecutarReduceLocal(job_reduce);

	bloqueAEnviar = serializarResultadoReduceParcialConCombiner(resultado);

	enviarDatos(fd_job, bloqueAEnviar);

	free(job_reduce->instruccionReduce);
	free(job_reduce);
}

void trabajarReduceFinalConCombiner(int fd_job, bloque *block) {

	bloque* bloqueAEnviar;
	int resultado;
	int fdNodoRemoto;
	int realizarTrabajo = 1;

	trabajoReduce* trabajo = malloc(sizeof(trabajoReduce));

	deserializarReduceFinal(block, trabajo);

	void establecerConexionConNodos(datoBloqueArchivo* nodo) {
		if (realizarTrabajo) {

			if (!strcmp(nodo->ipNodo, trabajo->ipLocal)
					&& !strcmp(nodo->puertoNodo, trabajo->puertoLocal)) {
				// me llamo a mi mismo, no tomo en cuenta mis maps
			} else {
				pthread_mutex_lock(&mutexConexiones);
				fdNodoRemoto = crearConexion(nodo->ipNodo, nodo->puertoNodo);
				pthread_mutex_unlock(&mutexConexiones);

				if (!fdNodoRemoto) {
					realizarTrabajo = 0; // frenar trabajo
					resultado = 0;
					bloqueAEnviar = serializarResultadoReduceFinal(resultado);

					enviarDatos(fd_job, bloqueAEnviar);

				} else {
					bloqueAEnviar = serializarPedidoReduceRealizado(
							trabajo->nombreJob);

					enviarDatos(fdNodoRemoto, bloqueAEnviar);

					bloque* bloqueARecibir = malloc(sizeof(bloque));
					if ((receive_and_unpack(bloqueARecibir, fdNodoRemoto))
							<= 0) {

						realizarTrabajo = 0; // frenar trabajo
						resultado = 0;
						bloqueAEnviar = serializarResultadoReduceFinal(
								resultado);

						enviarDatos(fd_job, bloqueAEnviar);

						pthread_mutex_lock(&mutexConexiones);
						close(fdNodoRemoto); // fijarse si sacarlo
						pthread_mutex_unlock(&mutexConexiones);

					} else {
						bloque_mapeado* bloque_recibido =
								deserializarBloqueArchivoSolicitado(
										bloqueARecibir);
						guardarRecibido(bloque_recibido, trabajo->nombreJob);

						pthread_mutex_lock(&mutexConexiones);
						close(fdNodoRemoto);
						pthread_mutex_unlock(&mutexConexiones);
					}
				}

			}

		}
	}

	list_iterate(trabajo->datosBloqueArchivo,
			(void*) establecerConexionConNodos);

	// llamar al que hace el reduce de los m

	if (realizarTrabajo) {
		resultado = hacerReduceTotalConCombiner(trabajo->instruccionReduce,
				trabajo->nombreJob, trabajo->tamanioScriptReduce);

		bloqueAEnviar = serializarResultadoReduceFinal(resultado);

		enviarDatos(fd_job, bloqueAEnviar);

	}
	limpiarTrabajoReduce(trabajo);

}

void realizarPedidoMap(int fd_Nodo, bloque* block) {

	pedidoMap* pedido_map = malloc(sizeof(pedidoMap));
	bloque* bloqueAEnviar;

	deserializarPedidoMapRealizado(block, pedido_map);

	bloque_mapeado* a_enviar = enviarMap(pedido_map->nombreJob,
			pedido_map->nroBloque);

	//serializar el bloque mapeado
	bloqueAEnviar = serializarBloqueArchivoSolicitado(a_enviar);

	enviarDatos(fd_Nodo, bloqueAEnviar);

}

void realizarPedidoReduceParciales(int fd_Nodo, bloque* block) {

	bloque* bloqueAEnviar;
	int nombreJob;

	nombreJob = deserializarPedidoReduceRealizado(block);

	bloque_mapeado * a_enviar = enviarReduceLocal(nombreJob);

	bloqueAEnviar = serializarBloqueArchivoSolicitado(a_enviar);

	enviarDatos(fd_Nodo, bloqueAEnviar);

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

void limpiarTrabajoReduce(trabajoReduce* recibido) {
	free(recibido->instruccionReduce);
	free(recibido->ipLocal);
	free(recibido->puertoLocal);

	void limpioListaTrabajoReduce(datoBloqueArchivo* a_liberar) {
		free(a_liberar->ipNodo);
		free(a_liberar->puertoNodo);
		free(a_liberar);
	}

	list_destroy_and_destroy_elements(recibido->datosBloqueArchivo,
			(void*) limpioListaTrabajoReduce);

	free(recibido);
}

void realizarSolicitudBloqueArchivo(int fd_filesystem, bloque* block) {

	bloque_mapeado* bloqueMapeado;
	bloque* bloqueAEnviar;

	bloqueMapeado = deserializarSolicitarBloqueArchivo(block);
	bloqueAEnviar = serializarBloqueArchivoSolicitado(bloqueMapeado);

	enviarDatos(fd_filesystem, bloqueAEnviar);
}

void realizarPedidoOperacionJobFinalizada(int fd_filesystem, bloque* block) {

	int nombreJob;
	bloque_mapeado* bloqueMapeado;
	bloque* bloqueAEnviar;

	nombreJob = deserializarPedidoOperacionJobFinalizada(block);

	bloqueMapeado = enviarResultadoFinal(nombreJob);

	bloqueAEnviar = serializarBloqueArchivoSolicitado(bloqueMapeado);

	enviarDatos(fd_filesystem, bloqueAEnviar);

}
