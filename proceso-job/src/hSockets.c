#include "hSockets.h"
#include "hSerializadores.h"

//variables globales -----------------------

pthread_mutex_t mutexConexionesMarta = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexFDConexiones = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexMap = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexRed = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
	instruccionMap* map;
	char* nombreArch;
} datosParaHiloMap;

datosParaHiloMap argumentosMap;

typedef struct {
	instruccionReduce* reduce;
	char* nombreArch;
} datosParaHiloReduce;

datosParaHiloReduce argumentosReduce;

fd_set master;
int fdmax; //cantidad maxima de descriptores de fichero

conexionMarta* marta;
script *datos_script;
int nombreJob;
int tipoCombiner;
char* puerto_escucha;
char* resultado_operacion;

// ------------------------------------------

void establecerConexionConMarta(conexionMarta* conexion_Marta) {

	int fd_conexionRemota;
	char* msg_log;

	if ((fd_conexionRemota = crearConexion(conexion_Marta->ip_Marta,
			conexion_Marta->puerto_Principal))) {

		FD_SET(fd_conexionRemota, &master); // agrega listenerSockfd al conjunto maestro

		if (fd_conexionRemota > fdmax) {
			fdmax = fd_conexionRemota;	// actualizar el máximo
		}

		msg_log = malloc(
				strlen("Conexion con MaRTA establecida - IP: ") + 1 + 16
						+ strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "Conexion con MaRTA establecida - ip: ");
		strcat(msg_log, conexion_Marta->ip_Marta);
		strcat(msg_log, " puerto: ");
		strcat(msg_log, conexion_Marta->puerto_Principal);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO, msg_log);

		free(msg_log);

	} else {

		msg_log = malloc(
				strlen("No se puede establecer conexion con MaRTA - IP: ") + 1
						+ 16 + strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "No se puede establecer conexion con MaRTA - IP: ");
		strcat(msg_log, conexion_Marta->ip_Marta);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, conexion_Marta->puerto_Principal);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR, msg_log);

		free(msg_log);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		exit(1);
	}

}

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

int obtenerDescriptorDeFichero(char* ip_destino, char* puerto_destino) {
	struct sockaddr_in socketAddr;  // direccion ip del socket
	int socketAddr_len;

	int fd; // descriptor de fichero de la ip_destinio
	int fd_buscado = -1;

	socketAddr_len = sizeof(socketAddr);
	for (fd = 0; fd <= fdmax; fd++) {
		if (FD_ISSET(fd, &master)) {
			getpeername(fd, (struct sockaddr *) &socketAddr, &socketAddr_len);

			if (!strcmp((inet_ntoa(socketAddr.sin_addr)), ip_destino)
					&& !strcmp(string_itoa(ntohs(socketAddr.sin_port)),
							puerto_destino)) {
				fd_buscado = fd;
				break;
			}
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
			package_enviado = 0;
			break;
		}

		total += bytesEnviados;
		bytesPendientes -= bytesEnviados;

	}

	return package_enviado;

}

int enviarDatos(char* ip_destino, char* puerto_destino, bloque *block) {

	int datosEnviados = 0;
	paquete *package;

	int fd_ip_destino;

	if ((fd_ip_destino = obtenerDescriptorDeFichero(ip_destino, puerto_destino))
			== -1) {
		return datosEnviados;
	}

	package = pack(block);

	datosEnviados = sendAll(fd_ip_destino, package);

	free(package);

	return datosEnviados;

}

int enviarDatosNodo(int fd_destino, bloque *block) {

	int datosEnviados = 0;
	paquete *package;

	package = pack(block);

	datosEnviados = sendAll(fd_destino, package);

	free(package->bloque);
	free(package);

	return datosEnviados;

}

void establecerConexiones(char* puertoEscucha, conexionMarta* conexion_Marta,
		char** direccionesArchivo, int combiner, script *mapper_reducer,
		int nombre_Job, char* resultadoOperacion) {

	int accion; //accion a tomar con el paquete recibido
	int i; // iterador
	char* msg_log;

	tipoCombiner = combiner;
	puerto_escucha = puertoEscucha;
	resultado_operacion = resultadoOperacion;

	bloque* bloqueAEnviar;

	// para manejo del select
	FD_ZERO(&master);// borra los conjuntos maestro y temporal
	fdmax = 0;

	nombreJob = nombre_Job;
	datos_script = mapper_reducer;

	establecerConexionConMarta(conexion_Marta); // se conecta con Marta

	marta = conexion_Marta; // lo guarda en una variable global

	//enviar solicitud a marta
	bloqueAEnviar = serializarPedirTrabajoDeArchivoAMarta(direccionesArchivo,
			combiner);
	if (!enviarDatos(conexion_Marta->ip_Marta, conexion_Marta->puerto_Principal,
			bloqueAEnviar)) {

		msg_log = malloc(
				strlen("MaRTA no se encuentra disponible - IP: ") + 1 + 16
						+ strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "MaRTA no se encuentra disponible - IP: ");
		strcat(msg_log, conexion_Marta->ip_Marta);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, conexion_Marta->puerto_Principal);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR, msg_log);

		free(msg_log);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
				"Proceso Terminado!");
		exit(1);
	}

	while (1) {

		if (select(fdmax + 1, &master, NULL, NULL, NULL) == -1) {

			loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR, "select");

			loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
					"Proceso Terminado!");
			exit(1);
		}

		for (i = 0; i <= fdmax; i++) { // explorar conexiones existentes en busca de datos que leer

			if (FD_ISSET(i, &master)) {	// comprueba si hay datos

				bloque *block = malloc(sizeof(bloque));
				falloNodo* fallo_nodo;
				char* archivo_buscado;

				accion = receive_and_unpack(block, i); // recive el paquete y retorna una accion

				char* accion_log = string_itoa(accion);

				msg_log = malloc(
						strlen("Mensaje Recibido: ") + 1 + strlen("Accion - ")
								+ 1 + 4 + strlen(" -> MaRTA - ") + 1
								+ strlen("IP: ") + 1 + 16 + strlen(" PUERTO: ")
								+ 1 + 6);

				strcpy(msg_log, "Mensaje Recibido: ");
				strcat(msg_log, "Accion - ");
				strcat(msg_log, accion_log);
				strcat(msg_log, " -> MaRTA - ");
				strcat(msg_log, "IP: ");
				strcat(msg_log, marta->ip_Marta);
				strcat(msg_log, " PUERTO: ");
				strcat(msg_log, marta->puerto_Principal);

				loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

				free(msg_log);
				free(accion_log);

				switch (accion) {
				case -1:
					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"recv principal");

					close(i); //cerrar el descriptor
					FD_CLR(i, &master); // eliminar del conjunto maestro

					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"Proceso Terminado!");
					exit(1);
					break;
				case 0:
					msg_log = malloc(
							strlen("MaRTA no se encuentra disponible - IP: ")
									+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

					strcpy(msg_log, "MaRTA no se encuentra disponible - IP: ");
					strcat(msg_log, conexion_Marta->ip_Marta);
					strcat(msg_log, " puerto: ");
					strcat(msg_log, conexion_Marta->puerto_Principal);

					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							msg_log);

					free(msg_log);

					close(i); //cerrar el descriptor
					FD_CLR(i, &master); // eliminar del conjunto maestro
					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"Proceso Terminado!");
					exit(1);
					break;
				case 1:
					crearHilosMaper(block);
					break;
				case 2:
					crearHiloMaperReplan(block);
					break;
				case 3:
					crearHiloReducerSinCombiner(block);
					break;
				case 4:
					crearHiloReducerParcialConCombiner(block);
					break;
				case 5:
					crearHiloReducerFinalConCombiner(block);
					break;
				case 6:
					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO,
							"Operacion realizada exitosamente");

					close(i); //cerrar el descriptor
					FD_CLR(i, &master); // eliminar del conjunto maestro
					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO,
							"Proceso Terminado!");
					exit(1);
					break;
				case 7:
					archivo_buscado = deserializarNoExisteArchivoPedido(block);
					msg_log = malloc(
							strlen("No se encuentra el archivo ") + 1
									+ strlen(archivo_buscado) + 1
									+ strlen(" en FilesSystem") + 1);

					strcpy(msg_log, "No se encuentra el archivo ");
					strcat(msg_log, archivo_buscado);
					strcat(msg_log, " en FilesSystem");

					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							msg_log);

					free(msg_log);

					close(i); //cerrar el descriptor
					FD_CLR(i, &master); // eliminar del conjunto maestro
					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"Proceso Terminado!");
					exit(1);
					break;
				case 8:
					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"FileSystem no esta disponible: Cantidad de nodos conectados insuficientes");

					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"Proceso Terminado!");
					exit(1);
					break;
				case 9:
					// fallo de reduce parcial
					fallo_nodo = deserializarFalloOperacion(block);
					msg_log = malloc(
							strlen("Fallo de Reduce Parcial en nodo - IP: ") + 1
									+ 16 + strlen(" PUERTO: ") + 1 + 6);

					strcpy(msg_log, "Fallo de Reduce Parcial en nodo - IP: ");
					strcat(msg_log, fallo_nodo->ipNodo);
					strcat(msg_log, " PUERTO: ");
					strcat(msg_log, fallo_nodo->puertoNodo);

					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							msg_log);

					free(msg_log);

					close(i); //cerrar el descriptor
					FD_CLR(i, &master); // eliminar del conjunto maestro

					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"Proceso Terminado!");
					exit(1);
					break;
				case 10:
					// fallo de operacion
					fallo_nodo = deserializarFalloOperacion(block);
					msg_log = malloc(
							strlen("Fallo de Reduce Final en nodo - IP: ") + 1
									+ 16 + strlen(" PUERTO: ") + 1 + 6);

					strcpy(msg_log, "Fallo de Reduce Final en nodo - IP: ");
					strcat(msg_log, fallo_nodo->ipNodo);
					strcat(msg_log, " PUERTO: ");
					strcat(msg_log, fallo_nodo->puertoNodo);

					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							msg_log);

					free(msg_log);

					close(i); //cerrar el descriptor
					FD_CLR(i, &master); // eliminar del conjunto maestro

					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"Proceso Terminado!");
					exit(1);
					break;
				case 11:
					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"No se pudo hallar una replanificacion Map posible");

					close(i); //cerrar el descriptor
					FD_CLR(i, &master); // eliminar del conjunto maestro

					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"Proceso Terminado!");
					exit(1);
					break;
				case 12:
					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"FileSystem no esta disponible: No se encuentra conectado");

					loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
							"Proceso Terminado!");
					exit(1);
					break;
				}
			}
		}

	}
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

void crearHilosMaper(bloque* block) {

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_TRACE, "Creando hilos Map");

	pthread_t hiloMaper;

	t_list* listaBloquesNodo = list_create();

	char* archivoTrabajo = deserializarMapJob(block, listaBloquesNodo);

	while (list_size(listaBloquesNodo)) {

		t_list* lBloquesNodoAux = list_take_and_remove(listaBloquesNodo, 1);
		instruccionMap* nodoBloque = list_get(lBloquesNodoAux, 0);

		pthread_mutex_lock(&mutexMap);

		argumentosMap.map = nodoBloque;
		argumentosMap.nombreArch = archivoTrabajo;

		pthread_create(&hiloMaper, NULL, hiloParaMapear, NULL);
	}

}

void crearHiloMaperReplan(bloque* block) {

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_TRACE,
			"Creando hilo replanificacion Map");

	pthread_t hiloMaper;

	instruccionMap* instruccion = malloc(sizeof(instruccionMap));

	char* archivoTrabajo = deserializarMapJobReplan(block, instruccion);

	pthread_mutex_lock(&mutexMap);

	argumentosMap.map = instruccion;
	argumentosMap.nombreArch = archivoTrabajo;

	pthread_create(&hiloMaper, NULL, hiloParaMapear, NULL);

}

void crearHiloReducerSinCombiner(bloque* block) {

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_TRACE,
			"Creando hilo Reduce Final Sin Combiner");

	pthread_t hiloReduce;

	instruccionReduce* instrucciones_Reduce = malloc(sizeof(instruccionReduce));
	char* archivoTrabajo = deserializarReduceJobSinCombiner(block,
			instrucciones_Reduce);

	pthread_mutex_lock(&mutexRed);

	argumentosReduce.reduce = instrucciones_Reduce;
	argumentosReduce.nombreArch = archivoTrabajo;

	pthread_create(&hiloReduce, NULL, hiloParaReduceSinCombiner, NULL);

}

void crearHiloReducerParcialConCombiner(bloque* block) {

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_TRACE,
			"Creando hilo Reduce Parcial Con Combiner");

	pthread_t hiloReduce;

	instruccionReduce* instrucciones_Reduce = malloc(sizeof(instruccionReduce));
	char* archivoTrabajo = deserializarReduceParcialJobConCombiner(block,
			instrucciones_Reduce);

	pthread_mutex_lock(&mutexRed);

	argumentosReduce.reduce = instrucciones_Reduce;
	argumentosReduce.nombreArch = archivoTrabajo;

	pthread_create(&hiloReduce, NULL, hiloParaReduceParcialConCombiner,
	NULL);

}

void crearHiloReducerFinalConCombiner(bloque* block) {

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_TRACE,
			"Creando hilo Reduce Final Con Combiner");

	pthread_t hiloReduce;

	instruccionReduce* instrucciones_Reduce = malloc(sizeof(instruccionReduce));
	char* archivoTrabajo = deserializarReduceFinalJobConCombiner(block,
			instrucciones_Reduce);

	pthread_mutex_lock(&mutexRed);

	argumentosReduce.reduce = instrucciones_Reduce;
	argumentosReduce.nombreArch = archivoTrabajo;

	pthread_create(&hiloReduce, NULL, hiloParaReduceFinalConCombiner,
	NULL);

}

void* hiloParaReduceSinCombiner(void* foo) {
	rutinaReduceSinCombiner();

	return 0;
}

void* hiloParaReduceParcialConCombiner(void* foo) {
	rutinaReduceParcialConCombiner();

	return 0;
}

void* hiloParaReduceFinalConCombiner(void* foo) {
	rutinaReduceFinalConCombiner();

	return 0;
}

void* hiloParaMapear(void* foo) {
	rutinaMapper();

	return 0;
}

void rutinaReduceSinCombiner() {

	instruccionReduce* nodoBloque = malloc(sizeof(instruccionReduce));
	nodoBloque->ipNodoPrincipal = malloc(16);
	nodoBloque->puertoNodoPrincipal = malloc(6);
	t_list* listaRed = list_create();

	strcpy(nodoBloque->ipNodoPrincipal,
			argumentosReduce.reduce->ipNodoPrincipal);
	strcpy(nodoBloque->puertoNodoPrincipal,
			argumentosReduce.reduce->puertoNodoPrincipal);
	list_add_all(listaRed, argumentosReduce.reduce->instruccionesReduce);
	nodoBloque->instruccionesReduce = listaRed;

	char* archivoTrabajo = malloc(strlen(argumentosReduce.nombreArch) + 1);
	strcpy(archivoTrabajo, argumentosReduce.nombreArch);

	pthread_mutex_unlock(&mutexRed);

	bloque *block = malloc(sizeof(bloque)); //para recibir
	bloque* bloqueAEnviar;
	char* accion_log;
	char* msg_log;
	int accion;
	int nodoSocket_fd;

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO,
			"Hilo Reduce Final Sin Combiner");

	msg_log = malloc(
			strlen("Nodo Principal - ") + 1 + strlen("IP: ") + 1 + 16
					+ strlen(" PUERTO: ") + 1 + 6);

	strcpy(msg_log, "Nodo Principal - ");
	strcat(msg_log, "IP: ");
	strcat(msg_log, nodoBloque->ipNodoPrincipal);
	strcat(msg_log, " PUERTO: ");
	strcat(msg_log, nodoBloque->puertoNodoPrincipal);

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO, msg_log);
	free(msg_log);

	void loguearNodosSecundarios(datosInstruccionReduce* instruccion) {

		char* bloqueNodo = string_itoa(instruccion->bloqueNodo);

		msg_log = malloc(
				strlen("Nodo Secundario - ") + 1 + strlen("IP: ") + 1 + 16
						+ strlen(" PUERTO: ") + 1 + 6 + strlen(" BLOQUE: ") + 1
						+ 4);

		strcpy(msg_log, "Nodo Secundario - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, instruccion->ipNodo);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, instruccion->puertoNodo);
		strcat(msg_log, " BLOQUE: ");
		strcat(msg_log, bloqueNodo);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO, msg_log);
		free(msg_log);
		free(bloqueNodo);
	}

	list_iterate(nodoBloque->instruccionesReduce,
			(void*) loguearNodosSecundarios);

	pthread_mutex_lock(&mutexFDConexiones);

	nodoSocket_fd = crearConexion(nodoBloque->ipNodoPrincipal,
			nodoBloque->puertoNodoPrincipal);

	pthread_mutex_unlock(&mutexFDConexiones);

	if (nodoSocket_fd) {

		bloqueAEnviar = serializarReduceSinCombinerNodo(datos_script,
				nodoBloque, nombreJob);

		enviarDatosNodo(nodoSocket_fd, bloqueAEnviar);

		char* accion_log = string_itoa(2);

		msg_log = malloc(
				strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1 + 4
						+ strlen(" -> Nodo Principal - ") + 1 + strlen("IP: ")
						+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "Mensaje Enviado: ");
		strcat(msg_log, "Accion - ");
		strcat(msg_log, accion_log);
		strcat(msg_log, " -> Nodo Principal - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, nodoBloque->ipNodoPrincipal);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, nodoBloque->puertoNodoPrincipal);

		loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

		free(msg_log);
		free(accion_log);

		if ((accion = receive_and_unpack(block, nodoSocket_fd)) > 0) {

			accion_log = string_itoa(accion);

			msg_log = malloc(
					strlen("Mensaje Recibido: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> Nodo Principal - ") + 1
							+ strlen("IP: ") + 1 + 16 + strlen(" PUERTO: ") + 1
							+ 6);

			strcpy(msg_log, "Mensaje Recibido: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> Nodo Principal - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, nodoBloque->ipNodoPrincipal);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, nodoBloque->puertoNodoPrincipal);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			pthread_mutex_lock(&mutexConexionesMarta);

			bloqueAEnviar = serializarResultadoReduceExitoso(nombreJob,
					resultado_operacion, nodoBloque->ipNodoPrincipal,
					nodoBloque->puertoNodoPrincipal);

			enviarDatos(marta->ip_Marta, marta->puerto_Principal,
					bloqueAEnviar);

			accion_log = string_itoa(5);

			msg_log = malloc(
					strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> MaRTA - ") + 1 + strlen("IP: ")
							+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log, "Mensaje Enviado: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> MaRTA - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, marta->ip_Marta);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, marta->puerto_Principal);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			pthread_mutex_unlock(&mutexConexionesMarta);

			msg_log =
					malloc(
							strlen(
									"Hilo Reduce Final Sin Combiner terminado exitosamente")
									+ 1 + strlen(" -> Nodo Principal - ") + 1
									+ strlen("IP: ") + 1 + 16
									+ strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log,
					"Hilo Reduce Final Sin Combiner terminado exitosamente");
			strcat(msg_log, " -> Nodo Principal - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, nodoBloque->ipNodoPrincipal);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, nodoBloque->puertoNodoPrincipal);

			loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_TRACE, msg_log);

			free(msg_log);

		} else {
			pthread_mutex_lock(&mutexConexionesMarta);

			bloqueAEnviar = serializarResultadoReduceSinCombinerFallo(
					nodoBloque->ipNodoPrincipal,
					nodoBloque->puertoNodoPrincipal);
			enviarDatos(marta->ip_Marta, marta->puerto_Principal,
					bloqueAEnviar);

			accion_log = string_itoa(7);

			msg_log = malloc(
					strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> MaRTA - ") + 1 + strlen("IP: ")
							+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log, "Mensaje Enviado: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> MaRTA - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, marta->ip_Marta);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, marta->puerto_Principal);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			pthread_mutex_unlock(&mutexConexionesMarta);

			loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
					"Terminado hilo Reduce Final Sin Combiner -> No se pudo finalizar la operacion");

		}
	} else {
		pthread_mutex_lock(&mutexConexionesMarta);

		bloqueAEnviar = serializarResultadoReduceSinCombinerFallo(
				nodoBloque->ipNodoPrincipal, nodoBloque->puertoNodoPrincipal);
		enviarDatos(marta->ip_Marta, marta->puerto_Principal, bloqueAEnviar);

		accion_log = string_itoa(7);

		msg_log = malloc(
				strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1 + 4
						+ strlen(" -> MaRTA - ") + 1 + strlen("IP: ") + 1 + 16
						+ strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "Mensaje Enviado: ");
		strcat(msg_log, "Accion - ");
		strcat(msg_log, accion_log);
		strcat(msg_log, " -> MaRTA - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, marta->ip_Marta);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, marta->puerto_Principal);

		loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

		free(msg_log);
		free(accion_log);

		pthread_mutex_unlock(&mutexConexionesMarta);

		msg_log = malloc(
				strlen("Terminado hilo Reduce Final Sin Combiner") + 1
						+ strlen(" -> Nodo principal no disponible - ") + 1
						+ strlen("IP: ") + 1 + 16 + strlen(" PUERTO: ") + 1
						+ 6);

		strcpy(msg_log, "Terminado hilo Reduce Final Sin Combiner");
		strcat(msg_log, " -> Nodo principal no disponible - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, nodoBloque->ipNodoPrincipal);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, nodoBloque->puertoNodoPrincipal);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR, msg_log);

		free(msg_log);
	}

	free(nodoBloque->ipNodoPrincipal);
	free(nodoBloque->puertoNodoPrincipal);

	void destruirListaReduceSinCombiner(
			datosInstruccionReduce* datos_instruccion) {
		free(datos_instruccion->ipNodo);
		free(datos_instruccion->puertoNodo);
		free(datos_instruccion);

	}

	list_destroy_and_destroy_elements(nodoBloque->instruccionesReduce,
			(void*) destruirListaReduceSinCombiner);

	free(nodoBloque);

	//close(nodoSocket_fd);

}

void rutinaReduceParcialConCombiner() {

	instruccionReduce* nodoBloque = malloc(sizeof(instruccionReduce));
	nodoBloque->ipNodoPrincipal = malloc(16);
	nodoBloque->puertoNodoPrincipal = malloc(6);
	t_list* listaRed = list_create();

	strcpy(nodoBloque->ipNodoPrincipal,
			argumentosReduce.reduce->ipNodoPrincipal);
	strcpy(nodoBloque->puertoNodoPrincipal,
			argumentosReduce.reduce->puertoNodoPrincipal);
	list_add_all(listaRed, argumentosReduce.reduce->instruccionesReduce);
	nodoBloque->instruccionesReduce = listaRed;

	char* archivoTrabajo = malloc(strlen(argumentosReduce.nombreArch) + 1);
	strcpy(archivoTrabajo, argumentosReduce.nombreArch);

	pthread_mutex_unlock(&mutexRed);

	bloque *block = malloc(sizeof(bloque)); //para recibir
	bloque* bloqueAEnviar;
	char* accion_log;
	char* msg_log;
	int accion;
	int nodoSocket_fd;
	int resultado;

	pthread_mutex_lock(&mutexFDConexiones);

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO,
			"Hilo Reduce Parcial Con Combiner");

	msg_log = malloc(
			strlen("Nodo - ") + 1 + strlen("IP: ") + 1 + 16
					+ strlen(" PUERTO: ") + 1 + 6);

	strcpy(msg_log, "Nodo - ");
	strcat(msg_log, "IP: ");
	strcat(msg_log, nodoBloque->ipNodoPrincipal);
	strcat(msg_log, " PUERTO: ");
	strcat(msg_log, nodoBloque->puertoNodoPrincipal);

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO, msg_log);
	free(msg_log);

	void loguearBloques(datosInstruccionReduce* instruccion) {

		char* bloqueNodo = string_itoa(instruccion->bloqueNodo);

		msg_log = malloc((strlen("BLOQUE: ") + 1 + 4));

		strcpy(msg_log, "BLOQUE: ");
		strcat(msg_log, bloqueNodo);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO, msg_log);
		free(msg_log);
		free(bloqueNodo);
	}

	list_iterate(nodoBloque->instruccionesReduce, (void*) loguearBloques);


	nodoSocket_fd = crearConexion(nodoBloque->ipNodoPrincipal,
			nodoBloque->puertoNodoPrincipal);

	pthread_mutex_unlock(&mutexFDConexiones);

	if (nodoSocket_fd) {

		bloqueAEnviar = serializarReduceParcialConCombinerNodo(datos_script,
				nodoBloque, nombreJob);

		enviarDatosNodo(nodoSocket_fd, bloqueAEnviar);

		accion_log = string_itoa(3);

		msg_log = malloc(
				strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1 + 4
						+ strlen(" -> Nodo Principal - ") + 1 + strlen("IP: ")
						+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "Mensaje Enviado: ");
		strcat(msg_log, "Accion - ");
		strcat(msg_log, accion_log);
		strcat(msg_log, " -> Nodo Principal - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, nodoBloque->ipNodoPrincipal);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, nodoBloque->puertoNodoPrincipal);

		loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

		free(msg_log);
		free(accion_log);

		if ((accion = receive_and_unpack(block, nodoSocket_fd)) > 0) {

			accion_log = string_itoa(accion);

			msg_log = malloc(
					strlen("Mensaje Recibido: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> Nodo - ") + 1 + strlen("IP: ") + 1
							+ 16 + strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log, "Mensaje Recibido: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> Nodo - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, nodoBloque->ipNodoPrincipal);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, nodoBloque->puertoNodoPrincipal);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			resultado = deserializarResultadoReduceParcialConCombiner(block);

			pthread_mutex_lock(&mutexConexionesMarta);

			bloqueAEnviar = serializarResultadoReduceParcial(resultado,
					nodoBloque);
			enviarDatos(marta->ip_Marta, marta->puerto_Principal,
					bloqueAEnviar);

			accion_log = string_itoa(4);

			msg_log = malloc(
					strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> MaRTA - ") + 1 + strlen("IP: ")
							+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log, "Mensaje Enviado: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> MaRTA - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, marta->ip_Marta);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, marta->puerto_Principal);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			pthread_mutex_unlock(&mutexConexionesMarta);

			msg_log =
					malloc(
							strlen(
									"Hilo Reduce Final Con Combiner terminado exitosamente")
									+ 1 + strlen(" -> Nodo Principal - ") + 1
									+ strlen("IP: ") + 1 + 16
									+ strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log,
					"Hilo Reduce Parcial Con Combiner terminado exitosamente");
			strcat(msg_log, " -> Nodo Principal - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, nodoBloque->ipNodoPrincipal);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, nodoBloque->puertoNodoPrincipal);

			loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_TRACE, msg_log);

			free(msg_log);

		} else {

			pthread_mutex_lock(&mutexConexionesMarta);
			resultado = 0;

			bloqueAEnviar = serializarResultadoReduceParcial(resultado,
					nodoBloque);
			enviarDatos(marta->ip_Marta, marta->puerto_Principal,
					bloqueAEnviar);

			accion_log = string_itoa(4);

			msg_log = malloc(
					strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> MaRTA - ") + 1 + strlen("IP: ")
							+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log, "Mensaje Enviado: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> MaRTA - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, marta->ip_Marta);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, marta->puerto_Principal);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			pthread_mutex_unlock(&mutexConexionesMarta);

			loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
					"Terminado hilo Reduce Parcial Con Combiner -> No se pudo finalizar la operacion");
		}
	} else {
		pthread_mutex_lock(&mutexConexionesMarta);
		resultado = 0;

		bloqueAEnviar = serializarResultadoReduceParcial(resultado, nodoBloque);
		enviarDatos(marta->ip_Marta, marta->puerto_Principal, bloqueAEnviar);

		accion_log = string_itoa(4);

		msg_log = malloc(
				strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1 + 4
						+ strlen(" -> MaRTA - ") + 1 + strlen("IP: ") + 1 + 16
						+ strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "Mensaje Enviado: ");
		strcat(msg_log, "Accion - ");
		strcat(msg_log, accion_log);
		strcat(msg_log, " -> MaRTA - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, marta->ip_Marta);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, marta->puerto_Principal);

		loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

		free(msg_log);
		free(accion_log);

		pthread_mutex_unlock(&mutexConexionesMarta);

		msg_log = malloc(
				strlen("Terminado hilo Reduce Parcial Con Combiner") + 1
						+ strlen(" -> Nodo principal no disponible - ") + 1
						+ strlen("IP: ") + 1 + 16 + strlen(" PUERTO: ") + 1
						+ 6);

		strcpy(msg_log, "Terminado hilo Reduce Parcial Con Combiner");
		strcat(msg_log, " -> Nodo principal no disponible - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, nodoBloque->ipNodoPrincipal);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, nodoBloque->puertoNodoPrincipal);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR, msg_log);

		free(msg_log);
	}

	free(nodoBloque->ipNodoPrincipal);
	free(nodoBloque->puertoNodoPrincipal);

	void destruirListaReduceParcialConCombiner(
			datosInstruccionReduce* datos_instruccion) {

		free(datos_instruccion);
	}

	list_destroy_and_destroy_elements(nodoBloque->instruccionesReduce,
			(void*) destruirListaReduceParcialConCombiner);

	free(nodoBloque);

	//close(nodoSocket_fd);

}

void rutinaReduceFinalConCombiner() {

	instruccionReduce* nodoBloque = malloc(sizeof(instruccionReduce));
	nodoBloque->ipNodoPrincipal = malloc(16);
	nodoBloque->puertoNodoPrincipal = malloc(6);
	t_list* listaRed = list_create();

	strcpy(nodoBloque->ipNodoPrincipal,
			argumentosReduce.reduce->ipNodoPrincipal);
	strcpy(nodoBloque->puertoNodoPrincipal,
			argumentosReduce.reduce->puertoNodoPrincipal);
	list_add_all(listaRed, argumentosReduce.reduce->instruccionesReduce);
	nodoBloque->instruccionesReduce = listaRed;

	char* archivoTrabajo = malloc(strlen(argumentosReduce.nombreArch) + 1);
	strcpy(archivoTrabajo, argumentosReduce.nombreArch);

	pthread_mutex_unlock(&mutexRed);

	bloque *block = malloc(sizeof(bloque)); //para recibir
	bloque* bloqueAEnviar;
	char* accion_log;
	char* msg_log;
	int accion;
	int nodoSocket_fd;

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO,
			"Hilo Reduce Final Con Combiner");

	msg_log = malloc(
			strlen("Nodo Principal - ") + 1 + strlen("IP: ") + 1 + 16
					+ strlen(" PUERTO: ") + 1 + 6);

	strcpy(msg_log, "Nodo Principal - ");
	strcat(msg_log, "IP: ");
	strcat(msg_log, nodoBloque->ipNodoPrincipal);
	strcat(msg_log, " PUERTO: ");
	strcat(msg_log, nodoBloque->puertoNodoPrincipal);

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO, msg_log);
	free(msg_log);

	void loguearNodosSecundarios(reducesParcialesParaJob* instruccion) {

		msg_log = malloc(
				strlen("Nodo Secundario - ") + 1 + strlen("IP: ") + 1 + 16
						+ strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "Nodo Secundario - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, instruccion->ipNodo);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, instruccion->puertoNodo);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO, msg_log);
		free(msg_log);
	}

	list_iterate(nodoBloque->instruccionesReduce,
			(void*) loguearNodosSecundarios);

	pthread_mutex_lock(&mutexFDConexiones);

	nodoSocket_fd = crearConexion(nodoBloque->ipNodoPrincipal,
			nodoBloque->puertoNodoPrincipal);

	pthread_mutex_unlock(&mutexFDConexiones);

	if (nodoSocket_fd) {

		bloqueAEnviar = serializarReduceFinalConCombinerNodo(datos_script,
				nodoBloque, nombreJob);

		enviarDatosNodo(nodoSocket_fd, bloqueAEnviar);

		accion_log = string_itoa(4);

		msg_log = malloc(
				strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1 + 4
						+ strlen(" -> Nodo Principal - ") + 1 + strlen("IP: ")
						+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "Mensaje Enviado: ");
		strcat(msg_log, "Accion - ");
		strcat(msg_log, accion_log);
		strcat(msg_log, " -> Nodo Principal - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, nodoBloque->ipNodoPrincipal);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, nodoBloque->puertoNodoPrincipal);

		loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

		free(msg_log);
		free(accion_log);

		if ((accion = receive_and_unpack(block, nodoSocket_fd)) > 0) {

			accion_log = string_itoa(accion);

			msg_log = malloc(
					strlen("Mensaje Recibido: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> Nodo Principal - ") + 1
							+ strlen("IP: ") + 1 + 16 + strlen(" PUERTO: ") + 1
							+ 6);

			strcpy(msg_log, "Mensaje Recibido: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> Nodo Principal - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, nodoBloque->ipNodoPrincipal);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, nodoBloque->puertoNodoPrincipal);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			// reduce aplicado con exito
			pthread_mutex_lock(&mutexConexionesMarta);
			//avisar marta resultado de operacion reduce
			bloqueAEnviar = serializarResultadoReduceExitoso(nombreJob,
					resultado_operacion, nodoBloque->ipNodoPrincipal,
					nodoBloque->puertoNodoPrincipal);

			enviarDatos(marta->ip_Marta, marta->puerto_Principal,
					bloqueAEnviar);

			accion_log = string_itoa(5);

			msg_log = malloc(
					strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> MaRTA - ") + 1 + strlen("IP: ")
							+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log, "Mensaje Enviado: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> MaRTA - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, marta->ip_Marta);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, marta->puerto_Principal);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			pthread_mutex_unlock(&mutexConexionesMarta);

			msg_log =
					malloc(
							strlen(
									"Hilo Reduce Final Con Combiner terminado exitosamente")
									+ 1 + strlen(" -> Nodo Principal - ") + 1
									+ strlen("IP: ") + 1 + 16
									+ strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log,
					"Hilo Reduce Final Con Combiner terminado exitosamente");
			strcat(msg_log, " -> Nodo Principal - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, nodoBloque->ipNodoPrincipal);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, nodoBloque->puertoNodoPrincipal);

			loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_TRACE, msg_log);

			free(msg_log);
		} else {

			pthread_mutex_lock(&mutexConexionesMarta);

			bloqueAEnviar = serializarResultadoReduceConCombinerFallo(
					nodoBloque->ipNodoPrincipal,
					nodoBloque->puertoNodoPrincipal);
			enviarDatos(marta->ip_Marta, marta->puerto_Principal,
					bloqueAEnviar);

			accion_log = string_itoa(8);

			msg_log = malloc(
					strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> MaRTA - ") + 1 + strlen("IP: ")
							+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log, "Mensaje Enviado: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> MaRTA - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, marta->ip_Marta);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, marta->puerto_Principal);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			pthread_mutex_unlock(&mutexConexionesMarta);

			loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR,
					"Terminado hilo Reduce Final Con Combiner -> No se pudo finalizar la operacion");

		}

	} else {
		pthread_mutex_lock(&mutexConexionesMarta);

		bloqueAEnviar = serializarResultadoReduceConCombinerFallo(
				nodoBloque->ipNodoPrincipal, nodoBloque->puertoNodoPrincipal);
		enviarDatos(marta->ip_Marta, marta->puerto_Principal, bloqueAEnviar);

		accion_log = string_itoa(8);

		msg_log = malloc(
				strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1 + 4
						+ strlen(" -> MaRTA - ") + 1 + strlen("IP: ") + 1 + 16
						+ strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "Mensaje Enviado: ");
		strcat(msg_log, "Accion - ");
		strcat(msg_log, accion_log);
		strcat(msg_log, " -> MaRTA - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, marta->ip_Marta);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, marta->puerto_Principal);

		loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

		free(msg_log);
		free(accion_log);

		pthread_mutex_unlock(&mutexConexionesMarta);

		msg_log = malloc(
				strlen("Terminado hilo Reduce Final Con Combiner") + 1
						+ strlen(" -> Nodo principal no disponible - ") + 1
						+ strlen("IP: ") + 1 + 16 + strlen(" PUERTO: ") + 1
						+ 6);

		strcpy(msg_log, "Terminado hilo Reduce Final Con Combiner");
		strcat(msg_log, " -> Nodo principal no disponible - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, nodoBloque->ipNodoPrincipal);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, nodoBloque->puertoNodoPrincipal);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR, msg_log);

		free(msg_log);
	}

	free(nodoBloque->ipNodoPrincipal);
	free(nodoBloque->puertoNodoPrincipal);

	void destruirListaReduceFinalConCombiner(
			reducesParcialesParaJob* datos_instruccion) {
		free(datos_instruccion->ipNodo);
		free(datos_instruccion->puertoNodo);
		free(datos_instruccion);

	}

	list_destroy_and_destroy_elements(nodoBloque->instruccionesReduce,
			(void*) destruirListaReduceFinalConCombiner);

	free(nodoBloque);

	//close(nodoSocket_fd);

}

void rutinaMapper() {

	instruccionMap* nodoBloque = malloc(sizeof(instruccionMap));
	nodoBloque->ipNodo = malloc(16);
	nodoBloque->puertoNodo = malloc(6);

	strcpy(nodoBloque->ipNodo, argumentosMap.map->ipNodo);
	strcpy(nodoBloque->puertoNodo, argumentosMap.map->puertoNodo);
	nodoBloque->bloqueNodo = argumentosMap.map->bloqueNodo;

	char* archivoTrabajo = malloc(strlen(argumentosMap.nombreArch) + 1);
	strcpy(archivoTrabajo, argumentosMap.nombreArch);

	pthread_mutex_unlock(&mutexMap);

	bloque *block = malloc(sizeof(bloque)); //para recibir
	bloque* bloqueAEnviar;
	char* accion_log;
	char* msg_log;
	int resultado;
	int accion;
	int nodoSocket_fd;

	char* bloqueNodo = string_itoa(nodoBloque->bloqueNodo);

	msg_log = malloc(
			strlen("Hilo Map") + 1 + strlen(" -> Nodo - ") + 1 + strlen("IP: ")
					+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6 + strlen(" BLOQUE: ")
					+ 1 + 4);

	strcpy(msg_log, "Hilo Map");
	strcat(msg_log, " -> Nodo - ");
	strcat(msg_log, "IP: ");
	strcat(msg_log, nodoBloque->ipNodo);
	strcat(msg_log, " PUERTO: ");
	strcat(msg_log, nodoBloque->puertoNodo);
	strcat(msg_log, " BLOQUE: ");
	strcat(msg_log, bloqueNodo);

	loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_INFO, msg_log);

	free(msg_log);
	free(bloqueNodo);

	pthread_mutex_lock(&mutexFDConexiones);

	nodoSocket_fd = crearConexion(nodoBloque->ipNodo, nodoBloque->puertoNodo);

	pthread_mutex_unlock(&mutexFDConexiones);

	if (nodoSocket_fd) {

		bloqueAEnviar = serializarMapNodo(datos_script, nodoBloque->bloqueNodo,
				nombreJob);

		enviarDatosNodo(nodoSocket_fd, bloqueAEnviar);

		accion_log = string_itoa(1);

		msg_log = malloc(
				strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1 + 4
						+ strlen(" -> Nodo - ") + 1 + strlen("IP: ") + 1 + 16
						+ strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "Mensaje Enviado: ");
		strcat(msg_log, "Accion - ");
		strcat(msg_log, accion_log);
		strcat(msg_log, " -> Nodo - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, nodoBloque->ipNodo);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, nodoBloque->puertoNodo);

		loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

		free(msg_log);
		free(accion_log);


		if ((accion = receive_and_unpack(block, nodoSocket_fd)) > 0) {

			accion_log = string_itoa(accion);

			msg_log = malloc(
					strlen("Mensaje Recibido: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> Nodo - ") + 1 + strlen("IP: ") + 1
							+ 16 + strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log, "Mensaje Recibido: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> Nodo - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, nodoBloque->ipNodo);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, nodoBloque->puertoNodo);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			resultado = deserializarResultadoMapNodo(block);

			pthread_mutex_lock(&mutexConexionesMarta);

			bloqueAEnviar = serializarResultadoMapMarta(nodoBloque, resultado,
					archivoTrabajo);
			enviarDatos(marta->ip_Marta, marta->puerto_Principal,
					bloqueAEnviar);

			accion_log = string_itoa(3);

			msg_log = malloc(
					strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> MaRTA - ") + 1 + strlen("IP: ")
							+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log, "Mensaje Enviado: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> MaRTA - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, marta->ip_Marta);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, marta->puerto_Principal);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			pthread_mutex_unlock(&mutexConexionesMarta);

			char* bloqueNodo = string_itoa(nodoBloque->bloqueNodo);

			msg_log = malloc(
					strlen("Hilo Map terminado exitosamente") + 1
							+ strlen(" -> Nodo - ") + 1 + strlen("IP: ") + 1
							+ 16 + strlen(" PUERTO: ") + 1 + 6
							+ strlen(" BLOQUE: ") + 1 + 4);

			strcpy(msg_log, "Hilo Map terminado exitosamente");
			strcat(msg_log, " -> Nodo - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, nodoBloque->ipNodo);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, nodoBloque->puertoNodo);
			strcat(msg_log, " BLOQUE: ");
			strcat(msg_log, bloqueNodo);

			loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_TRACE, msg_log);

			free(msg_log);
			free(bloqueNodo);

		} else {
			pthread_mutex_lock(&mutexConexionesMarta);

			resultado = 0;
			bloqueAEnviar = serializarResultadoMapMarta(nodoBloque, resultado,
					archivoTrabajo);
			enviarDatos(marta->ip_Marta, marta->puerto_Principal,
					bloqueAEnviar);

			char* accion_log = string_itoa(3);

			msg_log = malloc(
					strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1
							+ 4 + strlen(" -> MaRTA - ") + 1 + strlen("IP: ")
							+ 1 + 16 + strlen(" PUERTO: ") + 1 + 6);

			strcpy(msg_log, "Mensaje Enviado: ");
			strcat(msg_log, "Accion - ");
			strcat(msg_log, accion_log);
			strcat(msg_log, " -> MaRTA - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, marta->ip_Marta);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, marta->puerto_Principal);

			loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

			free(msg_log);
			free(accion_log);

			pthread_mutex_unlock(&mutexConexionesMarta);

			char* bloqueNodo = string_itoa(nodoBloque->bloqueNodo);

			msg_log = malloc(
					strlen("Terminado hilo Map") + 1
							+ strlen(" -> Desconexion nodo - ") + 1
							+ strlen("IP: ") + 1 + 16 + strlen(" PUERTO: ") + 1
							+ 6 + strlen(" BLOQUE: ") + 1 + 4);

			strcpy(msg_log, "Terminado hilo Map");
			strcat(msg_log, " -> Desconexion nodo - ");
			strcat(msg_log, "IP: ");
			strcat(msg_log, nodoBloque->ipNodo);
			strcat(msg_log, " PUERTO: ");
			strcat(msg_log, nodoBloque->puertoNodo);
			strcat(msg_log, " BLOQUE: ");
			strcat(msg_log, bloqueNodo);

			loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR, msg_log);

			free(msg_log);
			free(bloqueNodo);
		}
	} else {
		pthread_mutex_lock(&mutexConexionesMarta);

		resultado = 0;
		bloqueAEnviar = serializarResultadoMapMarta(nodoBloque, resultado,
				archivoTrabajo);
		enviarDatos(marta->ip_Marta, marta->puerto_Principal, bloqueAEnviar);

		char* accion_log = string_itoa(3);

		msg_log = malloc(
				strlen("Mensaje Enviado: ") + 1 + strlen("Accion - ") + 1 + 4
						+ strlen(" -> MaRTA - ") + 1 + strlen("IP: ") + 1 + 16
						+ strlen(" PUERTO: ") + 1 + 6);

		strcpy(msg_log, "Mensaje Enviado: ");
		strcat(msg_log, "Accion - ");
		strcat(msg_log, accion_log);
		strcat(msg_log, " -> MaRTA - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, marta->ip_Marta);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, marta->puerto_Principal);

		loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_INFO, msg_log);

		free(msg_log);
		free(accion_log);

		pthread_mutex_unlock(&mutexConexionesMarta);

		char* bloqueNodo = string_itoa(nodoBloque->bloqueNodo);

		msg_log = malloc(
				strlen("Terminado hilo Map") + 1
						+ strlen(" -> Nodo no disponible - ") + 1
						+ strlen("IP: ") + 1 + 16 + strlen(" PUERTO: ") + 1 + 6
						+ strlen(" BLOQUE: ") + 1 + 4);

		strcpy(msg_log, "Terminado hilo Map");
		strcat(msg_log, " -> Nodo no disponible - ");
		strcat(msg_log, "IP: ");
		strcat(msg_log, nodoBloque->ipNodo);
		strcat(msg_log, " PUERTO: ");
		strcat(msg_log, nodoBloque->puertoNodo);
		strcat(msg_log, " BLOQUE: ");
		strcat(msg_log, bloqueNodo);

		loguearme(PATH_LOG, "Proceso Job", 1, LOG_LEVEL_ERROR, msg_log);

		free(msg_log);
		free(bloqueNodo);
	}

	free(nodoBloque->ipNodo);
	free(nodoBloque->puertoNodo);
	free(nodoBloque);
	free(archivoTrabajo);

	//close(nodoSocket_fd);

}
