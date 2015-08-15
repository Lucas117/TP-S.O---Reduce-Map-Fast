#include "hSockets.h"
#include "hSerializadores.h"
#include "EstructurasFS.h"
#include "FuncionesFS.h"

//variables globales -----------------------

// para manejo de descriptores de fichero
fd_set master;
fd_set read_fds;
int fdmax; //cantidad maxima de descriptores de fichero

// ------------------------------------------

void* hiloParaEscuchar(void** args) {
	//incicializarSocket
	escucharConexiones(args[0], args[1], args[2], args[3], args[4], args[5],
			args[6], args[7], args[8], args[9]);

	return 0;
}

void establecerConexionConMarta(conexionMarta* conexion_Marta,
		char* puerto_escucha) {

	int fd_conexionRemota;
	char* msg_log;

	if ((fd_conexionRemota = crearConexion(conexion_Marta->ip_Marta,
			conexion_Marta->puerto_Principal, puerto_escucha))) {

		FD_SET(fd_conexionRemota, &master); // agrega listenerSockfd al conjunto maestro

		if (fd_conexionRemota > fdmax) {
			fdmax = fd_conexionRemota;	// actualizar el máximo
		}

	} else {

		msg_log = malloc(
				strlen("No se puede establecer conexion con MaRTA - IP: ") + 1
						+ 16 + strlen(" PUERTO PRINCIPAL: ") + 1 + 6);

		strcpy(msg_log, "No se puede establecer conexion con MaRTA - IP: ");
		strcat(msg_log, conexion_Marta->ip_Marta);
		strcat(msg_log, " PUERTO PRINCIPAL: ");
		strcat(msg_log, conexion_Marta->puerto_Principal);

		loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_ERROR, msg_log);

		free(msg_log);
		exit1ConMutexPersistencia();
	}

	if ((fd_conexionRemota = crearConexion(conexion_Marta->ip_Marta,
			conexion_Marta->puerto_ActualizadorLista, puerto_escucha))) {

		FD_SET(fd_conexionRemota, &master); // agrega listenerSockfd al conjunto maestro

		if (fd_conexionRemota > fdmax) {
			fdmax = fd_conexionRemota;	// actualizar el máximo
		}

	} else {

		msg_log = malloc(
				strlen("No se puede establecer conexion con MaRTA - IP: ") + 1
						+ 16 + strlen(" PUERTO ACTUALIZACION: ") + 1 + 6);

		strcpy(msg_log, "No se puede establecer conexion con MaRTA - IP: ");
		strcat(msg_log, conexion_Marta->ip_Marta);
		strcat(msg_log, " PUERTO ACTUALIZACION: ");
		strcat(msg_log, conexion_Marta->puerto_Principal);

		loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_ERROR, msg_log);

		free(msg_log);
		exit1ConMutexPersistencia();
	}

	bloque* bloqueAEnviar;
	bloqueAEnviar = serializarSaludoMarta();
	enviarDatos(conexion_Marta->ip_Marta, conexion_Marta->puerto_Principal,
			bloqueAEnviar);

	loguearme(PATH_LOG, "Proceso File System", 0, 2,
			"Conexion con Marta establecida");
}

int crearConexion(char* ip_destino, char* puerto_destino, char* puerto_escucha) {

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

int enviarDatosParaObtenerBloque(int fd_destino, bloque *block) {

	int datosEnviados = 0;
	paquete *package;

	package = pack(block);

	datosEnviados = sendAll(fd_destino, package);

	free(package->bloque);
	free(package);

	return datosEnviados;

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
			loguearme(PATH_LOG, "Proceso File System", 0, 4,
					"Error en el send de sendAll");
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
		loguearme(PATH_LOG, "Proceso File System", 0, 4,
				"No existe la IP de destino.");
		return datosEnviados;
	}

	package = pack(block);

	datosEnviados = sendAll(fd_ip_destino, package);

	free(package->bloque);
	free(package);

	return datosEnviados;

}

int nombrarNodo(int nombreNodo, char* ip, char* puerto) {
	bloque* bloqueAEnviar;

	bloqueAEnviar = serializarNombreNodo(nombreNodo);

	return enviarDatos(ip, puerto, bloqueAEnviar);

}

void escucharConexiones(char* puerto_escucha, conexionMarta* conexion_Marta,
		t_list* listaNodosEnElSitema, t_list* listaNodosEnEspera,
		t_list* archivos, t_list* directorios, void* nodos_Minimo,
		pthread_mutex_t* mutexListasNodos,
		pthread_mutex_t* mutexArchivosYDirectorios, pthread_mutex_t* mutexConexionMaRTA) {

	struct addrinfo hints;
	struct addrinfo *serverInfo;
	struct sockaddr_in client_addr;

	// para manejo del select
	FD_ZERO(&master);// borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
	fdmax = 0;

	int accion; //accion a tomar con el paquete recibido
	int new_fd; //descriptor de fichero del cliente
	int i; // iterador
	uint32_t nombreNodo;

	int nodosMinimo = *(int*) nodos_Minimo;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;		// No importa si uso IPv4 o IPv6
	hints.ai_flags = AI_PASSIVE;// Asigna el address del localhost: 127.0.0.1
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, puerto_escucha, &hints, &serverInfo);

	int listenerSockfd;
	if ((listenerSockfd = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol)) == -1) {
		perror("socket");
		loguearme(PATH_LOG, "Proceso File System", 0, 4,
				"Error al crear el socket");

		exit1ConMutexPersistencia();
	}

	int yes; // un inicador que lo utiliza el setsockpot
	if ((setsockopt(listenerSockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)))
			== -1) {
		loguearme(PATH_LOG, "Proceso File System", 0, 4, "Error de setsockopt");
		exit1ConMutexPersistencia();
	}

	if ((bind(listenerSockfd, serverInfo->ai_addr, serverInfo->ai_addrlen))
			== -1) {

		loguearme(PATH_LOG, "Proceso File System", 0, 4,
				"Puerto de escucha ya bindeado");
		exit1ConMutexPersistencia();
	}

	freeaddrinfo(serverInfo);

	int sin_size;

	if ((listen(listenerSockfd, BACKLOG)) == -1) {

		loguearme(PATH_LOG, "Proceso File System", 0, 4,
				"Error en la funcion listen");
		exit1ConMutexPersistencia();
	}

	FD_SET(listenerSockfd, &master); // agrega listenerSockfd al conjunto maestro

	if (listenerSockfd > fdmax) {
		fdmax = listenerSockfd;	// actualizar el máximo
	}

	establecerConexionConMarta(conexion_Marta, puerto_escucha); // se conecta con Marta
	pthread_mutex_unlock(mutexConexionMaRTA);

	while (1) {
		read_fds = master;
		if (select(fdmax + 1, &read_fds, NULL, NULL, NULL) == -1) {

			loguearme(PATH_LOG, "Proceso File System", 0, 4,
					"Error al crear el select de escucha");
			exit1ConMutexPersistencia();
		}

		for (i = 0; i <= fdmax; i++) { // explorar conexiones existentes en busca de datos que leer

			if (FD_ISSET(i, &read_fds)) {	// comprueba si hay datos

				if (i == listenerSockfd) {	// gestionar nuevas conexiones
					sin_size = sizeof(client_addr);
					if ((new_fd = accept(listenerSockfd,
							(struct sockaddr *) &client_addr, &sin_size))
							== -1) {

						loguearme(PATH_LOG, "Proceso File System", 0, 4,
								"Error en el accept del select");
					} else {
						FD_SET(new_fd, &master); // añadir al conjunto maestro
						if (new_fd > fdmax) {
							fdmax = new_fd;	// actualizar el máximo
						}

						/*printf("server: nueva coneccion de %s on "
						 "socket %d\n", inet_ntoa(client_addr.sin_addr), new_fd);*/
						pthread_mutex_lock(mutexListasNodos);
						loguearme(PATH_LOG, "Proceso File System", 0, 2,
								"Nueva conexion de Nodo");
						pthread_mutex_unlock(mutexListasNodos);
					}
				} else {	// ver si hay datos de un cliente

					bloque *block = malloc(sizeof(bloque));
					caracteristicasNodo* nodoNuevo;
					pedidoArchivo* pedido_archivo;
					trabajoJobTerminado* trabajo_terminado;

					accion = receive_and_unpack(block, i); // recive el paquete y retorna una accion

					switch (accion) {
					case -1:
						loguearme(PATH_LOG, "Proceso Job", 0, LOG_LEVEL_ERROR,
								"recv principal");
						pthread_mutex_lock(mutexListasNodos);
						if (i
								== obtenerDescriptorDeFichero(
										conexion_Marta->ip_Marta,
										conexion_Marta->puerto_Principal)) {
							loguearme(PATH_LOG, "Proceso File System", 0, 2,
									"Marta no se encuentra disponible");

							pthread_mutex_unlock(mutexListasNodos);

							exit1ConMutexPersistencia();
						}
						loguearme(PATH_LOG, "Proceso File System", 0, 2,
								"Se ha desconectado un Nodo");
						desconexionNodo(i, listaNodosEnElSitema,
								listaNodosEnEspera, conexion_Marta,
								nodosMinimo);
						close(i); //cerrar el descriptor
						FD_CLR(i, &master); // eliminar del conjunto maestro
						pthread_mutex_unlock(mutexListasNodos);
						break;
					case 0:
						pthread_mutex_lock(mutexListasNodos);
						if (i
								== obtenerDescriptorDeFichero(
										conexion_Marta->ip_Marta,
										conexion_Marta->puerto_Principal)) {
							loguearme(PATH_LOG, "Proceso File System", 0, 2,
									"Marta no se encuentra disponible");

							pthread_mutex_unlock(mutexListasNodos);

							exit1ConMutexPersistencia();
						}
						loguearme(PATH_LOG, "Proceso File System", 0, 2,
								"Se ha desconectado un Nodo");
						desconexionNodo(i, listaNodosEnElSitema,
								listaNodosEnEspera, conexion_Marta,
								nodosMinimo);
						close(i); //cerrar el descriptor
						FD_CLR(i, &master); // eliminar del conjunto maestro
						pthread_mutex_unlock(mutexListasNodos);
						break;
					case 1:
						pthread_mutex_lock(mutexListasNodos);
						//conexion establecida
						nodoNuevo = deserializarConexionNodo(block);
						conexionNodo(nodoNuevo, i, listaNodosEnElSitema,
								listaNodosEnEspera, conexion_Marta,
								nodosMinimo);
						pthread_mutex_unlock(mutexListasNodos);
						break;
					case 2:
						//tabla de archivos filesystem-marta
						pedido_archivo = deserializarPedidoBloquesDeArchivo(
								block);
						pthread_mutex_lock(mutexArchivosYDirectorios);
						buscarListaDeBloquesDeArchivoYEnviar(pedido_archivo,
								archivos, directorios, listaNodosEnElSitema,
								conexion_Marta->ip_Marta,
								conexion_Marta->puerto_Principal);
						pthread_mutex_unlock(mutexArchivosYDirectorios);
						break;
					case 3:
						// pedido marta guardar resultado job
						trabajo_terminado =
								deserializarGuardarResultadoFileSystem(block);
						guardarResultadoOperacionJobFileSystem(
								trabajo_terminado, archivos, directorios,
								listaNodosEnElSitema, mutexListasNodos,
								nodosMinimo, puerto_escucha, conexion_Marta);
						break;
					}
				}

			}
		}

	}

	close(listenerSockfd);
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

void eliminarSocketNodo(datosNodo* nodo) {
	bloque *bloqueAEnviar;

	bloqueAEnviar = serializarDesconexionNodo();
	enviarDatos(nodo->ip, nodo->puerto, bloqueAEnviar);

}

void conexionNodo(caracteristicasNodo* nodoNuevo, int fdNodo,
		t_list* listaNodosEnElSitema, t_list* listaNodosEnEspera,
		conexionMarta* conexion_Marta, int nodosMinimo) {

	struct sockaddr_in socketAddr;  // direccion ip del socket
	int socketAddr_len;

	datosNodo* aux;
	int i;
	int cantidadBloquesNodo;

	socketAddr_len = sizeof(socketAddr);
	getpeername(fdNodo, (struct sockaddr *) &socketAddr, &socketAddr_len); // sacar la direccion y puerto

	bool existeNodoEnLaLista(datosNodo* nodo) {
		return nodo->nombreNodo == nodoNuevo->nombreNodo;
	}

	if (list_any_satisfy(listaNodosEnElSitema, (void*) existeNodoEnLaLista)) {

		aux = list_find(listaNodosEnElSitema, (void*) existeNodoEnLaLista);
		strcpy(aux->ip, inet_ntoa(socketAddr.sin_addr));
		strcpy(aux->puerto, string_itoa(ntohs(socketAddr.sin_port)));
		strcpy(aux->puerto_escucha, nodoNuevo->puerto_escucha);
		aux->disponible = 1;

		loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_INFO,
				"Nodo ya conocido agregado al sistema");
		actualizarMarta(listaNodosEnElSitema, conexion_Marta, nodosMinimo);
	} else {
		aux = malloc(sizeof(datosNodo));

		aux->ip = malloc(16);
		aux->puerto = malloc(6);
		aux->puerto_escucha = malloc(6);
		aux->capacidadNodo = nodoNuevo->capacidadNodo;

		strcpy(aux->ip, inet_ntoa(socketAddr.sin_addr));
		strcpy(aux->puerto, string_itoa(ntohs(socketAddr.sin_port)));
		strcpy(aux->puerto_escucha, nodoNuevo->puerto_escucha);

		cantidadBloquesNodo = aux->capacidadNodo / 20971520;

		aux->bloquesOcupados = (int *) malloc(
				cantidadBloquesNodo * sizeof(int));

		for (i = 0; i < cantidadBloquesNodo; i++) {
			aux->bloquesOcupados[i] = 0;
		}

		aux->disponible = 0;

		list_add(listaNodosEnEspera, aux);
	}
	free(nodoNuevo);

}

void destruirNodo(datosNodo* nodo) {
	free(nodo->ip);
	free(nodo->puerto);
	free(nodo);
}

void desconexionNodo(int fdNodo, t_list* listaNodosEnElSitema,
		t_list* listaNodosEnEspera, conexionMarta* conexion_Marta,
		int nodosMinimo) {
	struct sockaddr_in socketAddr;  // direccion ip del socket
	int socketAddr_len;
	datosNodo* aux;

	socketAddr_len = sizeof(socketAddr);
	getpeername(fdNodo, (struct sockaddr *) &socketAddr, &socketAddr_len); // sacar la direccion y puerto

	bool existeNodoEnLaLista(datosNodo* nodo) {
		return (!strcmp(nodo->ip, inet_ntoa(socketAddr.sin_addr))
				&& !strcmp(nodo->puerto,
						string_itoa(ntohs(socketAddr.sin_port))));
	}

	if (list_any_satisfy(listaNodosEnElSitema, (void*) existeNodoEnLaLista)) {
		aux = list_find(listaNodosEnElSitema, (void*) existeNodoEnLaLista);
		aux->disponible = 0;
		actualizarMarta(listaNodosEnElSitema, conexion_Marta, nodosMinimo);

	} else {
		list_remove_and_destroy_by_condition(listaNodosEnEspera,
				(void*) existeNodoEnLaLista, (void*) destruirNodo);

	}

}

void actualizarMarta(t_list* listaNodosEnElSitema,
		conexionMarta* conexion_Marta, int nodosMinimo) {
	bloque* bloqueAEnviar;

	bloqueAEnviar = serializarActualizacionNodosDisponibles(
			listaNodosEnElSitema, nodosMinimo);
	enviarDatos(conexion_Marta->ip_Marta,
			conexion_Marta->puerto_ActualizadorLista, bloqueAEnviar);

	loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_INFO,
			"Actualizados nodos de MARTA.");
}

void buscarListaDeBloquesDeArchivoYEnviar(pedidoArchivo* archivo_buscado,
		t_list* archivos, t_list* directorios, t_list* nodos, char* ipMarta,
		char* puertoMarta) {

	bloque* bloqueAEnviar;
	char* msg_log;

	msg_log = malloc(
			strlen("MaRTA solicito la lista de datos bloque de ") + 1
					+ strlen(archivo_buscado->archivoBuscado) + 1);

	strcpy(msg_log, "MaRTA solicito la lista de datos bloque de ");
	strcat(msg_log, archivo_buscado->archivoBuscado);

	loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_TRACE, msg_log);

	free(msg_log);

	t_list* listadatosBloque;
	if (list_size(archivos) && list_size(directorios)) {
		if ((listadatosBloque = listaDeBloquesDelArchivo(
				archivo_buscado->archivoBuscado, archivos, directorios, nodos))
				== NULL) {
			bloqueAEnviar = serializarArchivoNoExiste(archivo_buscado);
			enviarDatos(ipMarta, puertoMarta, bloqueAEnviar);

			msg_log = malloc(
					strlen("No se encuentra en el sistema el archivo ") + 1
							+ strlen(archivo_buscado->archivoBuscado) + 1);

			strcpy(msg_log, "No se encuentra en el sistema el archivo ");
			strcat(msg_log, archivo_buscado->archivoBuscado);

			loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_WARNING,
					msg_log);

			free(msg_log);

		} else {
			bloqueAEnviar = serializarBloquesParaMarta(listadatosBloque,
					archivo_buscado, nodos);
			enviarDatos(ipMarta, puertoMarta, bloqueAEnviar);

			msg_log =
					malloc(
							strlen(
									"Se envio con exito la lista de datos bloque de archivo ")
									+ 1
									+ strlen(archivo_buscado->archivoBuscado)
									+ 1);

			strcpy(msg_log,
					"Se envio con exito la lista de datos bloque de archivo ");
			strcat(msg_log, archivo_buscado->archivoBuscado);

			loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_INFO,
					msg_log);

			free(msg_log);
		}

	} else {
		bloqueAEnviar = serializarArchivoNoExiste(archivo_buscado);
		enviarDatos(ipMarta, puertoMarta, bloqueAEnviar);

		msg_log = malloc(
				strlen("No se encuentra en el sistema el archivo ") + 1
						+ strlen(archivo_buscado->archivoBuscado) + 1);

		strcpy(msg_log, "No se encuentra en el sistema el archivo ");
		strcat(msg_log, archivo_buscado->archivoBuscado);

		loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_WARNING,
				msg_log);

		free(msg_log);
	}

}

int indiceDelDirectorio(char* ruta, t_list* directorios) { // ruta: "/root/pepe"
	if (!strcmp(ruta, "/root")) {
		return 0;

	}
	int contador = strlen(ruta) - 1;
	while (ruta[contador] != '/') {
		contador--;
	}

	int largoNombre = strlen(ruta) - contador - 1;
	char* nombreDirectorio = malloc(largoNombre + 1);
	int i;
	for (i = 0; i < largoNombre; i++) {
		nombreDirectorio[i] = ruta[contador + i + 1];
	}
	nombreDirectorio[largoNombre] = '\0';

	int largoRutaRestante = strlen(ruta) - largoNombre - 1;
	char* rutaRestante = malloc(largoRutaRestante + 1);
	for (i = 0; i < largoRutaRestante; i++) {
		rutaRestante[i] = ruta[i];
	}
	rutaRestante[largoRutaRestante] = '\0';

	bool directorioBuscado(directorio* dir) {
		return !strcmp(dir->nombre, nombreDirectorio)
				&& dir->padre == indiceDelDirectorio(rutaRestante, directorios);
	}

	directorio* dAux; // = list_find(directorios, (void*) directorioBuscado);
	if (list_find(directorios, (void*) directorioBuscado)) {
		dAux = list_find(directorios, (void*) directorioBuscado);
		free(rutaRestante);
		free(nombreDirectorio);

		return dAux->index;
	} else {
		free(rutaRestante);
		free(nombreDirectorio);

		return -1;
	}

}

int archivoEstaDisponible(archivo* arch, t_list* nodosEnSistema) {

	bool tieneCopiaDisponible(datoBloque* bloque) {

		bool estaDisponible(infoCopia* copia) {

			bool tieneNombreComoCopia(datosNodo* nodoAux) {
				return copia->nombreNodo == nodoAux->nombreNodo;
			}

			datosNodo* nodo = list_find(nodosEnSistema,
					(void*) tieneNombreComoCopia);
			return nodo->disponible;
		}

		return list_any_satisfy(bloque->copias, (void*) estaDisponible);
	}

	return list_all_satisfy(arch->listaBloques, (void*) tieneCopiaDisponible);
}

t_list* listaDeBloquesDelArchivo(char* archivo_buscado, t_list* archivos,
		t_list* directorios, t_list* nodosEnSistema) {
	int contador = strlen(archivo_buscado) - 1;
	while (archivo_buscado[contador] != '/') {
		contador--;
	}

	int largoNombre = strlen(archivo_buscado) - contador - 1;
	char* nombreArchivo = malloc(largoNombre + 1);
	int i;
	for (i = 0; i < largoNombre; i++) {
		nombreArchivo[i] = archivo_buscado[contador + i + 1];
	}
	nombreArchivo[largoNombre] = '\0';

	int largoRutaRestante = strlen(archivo_buscado) - largoNombre - 1;
	char* rutaRestante = malloc(largoRutaRestante + 1);
	for (i = 0; i < largoRutaRestante; i++) {
		rutaRestante[i] = archivo_buscado[i];
	}
	rutaRestante[largoRutaRestante] = '\0'; //"/root/pepe/caca/asd.txt"

	int directorioEnElQueEsta = indiceDelDirectorio(rutaRestante, directorios);

	if (directorioEnElQueEsta == -1) {
		free(nombreArchivo);
		free(rutaRestante);
		return NULL;
	}

	bool esElArchivoBuscado(archivo* archi) {
		return !strcmp(archi->nombre, nombreArchivo)
				&& archi->directorio == directorioEnElQueEsta;
	}

	archivo* archivoBuscado = list_find(archivos, (void*) esElArchivoBuscado);
	if (archivoBuscado == NULL) {
		return NULL;
	}
	if (!archivoEstaDisponible(archivoBuscado, nodosEnSistema)) {
		return NULL;
	}

	free(nombreArchivo);
	free(rutaRestante);
	return archivoBuscado->listaBloques;
}

void agregarResultadoAlMDFS(char* rutaDelResultado, char* dondeLoGuardo,
		t_list* archivos, t_list* directorios, t_list* nodosSistema,
		pthread_mutex_t* mutex, int nodosMinimos) {

	int contador = strlen(dondeLoGuardo) - 1;
	while (dondeLoGuardo[contador] != '/') {
		contador--;
	}
	int largoNombreArchivo = strlen(dondeLoGuardo) - contador - 1;

	int largoDirectorio = strlen(dondeLoGuardo) - largoNombreArchivo - 1;
	char* directorioEnMDFS = malloc(largoDirectorio + 1);
	memcpy(directorioEnMDFS, dondeLoGuardo, largoDirectorio);
	directorioEnMDFS[largoDirectorio] = '\0';

	int indexDelDirectorio = indiceDelDirectorio(directorioEnMDFS, directorios);

	agregarArchivoAlMDFS(rutaDelResultado, string_itoa(indexDelDirectorio),
			nodosSistema, mutex, archivos, directorios, nodosMinimos);

	free(directorioEnMDFS);
	return;
}

void guardarResultadoOperacionJobFileSystem(
		trabajoJobTerminado* trabajo_terminado, t_list* archivos,
		t_list* directorios, t_list* nodosSistema, pthread_mutex_t* mutex,
		int nodosMinimos, char* puerto_escucha, conexionMarta* conexion_Marta) {

	bloqueMapeado* bloque_mapeado;
	bloque* bloqueAEnviar;
	bloque *block = malloc(sizeof(bloque));

	int fdNodo;
	int accion;
	int resultadoGuardado;

	fdNodo = crearConexion(trabajo_terminado->ipNodo,
			trabajo_terminado->puertoNodo, puerto_escucha);
	if (fdNodo) {

		bloqueAEnviar = serializarPedidoOperacionJobFinalizada(
				trabajo_terminado->nombreJob);
		enviarDatosParaObtenerBloque(fdNodo, bloqueAEnviar);

		if ((accion = receive_and_unpack(block, fdNodo)) > 0) {

			bloque_mapeado = deserializarBloqueArchivoSolicitado(block);
			char* rutaDelResultado = bajarResultadoAlSistema(bloque_mapeado,
					trabajo_terminado->resultadoOperacion);
			agregarResultadoAlMDFS(rutaDelResultado,
					trabajo_terminado->resultadoOperacion, archivos,
					directorios, nodosSistema, mutex, nodosMinimos);

			persistirDirectorios(directorios);
			persistirArchivos(archivos);
			persistirNodos(nodosSistema);

			remove(rutaDelResultado);

			resultadoGuardado = 1;
			bloqueAEnviar = serializarGuardadoResultadoFileSystem(
					resultadoGuardado, trabajo_terminado);
			enviarDatos(conexion_Marta->ip_Marta,
					conexion_Marta->puerto_Principal, bloqueAEnviar);

		} else {
			resultadoGuardado = 0;
			bloqueAEnviar = serializarGuardadoResultadoFileSystem(
					resultadoGuardado, trabajo_terminado);
			enviarDatos(conexion_Marta->ip_Marta,
					conexion_Marta->puerto_Principal, bloqueAEnviar);

		}
	} else {
		resultadoGuardado = 0;
		bloqueAEnviar = serializarGuardadoResultadoFileSystem(resultadoGuardado,
				trabajo_terminado);
		enviarDatos(conexion_Marta->ip_Marta, conexion_Marta->puerto_Principal,
				bloqueAEnviar);
	}
}

char* bajarResultadoAlSistema(bloqueMapeado* bloque_mapeado,
		char* dondeLoGuardoEnMDFS) {

	int contador = strlen(dondeLoGuardoEnMDFS) - 1;
	while (dondeLoGuardoEnMDFS[contador] != '/') { // /root/asd
		contador--;
	}

	int largoNombreArchivo = strlen(dondeLoGuardoEnMDFS) - contador - 1;

	char* nombreArchivo = malloc(largoNombreArchivo + 1);
	memcpy(nombreArchivo, dondeLoGuardoEnMDFS + contador + 1,
			largoNombreArchivo);
	nombreArchivo[largoNombreArchivo] = '\0';

	char* rutaArchivo = malloc(strlen("/tmp/") + largoNombreArchivo + 1);
	strcpy(rutaArchivo, "/tmp/");
	strcat(rutaArchivo, nombreArchivo);
	FILE* archivo = fopen(rutaArchivo, "w");
	fwrite(bloque_mapeado->bloque, 1, bloque_mapeado->tamanio_bloque, archivo);
	fclose(archivo);

	free(nombreArchivo);

	return rutaArchivo;
}
