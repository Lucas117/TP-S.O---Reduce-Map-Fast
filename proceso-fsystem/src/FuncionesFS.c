#include "FuncionesFS.h"
#include "EstructurasFS.h"
#include <commons/string.h>
#include <commons/collections/list.h>
#include <commons/config.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include "hSerializadores.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include "hSockets.h"
/*
 * PATH es una direccion absoluta para el archivo a abrir, por ejemplo:
 * 	/home/utnso/Escritorio/archivo.conf --
 * 	Cambienla por la ruta al archivo donde lo pongan ustedes
 * 		*/
// LOG
pthread_mutex_t mutexLogFS = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexPersistencia = PTHREAD_MUTEX_INITIALIZER;
int nodoAEnviarDatos = 0;

void exit1ConMutexPersistencia() {
	pthread_mutex_lock(&mutexPersistencia);
	loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_ERROR,"Proceso Terminado!");
	exit(1);
}

void loguearme(char *ruta, char *nombre_proceso, bool mostrar_por_consola,
		t_log_level nivel, const char *mensaje) {

	pthread_mutex_lock(&mutexLogFS);
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
	pthread_mutex_unlock(&mutexLogFS);
}

void espacioDisponibleDeMDFS(t_list* nodosEnSistema) {
	int cantNodosEnSistema = list_size(nodosEnSistema);
	int j;
	int i;
	int bloquesLibresDis = 0;
	int bloquesTotalesDis = 0;
	int bloquesLibres = 0;
	int bloquesTotales = 0;
	datosNodo* nodoAux;
	for (j = 0; j < cantNodosEnSistema; j++) {
		nodoAux = list_get(nodosEnSistema, j);
		if (nodoAux->disponible) {
			for (i = 0; i < ((nodoAux->capacidadNodo) / (1024 * 1024 * 20));
					i++) {
				if (!nodoAux->bloquesOcupados[i]) {
					bloquesLibresDis++;
				}
				bloquesTotalesDis++;
			}
			if (!nodoAux->bloquesOcupados[i]) {
				bloquesLibres++;
			}
			bloquesTotales++;
		}
	}

	printf(
			"Espacio disponible: %dMB de un total de %dMB (solo nodos disponibles)\n",
			bloquesLibresDis * 20, bloquesTotalesDis * 20);
	//printf("Espacio disponible: %dMB de un total de %dMB (incluyendo nodos no disponibles)\n",bloquesLibres*20,bloquesTotales*20);

}

void mostrarNodo(datosNodo* nodo) {
	int i;
	printf("//////////////////////////////////////////////////\n");
	printf("Nombre de nodo: %d\n", nodo->nombreNodo);
	printf("Ip: %s\n", nodo->ip);
	printf("Puerto: %s\n", nodo->puerto);
	printf("Puerto escucha: %s\n", nodo->puerto_escucha);
	for (i = 0; i < ((nodo->capacidadNodo) / (1024 * 1024 * 20)); i++) {
		printf("%d", nodo->bloquesOcupados[i]);
	}
	printf("\n");
	printf("Capacidad: %dB\n", nodo->capacidadNodo);
	printf("Disponible: %d\n", nodo->disponible);
	printf("//////////////////////////////////////////////////\n");
}

void mostrarNodos(t_list* nodos) {
	if (!list_is_empty(nodos)) {
		list_iterate(nodos, (void*) mostrarNodo);
	} else
		fprintf(stderr, "ERROR: No hay nodos disponibles en el sistema\n");

}

int copiaDisponible(infoCopia* copia, t_list* nodosDisponibles) {

	bool nodoBuscado(datosNodo* nodo) {
		return nodo->nombreNodo == copia->nombreNodo;
	}

	datosNodo* nodo = list_find(nodosDisponibles, (void*) nodoBuscado);

	return nodo->disponible;
}

void recuperarArchivoDelMDFS(char* nombreArchivo, char* directorioArchivo,
		char* rutaDestino, t_list* archivos, t_list* directorios,
		t_list* nodosEnSistema, conexionMarta* conexionMarta, int nodosMinimos,
		t_list* nodosEnEspera, char* puerto_escucha) {

	int directorioArchivoInt = atoi(directorioArchivo);

	bool esElArchivoBuscado(archivo* arch) {
		return arch->directorio == directorioArchivoInt
				&& !strcmp(nombreArchivo, arch->nombre);
	}

	archivo* archivoBuscado = list_find(archivos, (void*) esElArchivoBuscado);

	if (!archivoBuscado) {
		loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
				"No existe el archivo buscado");
		return;
	}

	t_list* copiasSeleccionadas = list_create();

	void seleccionarCopia(datoBloque* bloque) {
		int i;

//		infoCopia* copia = list_get(bloque->copias, 0);
//		if (copiaDisponible(copia, nodosEnSistema)) {
//			list_add(copiasSeleccionadas, copia);
//			return;
//		}

		for (i = 0; i < bloque->numeroDeCopias; i++) {
			infoCopia* copia2 = list_get(bloque->copias, i);

			if (copiaDisponible(copia2, nodosEnSistema)) {
				list_add(copiasSeleccionadas, copia2);
				return;
			}

		}

	}

	list_iterate(archivoBuscado->listaBloques, (void*) seleccionarCopia);

	if (list_size(copiasSeleccionadas)
			!= list_size(archivoBuscado->listaBloques)) {
		loguearme(PATH_LOG, "Proceso File System", 1, LOG_LEVEL_ERROR,
				"No se pueden recuperar los bloques del archivo.");
		return;
	}

	t_list* cachosDeArchivo = list_create();

	void recuperarArchivo(infoCopia* copia) {

		bloque *block = malloc(sizeof(bloque)); // para recibir los datos
		int accion;
		int fdNodo;

		bool obtenerNodo(datosNodo* nodo) {
			return copia->nombreNodo == nodo->nombreNodo;
		}

		datosNodo* nodoBuscado = list_find(nodosEnSistema, (void*) obtenerNodo);

		if ((fdNodo = crearConexion(nodoBuscado->ip,
				nodoBuscado->puerto_escucha, puerto_escucha))) {

			bloque* bloqueAEnviar;
			bloqueAEnviar = serializarSolicitarBloqueArchivo(copia->nroBloque);

			if (enviarDatosParaObtenerBloque(fdNodo, bloqueAEnviar)) {

				bloqueMapeado* datos;
				if ((accion = receive_and_unpack(block, fdNodo)) > 0) {
					datos = deserializarBloqueArchivoSolicitado(block);
					list_add(cachosDeArchivo, datos);

				}

			}
			close(fdNodo);
		}

		return;
	}

	list_iterate(copiasSeleccionadas, (void*) recuperarArchivo);

	if (list_size(cachosDeArchivo) != list_size(copiasSeleccionadas)) {

		loguearme(PATH_LOG, "Proceso File System", 1, LOG_LEVEL_ERROR,
				"No se pueden recuperar los bloques del archivo.");
		return;
	}

	char* rutaDondeCreoElArchivoConElNombreDelArchivo = malloc(
			strlen(nombreArchivo) + strlen(rutaDestino) + 5);

	int finalRuta = strlen(rutaDestino);
	int tieneBarra = 0;

	if (rutaDestino[finalRuta - 1] == '/') {
		tieneBarra = 1;
	}

	strcpy(rutaDondeCreoElArchivoConElNombreDelArchivo, rutaDestino);
	if (!tieneBarra) {
		strcat(rutaDondeCreoElArchivoConElNombreDelArchivo, "/");
	}
	strcat(rutaDondeCreoElArchivoConElNombreDelArchivo, nombreArchivo);

	FILE* archivoACrear = fopen(rutaDondeCreoElArchivoConElNombreDelArchivo,
			"a");
	int i;

	bloqueMapeado* caca;
	for (i = 0; i < (list_size(cachosDeArchivo)); i++) {
		caca = list_get(cachosDeArchivo, i);
		fwrite(caca->bloque, 1, caca->tamanio_bloque, archivoACrear);

	}
	fclose(archivoACrear);

	loguearme(PATH_LOG, "Proceso File System", 1, LOG_LEVEL_INFO,
			"Se ha recuperado el archivo exitosamente.");
	return;

}

int simuladorDeGuardadoDeArchivo(int nodosDisponibles, int tamanioArchivo,
		t_list* listaDisponibles) {
	int matriz[nodosDisponibles][103];
	int i, j;
	datosNodo* nodoAux;
	for (i = 0; i < nodosDisponibles; i++) {
		nodoAux = list_get(listaDisponibles, i);
		matriz[i][0] = (nodoAux->capacidadNodo) / (1024 * 1024 * 20);
		for (j = 1; j < ((nodoAux->capacidadNodo) / (1024 * 1024 * 20) + 1);
				j++) {
			if (!nodoAux->bloquesOcupados[j - 1]) {
				matriz[i][j] = -1;
			} else
				matriz[i][j] = -2;
		}
	}
	int numBloquesArchivo = tamanioArchivo / 20971520;
	if (tamanioArchivo % 20971520)
		numBloquesArchivo++;

	int contador = 0;
	int nodoActual = nodoAEnviarDatos;
	int caca = 0;
	int maestroPokemon = 0;
	while (contador != numBloquesArchivo) {
		for (j = 0; j < 3; j++) {
		maestroPokemon = 0;
			do {
				for (i = 1; i < matriz[nodoActual][0] + 1; i++) {
					if (matriz[nodoActual][i] == contador)
						return 0;
				}
				for (i = 1; i < matriz[nodoActual][0] + 1; i++) {
					if (matriz[nodoActual][i] == -1) {
						matriz[nodoActual][i] = contador;
						caca = 0;
						break;
					}
					if (i == matriz[nodoActual][0]) {
						nodoActual++;
                	if (nodoActual == nodosDisponibles) nodoActual = 0;
						caca = 1;
                      break;
					}
				}
			maestroPokemon++;
			if(maestroPokemon == nodosDisponibles && caca) return 0;
			} while (caca);
			nodoActual++;
			if (nodoActual == nodosDisponibles)
				nodoActual = 0;
		}
		contador++;
	}
	return 1;
}

void agregarArchivoAlMDFS(char *archivo_a_agregar, char* indice,
		t_list *nodos_sistema, pthread_mutex_t *mutexFD, t_list * archivos,
		t_list* directorios, int nodosMinimo) {

	int nodos_Disponibles = 0;

	void disponibilidadNodo(datosNodo* nodo) {
		nodos_Disponibles += nodo->disponible;
	}

	list_iterate(nodos_sistema, (void*) disponibilidadNodo);

	if (nodos_Disponibles < nodosMinimo) {

		loguearme(PATH_LOG, "Proceso File System", 0, LOG_LEVEL_ERROR,
				"Cantidad de nodos disponibles es menor al minimo.");
		return;
	}

	struct stat file;
	int indiceInt = atoi(indice);
	int file_descriptor, tamanio_archivo;
	int desplazamiento = 0;
	int bytes_sobrantes = 0;
	int tamanio_final = 0;
	archivo * archivoNuevo;
	archivoNuevo = malloc(sizeof(archivo));
	archivoNuevo->nombre = malloc(128);

	bool existeElDirectorioo(directorio* dire) {
		return dire->index == indiceInt;
	}

	if (list_any_satisfy(directorios, (void*) existeElDirectorioo)) {

	} else {
		loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
				"El directorio no existe, el archivo se guardara en el root");
		indiceInt = 0;
	}

	char* nombreArchivo = malloc(128);

	int aux = strlen(archivo_a_agregar) - 1;
	while (archivo_a_agregar[aux] != '/') {
		aux--;
	}
	aux++;
	int i = 0;
	while (archivo_a_agregar[aux] != '\0') {
		nombreArchivo[i] = archivo_a_agregar[aux];
		i++;
		aux++;
	}
	nombreArchivo[i] = '\0';

	bool existeArchivoConEseNombreEnEseDirectorio(archivo* archi) {
		return !strcmp(nombreArchivo, archi->nombre)
				&& archi->directorio == indiceInt;
	}

	if (list_any_satisfy(archivos,
			(void*) existeArchivoConEseNombreEnEseDirectorio)) {
		strcat(nombreArchivo, "-copia");
		while(list_any_satisfy(archivos,
			(void*) existeArchivoConEseNombreEnEseDirectorio)) {
			strcat(nombreArchivo, "-copia");
		}
		loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
				"Ya existe un archivo con ese nombre en ese directorio, se modificara el nombre del nuevo archivo");
	}

	strcpy(archivoNuevo->nombre, nombreArchivo);
	archivoNuevo->directorio = indiceInt;
	archivoNuevo->estado = 1;
	char *datos_a_copiar = malloc(TAMANIO_BLOQUE);
	void *archivo_mapeado;

	/*obtengo el descriptor del fichero*/
	file_descriptor = open(archivo_a_agregar, O_RDONLY);
	if (file_descriptor == -1) {

		loguearme(PATH_LOG, "Proceso File System", 0, LOG_LEVEL_ERROR,
				"No se puede abrir el archivo.");
		return;
	}

	fstat(file_descriptor, &file);

	tamanio_archivo = file.st_size;
	archivoNuevo->tamanioEnBytes = tamanio_archivo;

//begin magia
	bool estaNodoDisponible(datosNodo* nodo) {
		return nodo->disponible == 1;
	}

	t_list* listaFiltrada = list_filter(nodos_sistema,
			(void*) estaNodoDisponible);
	int nodosDisponibles = list_size(listaFiltrada);

	if (!simuladorDeGuardadoDeArchivo(nodosDisponibles, tamanio_archivo,
			listaFiltrada)) {

		loguearme(PATH_LOG, "Proceso File System", 1, LOG_LEVEL_ERROR,
				"No hay espacio suficiente. No se pudo guardar el archivo.");

		return;
	}
//end magia

	archivo_mapeado = mmap((caddr_t) 0, file.st_size, PROT_READ, MAP_PRIVATE,
			file_descriptor, 0);

	if (archivo_mapeado == MAP_FAILED) {
		loguearme(PATH_LOG, "Proceso File System", 1, LOG_LEVEL_ERROR,
				"No se pudo mapear el archivo en memoria.");
		return;
	}

	t_list* bloquesArchivo = list_create();
	t_list* copiasBloque;
	datoBloque* bloque;
	infoCopia* copia;
	int contbloq = 0;

	while (1) {

		if ((tamanio_archivo - desplazamiento) < TAMANIO_BLOQUE) {

			copiasBloque = list_create();

			bloque = malloc(sizeof(datoBloque));
			bloque->numeroDeCopias = 3;
			bloque->nrobloque = contbloq;

			tamanio_final = (tamanio_archivo - desplazamiento);

			memcpy(datos_a_copiar, archivo_mapeado + desplazamiento,
					tamanio_final); //cambio tamanio_bloque por tamanio_final

			armarBloqueFinal(datos_a_copiar, tamanio_final); //La que tengo que enviar es datos_a_copiar

			copia = malloc(sizeof(infoCopia));
			copia = enviarBloqueCortado(datos_a_copiar, nodos_sistema, mutexFD); // La variable que falta es la lista de nodos
			list_add(copiasBloque, copia);
			copia = malloc(sizeof(infoCopia));
			copia = enviarBloqueCortado(datos_a_copiar, nodos_sistema, mutexFD); // La variable que falta es la lista de nodos
			list_add(copiasBloque, copia);
			copia = malloc(sizeof(infoCopia));
			copia = enviarBloqueCortado(datos_a_copiar, nodos_sistema, mutexFD); // La variable que falta es la lista de nodos
			list_add(copiasBloque, copia);

			bloque->copias = copiasBloque;
			list_add(bloquesArchivo, bloque);
			loguearme(PATH_LOG, "Proceso File System", 1, LOG_LEVEL_INFO,
					"Enviadas las 3 copias del bloque.");

			contbloq++;

			free(datos_a_copiar);

			break;

		}

		copiasBloque = list_create();

		bloque = malloc(sizeof(datoBloque));
		bloque->numeroDeCopias = 3;
		bloque->nrobloque = contbloq;

		memcpy(datos_a_copiar, archivo_mapeado + desplazamiento,
		TAMANIO_BLOQUE);

		bytes_sobrantes = armarBloque(datos_a_copiar, TAMANIO_BLOQUE);
		copia = malloc(sizeof(infoCopia));
		copia = enviarBloqueCortado(datos_a_copiar, nodos_sistema, mutexFD); // La variable que falta es la lista de nodos
		list_add(copiasBloque, copia);
		copia = malloc(sizeof(infoCopia));
		copia = enviarBloqueCortado(datos_a_copiar, nodos_sistema, mutexFD); // La variable que falta es la lista de nodos
		list_add(copiasBloque, copia);
		copia = malloc(sizeof(infoCopia));
		copia = enviarBloqueCortado(datos_a_copiar, nodos_sistema, mutexFD); // La variable que falta es la lista de nodos
		list_add(copiasBloque, copia);

		bloque->copias = copiasBloque;
		list_add(bloquesArchivo, bloque);
		contbloq++;
		loguearme(PATH_LOG, "Proceso File System", 1, LOG_LEVEL_INFO,
				"Enviadas las 3 copias del bloque.");
		desplazamiento = desplazamiento + (TAMANIO_BLOQUE - bytes_sobrantes);

	}

	archivoNuevo->listaBloques = bloquesArchivo;
	list_add(archivos, archivoNuevo);
	loguearme(PATH_LOG, "Proceso File System", 1, LOG_LEVEL_INFO,
			"Archivo enviado a nodos correctamente.");
	munmap(archivo_mapeado, file.st_size);

	close(file_descriptor);

}

int armarBloque(char *bloque_completo, int tamanio_aux) {

	int restante = tamanio_aux - 1;

	while (restante >= 0 && bloque_completo[restante] != '\n') {
		bloque_completo[restante] = '0';

		restante--;

	}

	return TAMANIO_BLOQUE - (restante + 1);
}

void armarBloqueFinal(char *bloque_final, int cantidad_escrita) {

	int contador = TAMANIO_BLOQUE - 1;

	while (contador >= cantidad_escrita) {

		bloque_final[contador] = '0';

		contador--;
	}
}



infoCopia* enviarBloqueCortado(char* datosBloque, t_list* nodosEnSistema,
		pthread_mutex_t *mutexListasNodos) {

	pthread_mutex_lock(mutexListasNodos);
	infoCopia* ret = malloc(sizeof(infoCopia));
	ret->ip = malloc(16);
	ret->puerto = malloc(6);
	ret->puerto_escucha = malloc(6);
	bloque* bloqueAEnviar;
	datosNodo* nodoAux;
	int i = 0;

	do {
		nodoAux = list_get(nodosEnSistema, nodoAEnviarDatos);
		nodoAEnviarDatos++;
		if (nodoAEnviarDatos == list_size(nodosEnSistema)) {
			nodoAEnviarDatos = 0;
		}
	} while (!nodoAux->disponible);

	while (nodoAux->bloquesOcupados[i]) {
		i++;
		if (i == ((nodoAux->capacidadNodo) / (1024 * 1024 * 20))) {
			pthread_mutex_unlock(mutexListasNodos);
			return enviarBloqueCortado(datosBloque, nodosEnSistema,
					mutexListasNodos);  //MIKE WAS HER, y la cago
		}

	}

	bloqueAEnviar = serializarBloqueNodo(datosBloque, i);
	loguearme(PATH_LOG, "Proceso File System", 0, LOG_LEVEL_INFO,
			"Comienza envio de copia.");
	if (enviarDatos(nodoAux->ip, nodoAux->puerto, bloqueAEnviar)) {
		pthread_mutex_unlock(mutexListasNodos);
		nodoAux->bloquesOcupados[i] = 1;
		strcpy(ret->ip, nodoAux->ip);
		strcpy(ret->puerto, nodoAux->puerto);
		strcpy(ret->puerto_escucha, nodoAux->puerto_escucha);
		ret->nroBloque = i;
		ret->nombreNodo = nodoAux->nombreNodo;

		loguearme(PATH_LOG, "Proceso File System", 0, LOG_LEVEL_INFO,
				"Copia de bloque enviada.");
		return ret;

	} else {
		pthread_mutex_unlock(mutexListasNodos);
		return enviarBloqueCortado(datosBloque, nodosEnSistema,
				mutexListasNodos);
	}

}

void formatearMDFS(t_list* archivos, t_list* nodos, t_list* directorios) {

	borrarDirectorio("1024", "root", directorios, archivos, nodos);

	directorio *root;

	root = malloc(sizeof(directorio));

	root->nombre = malloc(sizeof(char) * 4);
	strcpy(root->nombre, "root");
	root->index = 0;
	root->padre = 1024;

	list_add(directorios, root);

	loguearme(PATH_LOG, "Proceso File System", 1, LOG_LEVEL_INFO,
			"MDFS Formateado.");

}

//NODOS

void agregarNodo(t_list* espera, t_list* sistema, conexionMarta* conexion_Marta,
		int nodosMinimo, pthread_mutex_t *mutexListasNodos) {

	pthread_mutex_lock(mutexListasNodos);

	int Listsistema_size;

	if (list_is_empty(espera)) {
		printf("No hay nodos para agregar\n");
		loguearme(PATH_LOG, "Proceso File System", 0, LOG_LEVEL_WARNING,
				"No hay nodos para agregar.");
		pthread_mutex_unlock(mutexListasNodos);
		return;
	}

	t_list* lAux = list_take_and_remove(espera, 1);
	datosNodo* aux = list_get(lAux, 0);
	Listsistema_size = list_size(sistema);

	if (Listsistema_size) {
		datosNodo* aux_sistema = list_get(sistema, Listsistema_size - 1);
		aux->nombreNodo = aux_sistema->nombreNodo + 1;

	} else {
		aux->nombreNodo = 1;

	}
	aux->disponible = 1;

	if (nombrarNodo(aux->nombreNodo, aux->ip, aux->puerto)) {
		list_add(sistema, aux);
		actualizarMarta(sistema, conexion_Marta, nodosMinimo);

		char* msjLog =
				malloc(
						strlen(
								"Agregado con exito el nodo con ip y puerto al FileSystem")
								+ 1 + 16 + 6);
		strcpy(msjLog, "Agregado con exito el nodo con ip ");
		strcat(msjLog, aux->ip);
		strcat(msjLog, " y puerto ");
		strcat(msjLog, aux->puerto_escucha);
		strcat(msjLog, " al FileSystem");

		loguearme(PATH_LOG, "Proceso File System", 0, LOG_LEVEL_INFO, msjLog);
		free(msjLog);
		printf(
				"Agregado con �xito el nodo con ip %s y puerto %s al FileSystem\n",
				aux->ip, aux->puerto_escucha);
		pthread_mutex_unlock(mutexListasNodos);

		return;
	} else {
		printf("No se pudo agregar el nodo al FileSystem");
		loguearme(PATH_LOG, "Proceso File System", 0, 4,
				"No se pudo agregar el nodo a la lista de aceptados.");
		pthread_mutex_unlock(mutexListasNodos);
		return;
	}
}

void eliminarNodo(char* ip, char* nombreNodo, t_list* sistema,
		pthread_mutex_t *mutexListasNodos) {

	pthread_mutex_lock(mutexListasNodos);

	int nombreNodoInt = atoi(nombreNodo);
	int nodo_fd;
	datosNodo* aux;

	bool ipYNombreBuscado(datosNodo * nodo) {
		return !strcmp(nodo->ip, ip) && (nodo->nombreNodo == nombreNodoInt);
	}

	if (list_any_satisfy(sistema, (void*) ipYNombreBuscado)) {
		datosNodo* aux = list_find(sistema, (void*) ipYNombreBuscado);
		if (aux->disponible) {
			aux->disponible = 0;
			eliminarSocketNodo(aux);
		}

		char* msjLog = malloc(
				strlen("Eliminado con exito el nodo con ip y puerto") + 1 + 16
						+ 6);
		strcpy(msjLog, "Eliminado con exito el nodo con ip ");
		strcat(msjLog, aux->ip);
		strcat(msjLog, " y puerto ");
		strcat(msjLog, aux->puerto_escucha);
		loguearme(PATH_LOG, "Proceso File System", 0, LOG_LEVEL_INFO, msjLog);
		free(msjLog);
		printf("Eliminado con �xito el nodo con ip %s y puerto %s\n", aux->ip,
				aux->puerto_escucha);
		pthread_mutex_unlock(mutexListasNodos);
		return;

	} else {
		loguearme(PATH_LOG, "Proceso File System", 0, 4,
				"El nodo a eliminar no existe.");
		printf("El nodo a eliminar no existe.");
		pthread_mutex_unlock(mutexListasNodos);
		return;
	}

}

//PERSISTENCIA

t_list* leerCopiasPersistidas(FILE* archivoArch, int num) {
	t_list* copias = list_create();

	void* aux = malloc(sizeof(int));
	void* auxip = malloc(16);
	void* auxp = malloc(6);
	infoCopia* ret;
	int i;
	for (i = 0; i < num; i++) {
		fread(auxip, 1, 16, archivoArch);
		ret = malloc(sizeof(infoCopia));
		ret->ip = malloc(16);
		strcpy(ret->ip, (char*) auxip);

		fread(auxp, 6, 1, archivoArch);
		ret->puerto = malloc(6);
		strcpy(ret->puerto, (char*) auxp);

		fread(aux, sizeof(int), 1, archivoArch);
		ret->nroBloque = *(int*) aux;

		fread(aux, sizeof(int), 1, archivoArch);
		ret->nombreNodo = *(int*) aux;

		list_add(copias, ret);

	}

	free(auxp);
	free(auxip);
	free(aux);
	return copias;
}

t_list* leerBloquesPersistidos(FILE* archivoArch, int num) {
	t_list* bloques = list_create();

	void* aux = malloc(sizeof(int));
	datoBloque* ret;
	int i = 0;
	for (i = 0; i < num; i++) {
		fread(aux, 1, 4, archivoArch);
		ret = malloc(sizeof(datoBloque));
		ret->nrobloque = *(int*) aux;
		fread(aux, 1, 4, archivoArch);
		ret->numeroDeCopias = *(int*) aux;
		ret->copias = leerCopiasPersistidas(archivoArch, ret->numeroDeCopias);
		list_add(bloques, ret);

	}
	free(aux);
	return bloques;
}

t_list* leerArchivosPersistidos() {

	t_list* archivos = list_create();

	if (access( PATH_ARCHIVOS, F_OK) != -1) {

		FILE* archivoArch = fopen(PATH_ARCHIVOS, "r");

		void* auxn = malloc(128);
		void* aux = malloc(sizeof(int));
		void* auxd = malloc(sizeof(int));
		archivo* ret;	//char* nombre;
						//double tamanioEnBytes;
						//int directorio;
						//int estado;

		while (fread(auxn, 1, 128, archivoArch) == 128) {
			ret = malloc(sizeof(archivo));
			ret->nombre = malloc(128);
			strcpy(ret->nombre, (char*) auxn);
			fread(auxd, sizeof(int), 1, archivoArch);
			ret->tamanioEnBytes = *(int*) auxd;
			fread(aux, sizeof(int), 1, archivoArch);
			ret->directorio = *(int*) aux;
			fread(aux, sizeof(int), 1, archivoArch);
			ret->estado = *(int*) aux;
			int num = ret->tamanioEnBytes / 20971520;
			if (ret->tamanioEnBytes % 20971520)
				num++;
			ret->listaBloques = leerBloquesPersistidos(archivoArch, num);
			list_add(archivos, ret);

		}

		free(aux);
		free(auxn);
		free(auxd);
		fclose(archivoArch);
		loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_INFO,
				"Cargados archivos persistidos");
		return archivos;

	} else {
		return archivos;
	}
}

void persistirUnNodo(datosNodo* nodo) {
	FILE* nodosArch = fopen(PATH_NODOS, "a");
	int* x = &(nodo->nombreNodo);
	fwrite((void*) x, sizeof(int), 1, nodosArch);
	fwrite((void*) nodo->ip, 16, 1, nodosArch);
	fwrite((void*) nodo->puerto, 6, 1, nodosArch);
	fwrite((void*) nodo->puerto_escucha, 6, 1, nodosArch);

	x = &(nodo->capacidadNodo);
	fwrite((void*) x, sizeof(int), 1, nodosArch);

	int num = nodo->capacidadNodo / 20971520;

	fwrite((void*) nodo->bloquesOcupados, 4 * num, 1, nodosArch);

//fwrite((void*) nodo->bloquesOcupados, sizeof(short), 50, nodosArch);

	int* y = &(nodo->disponible);
	fwrite((void*) y, sizeof(int), 1, nodosArch);
	fclose(nodosArch);
}

void persistirNodos(t_list* nodos) {
	pthread_mutex_lock(&mutexPersistencia);
	FILE* archivoNod = fopen(PATH_NODOS, "w");
	fflush(archivoNod);
	fclose(archivoNod);
	list_iterate(nodos, (void*) persistirUnNodo);
	pthread_mutex_unlock(&mutexPersistencia);
}

t_list* leerNodosPersistidos() {

	t_list* nodos = list_create();

	if (access( PATH_NODOS, F_OK) != -1) {

		FILE* archivoNod = fopen(PATH_NODOS, "r");

		void* auxIp = malloc(16);
		void* auxPuerto = malloc(6);
		void* auxPuertoE = malloc(6);
		void* aux = malloc(sizeof(int));
		void* auxSh = malloc(sizeof(int));

		//	short bloques[50];
		//  int i;
		datosNodo* ret;

		while (fread(aux, 1, 4, archivoNod) == 4) {
			ret = malloc(sizeof(datosNodo));
			ret->ip = malloc(16);
			ret->puerto = malloc(6);
			ret->puerto_escucha = malloc(6);
			ret->nombreNodo = *(int*) aux;
			fread(auxIp, 16, 1, archivoNod);
			strcpy(ret->ip, auxIp);
			fread(auxPuerto, 6, 1, archivoNod);
			strcpy(ret->puerto, auxPuerto);
			fread(auxPuertoE, 6, 1, archivoNod);
			strcpy(ret->puerto_escucha, auxPuertoE);
			fread(aux, 1, 4, archivoNod);
			ret->capacidadNodo = *(int*) aux;
			int num = ret->capacidadNodo / 20971520;
			//if (ret->capacidadNodo % 20971520)
			//num++;
			void* auxBloques = malloc(4 * num);
			ret->bloquesOcupados = malloc(4 * num);
			fread(auxBloques, 4 * num, 1, archivoNod);
			memcpy(ret->bloquesOcupados, auxBloques, 4 * num);

			//fread(bloques, sizeof(short), 50, archivoNod);
			//for (i = 0; i < 50; i++) {
			//ret->bloquesOcupados[i] = bloques[i];
			//}

			fread(auxSh, sizeof(int), 1, archivoNod);
			ret->disponible = *(int*) auxSh;

			ret->disponible = 0;
			list_add(nodos, ret);

			free(auxBloques);
		}

		free(auxIp);
		free(auxPuerto);
		free(auxPuertoE);

		free(aux);

		free(auxSh);

		fclose(archivoNod);
		loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_INFO,
				"Cargados nodos persistidos");

		return nodos;

	} else {

		return nodos;
	}
}

void persistirUnArchivo(archivo* arch) {
	FILE* archivoArch = fopen(PATH_ARCHIVOS, "a");

	void persistirUnBloque(datoBloque* bl) {

		void persistirUnaCopia(infoCopia* cp) {
			void* auxc;
			fwrite((void*) cp->ip, 16, 1, archivoArch);
			fwrite((void*) cp->puerto, 6, 1, archivoArch);
			auxc = &cp->nroBloque;
			fwrite(auxc, sizeof(int), 1, archivoArch);
			auxc = &cp->nombreNodo;
			fwrite(auxc, sizeof(int), 1, archivoArch);
		}

		void* auxb = &bl->nrobloque;
		fwrite(auxb, sizeof(int), 1, archivoArch);
		auxb = &bl->numeroDeCopias;
		fwrite(auxb, sizeof(int), 1, archivoArch);
		list_iterate(bl->copias, (void*) persistirUnaCopia);

	}

	void* aux;           //SI NO FUNCIONA CAMBIAMOS ESTO POR LO DE ABAJO
	fwrite((void*) arch->nombre, 128, 1, archivoArch);
	aux = &arch->tamanioEnBytes;
	fwrite(aux, sizeof(int), 1, archivoArch);
	aux = &arch->directorio;
	fwrite(aux, sizeof(int), 1, archivoArch);
	aux = &arch->estado;
	fwrite(aux, sizeof(int), 1, archivoArch);
	list_iterate(arch->listaBloques, (void*) persistirUnBloque);
	fclose(archivoArch);
}

void persistirArchivos(t_list* archivos) {
	pthread_mutex_lock(&mutexPersistencia);
	FILE* archivoArch = fopen(PATH_ARCHIVOS, "w");
	fflush(archivoArch);
	fclose(archivoArch);
	list_iterate(archivos, (void*) persistirUnArchivo);
	pthread_mutex_unlock(&mutexPersistencia);
}

void persistirUnDirectorio(directorio*dir) {
	int* x = &(dir->index);
	FILE* archivoDir = fopen(PATH_DIRECTORIOS, "a");
	fwrite((void*) x, sizeof(int), 1, archivoDir);
	x = &(dir->padre);
	fwrite((void*) x, sizeof(int), 1, archivoDir);
	fwrite((void*) dir->nombre, 129, 1, archivoDir);
	fclose(archivoDir);
}

t_list* leerDirectoriosPersistidos() {

	t_list* directorios = list_create();

	if (access( PATH_DIRECTORIOS, F_OK) != -1) {

		FILE* archivoDir = fopen(PATH_DIRECTORIOS, "r");

		void* auxn = malloc(129);
		void* aux = malloc(sizeof(int));
		directorio* ret;

		while (fread(aux, 1, 4, archivoDir) == 4) {
			ret = malloc(sizeof(directorio));
			ret->nombre = malloc(129);
			ret->index = *(int*) aux;
			fread(aux, sizeof(int), 1, archivoDir);
			ret->padre = *(int*) aux;
			fread(auxn, 129, 1, archivoDir);
			strcpy(ret->nombre, (char*) auxn);

			list_add(directorios, ret);

		}

		free(aux);
		free(auxn);

		fclose(archivoDir);
		loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_INFO,
				"Cargados directorios persistidos");

		return directorios;

	} else {

		directorio *root;

		root = malloc(sizeof(directorio));

		root->nombre = malloc(sizeof(char) * 4);
		strcpy(root->nombre, "root");
		root->index = 0;
		root->padre = 1024;

		list_add(directorios, root);

		return directorios;
	}

}

void persistirDirectorios(t_list * directorios) {
	pthread_mutex_lock(&mutexPersistencia);
	FILE* archivoDir = fopen(PATH_DIRECTORIOS, "w");
	fflush(archivoDir);
	fclose(archivoDir);
	list_iterate(directorios, (void*) persistirUnDirectorio);
	pthread_mutex_unlock(&mutexPersistencia);
}
//ARCHIVOS

void mostrarCopia(infoCopia* copia) {
	printf("\t\tIP: %s  ", copia->ip);
	printf("\t\tPuerto: %s  ", copia->puerto);
	printf("\t\tNumero de bloque: %d\n", copia->nroBloque);
	printf("\t\tNombre de Nodo: %d\n\n", copia->nombreNodo);
}

void mostrarCopias(t_list* copias) {
	list_iterate(copias, (void*) mostrarCopia);
}

void mostrarBloque(datoBloque* bloque) {
	printf("\n\tNumero de bloque: %d\n", bloque->nrobloque);
	printf("\n\tNumero de Copias: %d\n", bloque->numeroDeCopias);
	mostrarCopias(bloque->copias);
}

void mostrarBloques(t_list* bloques) {
	list_iterate(bloques, (void*) mostrarBloque);
}

void verBloques(char* directorio, char* nombreArchivo, t_list* archivos) {

	int directorioInt = atoi(directorio);

	bool existeEseArchivoEnEseDirectorio(archivo * arch) {
		return !strcmp(arch->nombre, nombreArchivo)
				&& directorioInt == arch->directorio;
	}

	if (list_any_satisfy(archivos, (void*) existeEseArchivoEnEseDirectorio)) {

		archivo* aux = list_find(archivos,
				(void*) existeEseArchivoEnEseDirectorio);

		mostrarBloques(aux->listaBloques);

	} else
		fprintf(stderr, "ERROR: no existe el archivo %s en el directorio %d\n",
				nombreArchivo, directorioInt);

}

void moverArchivo(char* directorioViejo, char* nombreArchivo,
		char* directorioNuevo, t_list* archivos, t_list* directorios) {
	int directorioViejoInt = atoi(directorioViejo);
	int directorioNuevoInt = atoi(directorioNuevo);

	bool existeEseArchivoEnEseDirectorio(archivo * arch) {
		return !strcmp(arch->nombre, nombreArchivo)
				&& directorioViejoInt == arch->directorio;
	}

	bool existeNombreNuevoEnDirectorio(archivo * arch) {
		return !strcmp(arch->nombre, nombreArchivo)
				&& directorioNuevoInt == arch->directorio;
	}

	bool existeDirectorioDestino(directorio* dir) {
		return dir->index == directorioNuevoInt;
	}

	if (list_any_satisfy(archivos, (void*) existeEseArchivoEnEseDirectorio)) {
		if (list_any_satisfy(directorios, (void*) existeDirectorioDestino)) {
			if (!list_any_satisfy(archivos,
					(void*) existeNombreNuevoEnDirectorio)) {
				archivo * aux = list_find(archivos,
						(void*) existeEseArchivoEnEseDirectorio); //aca se usa para encontrar el directorio buscado
				aux->directorio = directorioNuevoInt;
				printf(
						"Movido exitosamente el archivo %s del directorio %d al directorio%d\n",
						nombreArchivo, directorioViejoInt, directorioNuevoInt);
			} else
				fprintf(stderr,
						"ERROR: Ya existe un archivo con el nombre %s en el directorio %d\n",
						nombreArchivo, directorioNuevoInt);
		} else
			fprintf(stderr, "ERROR: no existe el directorio de destino %d\n",
					directorioNuevoInt);
	} else
		fprintf(stderr, "ERROR: no existe el archivo %s en el directorio %d\n",
				nombreArchivo, directorioViejoInt);

}

void renombrarArchivo(char* directorio, char* nombreViejo, char* nombreNuevo,
		t_list * archivos) {

	int directorioInt = atoi(directorio);

	bool existeEseArchivoEnEseDirectorio(archivo * arch) {
		return !strcmp(arch->nombre, nombreViejo)
				&& directorioInt == arch->directorio;
	}

	bool existeNombreNuevoEnDirectorio(archivo * arch) {
		return !strcmp(arch->nombre, nombreNuevo)
				&& directorioInt == arch->directorio;
	}

	if (list_any_satisfy(archivos, (void*) existeEseArchivoEnEseDirectorio)) {

		if (!list_any_satisfy(archivos,
				(void*) existeNombreNuevoEnDirectorio)) {
			archivo * aux = list_find(archivos,
					(void*) existeEseArchivoEnEseDirectorio);
			strcpy(aux->nombre, nombreNuevo);
			printf("Renombrado exitosamente el archivo %s a %s \n", nombreViejo,
					nombreNuevo);
		} else
			fprintf(stderr,
					"ERROR: Ya existe un archivo con el nombre %s en el directorio %d\n",
					nombreNuevo, directorioInt);

	} else
		fprintf(stderr, "ERROR: no existe el archivo %s en el directorio %d\n",
				nombreViejo, directorioInt);

}

void destruirCopia(infoCopia* copia) {
	free(copia->ip);
	//free(copia->puerto_escucha);
	free(copia->puerto);
	free(copia);
}

void destruirBloque(datoBloque* bloque) {
	list_destroy_and_destroy_elements(bloque->copias, (void*) destruirCopia);
	free(bloque);
}

void destruirArchivo(archivo* archivo) {
	free(archivo->nombre);
	list_destroy_and_destroy_elements(archivo->listaBloques,
			(void*) destruirBloque);
	free(archivo);
}

void borrarBloquesDeNodosDeArchivo(archivo* arch, t_list * nodos) {

	void borrarCopiasDeNodo(infoCopia* copia) {

		bool ipYPuertoBuscado(datosNodo * nodo) {
			return nodo->nombreNodo == copia->nombreNodo;
		}

		datosNodo* aux = list_find(nodos, (void*) ipYPuertoBuscado);
		aux->bloquesOcupados[copia->nroBloque] = 0;
	}

	void borrarBloquesDeNodo(datoBloque* bloque) {
		list_iterate(bloque->copias, (void*) borrarCopiasDeNodo);
	}

	list_iterate(arch->listaBloques, (void*) borrarBloquesDeNodo);
}

void borrarArchivo(char* directorio, char* nombreArchivo, t_list * archivos,
		t_list * nodos) {

	int directorioInt = atoi(directorio);

	char* nombreArchivoAux = malloc(strlen(nombreArchivo) + 1);
	strcpy(nombreArchivoAux, nombreArchivo);

	bool esElArchivoBuscado(archivo* archivo) {
		return (!strcmp(archivo->nombre, nombreArchivoAux))
				&& archivo->directorio == directorioInt;
	}
	if (list_any_satisfy(archivos, (void*) esElArchivoBuscado)) {

		borrarBloquesDeNodosDeArchivo(
				(list_find(archivos, (void*) esElArchivoBuscado)), nodos);

		list_remove_and_destroy_by_condition(archivos,
				(void*) esElArchivoBuscado, (void*) destruirArchivo);
		printf("Se ha borrado correctamente el archivo %s del directorio %d\n",
				nombreArchivoAux, directorioInt);
	} else
		fprintf(stderr, "ERROR:No existe el archivo %s en el directorio %d\n",
				nombreArchivoAux, directorioInt);

}

void mostrarArchivos(t_list* archivos, t_list* nodosSistema) {
	void mostrarArchivo(archivo* archivo) {
		archivo->estado=1;

		if (!archivoEstaDisponible(archivo, nodosSistema)) {
			archivo->estado = 0;
		}
		printf("//////////////////////////////////////////////////\n");
		printf("Nombre de archivo: %s\n", archivo->nombre);
		printf("Tamaño: %d KB\n", archivo->tamanioEnBytes / 1024);
		printf("Directorio: %d\n", archivo->directorio);
		printf("Estado: %d\n", archivo->estado);
		//mostrarBloques(archivo->listaBloques);
		printf("//////////////////////////////////////////////////\n");
	}
	if (!list_is_empty(archivos)) {
		list_iterate(archivos, (void*) mostrarArchivo);
	} else
		fprintf(stderr, "ERROR: No hay archivos para mostrar\n");

}

//ARCHIVO CONFIG

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

		tam = string_length(config_get_string_value(archivo, "PUERTO_LISTEN"));
		a->puertoListen = malloc(tam + 1);
		strcpy(a->puertoListen,
				config_get_string_value(archivo, "PUERTO_LISTEN"));

		a->nodos_minimo = config_get_int_value(archivo, "CANTIDAD_NODOS");

		tam = string_length(config_get_string_value(archivo, "IP_MARTA"));
		a->ipMarta = malloc(tam + 1);
		strcpy(a->ipMarta, config_get_string_value(archivo, "IP_MARTA"));

		tam = string_length(
				config_get_string_value(archivo, "PUERTO_M_PRINCIPAL"));
		a->puertoMPrincipal = malloc(tam + 1);
		strcpy(a->puertoMPrincipal,
				config_get_string_value(archivo, "PUERTO_M_PRINCIPAL"));

		tam = string_length(
				config_get_string_value(archivo, "PUERTO_M_ACTUALIZ"));
		a->puertoMAct = malloc(tam + 1);
		strcpy(a->puertoMAct,
				config_get_string_value(archivo, "PUERTO_M_ACTUALIZ"));

		loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_INFO,
				"Leido el archivo de configuracion.");
		config_destroy(archivo);
		return a;
	} else {
		loguearme(PATH_LOG, "Proceso FileSystem", 0, LOG_LEVEL_ERROR,
				"No existe el archivo de configuracion.");
	}

	return NULL;
}

void liberarLectura(lectura* leido) {

	free(leido->puertoListen);
	free(leido->ipMarta);
	free(leido->puertoMAct);
	free(leido->puertoMPrincipal);
}

//DIRECTORIOS

int indexDisponible(t_list * dir) {
	int size = list_size(dir);
	directorio * direc;
	direc = list_get(dir, size - 1);
	return (direc->index + 1);
}

void mostrarDirectorio(directorio * dir) {
	printf("--------\n");
	printf("Nombre de directorio: %s\n", dir->nombre);
	printf("Index Nro: %d\n", dir->index);
	printf("Directorio padre: %d\n", dir->padre);
	printf("--------\n");

}

void destruirDirectorio(directorio* dir) {
	free(dir->nombre);
	free(dir);
}

void crearDirectorio(char* padre, char* nombre, t_list * dirs) {

	int padreInt = atoi(padre);
	directorio *dir1 = malloc(sizeof(directorio));

	bool existeDirectorioPadre(directorio *dir) {
		return (padreInt == dir->index);
	}

	bool noExisteDirectorioConMismoNombre(directorio *dir) {
		return (!strcmp(nombre, dir->nombre) && (padreInt == dir->padre));
	}
	if (list_size(dirs) <= 1024) {
		if (list_any_satisfy(dirs, (void*) existeDirectorioPadre)) {
			if (!list_any_satisfy(dirs,
					(void*) noExisteDirectorioConMismoNombre)) {
				dir1->index = indexDisponible(dirs);
				dir1->padre = padreInt;
				dir1->nombre = malloc(128);
				strcpy(dir1->nombre, nombre);
				list_add(dirs, dir1);

				printf("Directorio %s con indice %d creado exitosamente\n",
						nombre, dir1->index);

			} else
				fprintf(stderr,
						"ERROR: ya existe el directorio %s en directorio padre %d\n",
						nombre, padreInt);
		} else
			fprintf(stderr, "ERROR: no existe el directorio padre %d\n",
					padreInt);
	} else
		fprintf(stderr,
				"ERROR: ya existen 1024 directorios, no se pueden crear mas");

}

void moverDirectorio(char* padreViejo, char* padreNuevo, char* nombreDirectorio,
		t_list * directorios) {

	int padreNuevoInt = atoi(padreNuevo);
	int padreViejoInt = atoi(padreViejo);

	bool existeEseDirectorioEnEsePadre(directorio * dir) {
		return !strcmp(dir->nombre, nombreDirectorio)
				&& padreViejoInt == dir->padre;
	}

	bool existeNombreEnPadreNuevo(directorio * dir) {
		return !strcmp(dir->nombre, nombreDirectorio)
				&& padreNuevoInt == dir->padre;
	}

	if (list_any_satisfy(directorios, (void*) existeEseDirectorioEnEsePadre)) {

		if (!list_any_satisfy(directorios, (void*) existeNombreEnPadreNuevo)) {
			directorio * aux = list_find(directorios,
					(void*) existeEseDirectorioEnEsePadre); //aca se usa para encontrar el directorio buscado
			aux->padre = padreNuevoInt;
			printf(
					"Movido exitosamente directorio %s del directorio padre %d al directorio padre %d\n",
					nombreDirectorio, padreViejoInt, padreNuevoInt);
		} else
			fprintf(stderr,
					"ERROR: Ya existe el directorio %s en el directorio padre %d\n",
					nombreDirectorio, padreNuevoInt);

	} else
		fprintf(stderr,
				"ERROR: no existe directorio %s en el directorio padre %d\n",
				nombreDirectorio, padreViejoInt);

}
void borrarDirectorioConPadre(int padreInt, t_list* directorios,
		t_list* archivos, t_list* nodos) {

	bool tieneElPadre(directorio* dir) {
		return dir->padre == padreInt;
	}

	directorio* dire;

	void borrarArchivoEnDirectorio(archivo * arch) {

		if (arch->directorio == dire->index)
			borrarArchivo(string_itoa(arch->directorio), arch->nombre, archivos,
					nodos);

	}

	while (list_any_satisfy(directorios, (void*) tieneElPadre)) {
		dire = list_find(directorios, (void*) tieneElPadre);

		list_iterate(archivos, (void*) borrarArchivoEnDirectorio);
		borrarDirectorioConPadre(dire->index, directorios, archivos, nodos);

		list_remove_and_destroy_by_condition(directorios, (void*) tieneElPadre,
				(void*) destruirDirectorio);
	}

}
void borrarDirectorio(char* padre, char* nombreDirectorio, t_list * directorios,
		t_list * archivos, t_list * nodos) {
	int padreInt = atoi(padre);
	directorio* dir;

	bool existeEseDirectorioEnEsePadre(directorio * dir) {
		return !strcmp(dir->nombre, nombreDirectorio) && padreInt == dir->padre;
	}

	void borrarArchivoEnDirectorio(archivo * arch) {

		if (arch->directorio == dir->index)
			borrarArchivo(string_itoa(arch->directorio), arch->nombre, archivos,
					nodos);

	}

	if (list_any_satisfy(directorios, (void*) existeEseDirectorioEnEsePadre)) {
		dir = list_find(directorios, (void*) existeEseDirectorioEnEsePadre);

		list_iterate(archivos, (void*) borrarArchivoEnDirectorio);

		borrarDirectorioConPadre(dir->index, directorios, archivos, nodos);

		list_remove_and_destroy_by_condition(directorios,
				(void*) existeEseDirectorioEnEsePadre,
				(void*) destruirDirectorio);

		printf(
				"Se ha borrado con exito el directorio %s ubicado en el directorio padre %d y todos sus descendientes\n",
				nombreDirectorio, padreInt);
	} else
		fprintf(stderr,
				"ERROR: no existe directorio %s en el directorio padre %d\n",
				nombreDirectorio, padreInt);

}

void mostrarDirectorios(t_list * directorios) {

	list_iterate(directorios, (void*) mostrarDirectorio);

}

void renombrarDirectorio(char* padre, char* nombreViejo, char* nombreNuevo,
		t_list* directorios) {
	int padreInt = atoi(padre);

	bool existeEseDirectorioEnEsePadre(directorio * dir) {
		return !strcmp(dir->nombre, nombreViejo) && padreInt == dir->padre;
	}

	bool existeNombreNuevoEnPadre(directorio * dir) {
		return !strcmp(dir->nombre, nombreNuevo) && padreInt == dir->padre;
	}

	if (list_any_satisfy(directorios, (void*) existeEseDirectorioEnEsePadre)) {

		if (!list_any_satisfy(directorios, (void*) existeNombreNuevoEnPadre)) {
			directorio * aux = list_find(directorios,
					(void*) existeEseDirectorioEnEsePadre); //aca se usa para encontrar el directorio buscado
			strcpy(aux->nombre, nombreNuevo);
			printf(
					"Renombrado exitosamente directorio %s como directorio %s \n",
					nombreViejo, nombreNuevo);
		} else
			fprintf(stderr,
					"ERROR: Ya existe el directorio %s en el directorio padre %d\n",
					nombreNuevo, padreInt);

	} else
		fprintf(stderr,
				"ERROR: no existe directorio %s en el directorio padre %d\n",
				nombreViejo, padreInt);

}

int primerBloqueLibre(datosNodo* nodo) {
	int i;

	for (i = 0; i < ((nodo->capacidadNodo) / (1024 * 1024 * 20)); i++) {
		if (!nodo->bloquesOcupados[i]) {
			return i;
		}
	}

	return -1;

}

int tieneEspacioDisponible(datosNodo* nodo) {
	int i;
	int bloquesLibresDis = 0;

	if (nodo->disponible) {
		for (i = 0; i < ((nodo->capacidadNodo) / (1024 * 1024 * 20)); i++) {
			if (!nodo->bloquesOcupados[i]) {
				bloquesLibresDis++;
			}
		}

		if (bloquesLibresDis > 0)
			return 1;
		return 0;
	}
	return 0;
}
void copiarBloque(char* nombreArchivo, char* directorioArchivo,
		char* numeroBloque, char* nombreNodo, t_list* archivos, t_list* nodosEnSistema,
		char* puerto_escucha, pthread_mutex_t* mutexListasNodos) {

	pthread_mutex_lock(mutexListasNodos);

	int directorioArchivoInt = atoi(directorioArchivo);

	bool archivoBuscado(archivo* arch) {
		return arch->directorio == directorioArchivoInt
				&& !strcmp(nombreArchivo, arch->nombre);
	}

	archivo* archivoBuscadoParaHacerLaMagia = list_find(archivos,
			(void*) archivoBuscado);
	if (archivoBuscadoParaHacerLaMagia) {

		int numeroBloqueInt = atoi(numeroBloque);

		bool esElBloqueBuscado(datoBloque* block) {
			return block->nrobloque == numeroBloqueInt;
		}

		datoBloque* bloqueBuscado = list_find(
				archivoBuscadoParaHacerLaMagia->listaBloques,
				(void*) esElBloqueBuscado);
		if (!bloqueBuscado) {
			loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
					"No existe el bloque buscado");
			pthread_mutex_unlock(mutexListasNodos);
			return;
		}

		bool estaDisponible(infoCopia* copyAux) {
			return copiaDisponible(copyAux, nodosEnSistema);
		}

		infoCopia* copy = list_find(bloqueBuscado->copias,
				(void*) estaDisponible);

		bloque *block = malloc(sizeof(bloque)); // para recibir los datos
		int accion;
		int fdNodo;

		bool obtenerNodo(datosNodo* nodo) {
			return copy->nombreNodo == nodo->nombreNodo;
		}

		datosNodo* nodoBuscado = list_find(nodosEnSistema, (void*) obtenerNodo);

		bloqueMapeado* datos;

		char * bloque_con_datos = malloc(TAMANIO_BLOQUE);

		if ((fdNodo = crearConexion(nodoBuscado->ip,
				nodoBuscado->puerto_escucha, puerto_escucha))) {

			bloque* bloqueAEnviar;

			bloqueAEnviar = serializarSolicitarBloqueArchivo(copy->nroBloque);

			if (enviarDatosParaObtenerBloque(fdNodo, bloqueAEnviar)) {

				if ((accion = receive_and_unpack(block, fdNodo)) > 0) {
					datos = deserializarBloqueArchivoSolicitado(block);
					memcpy(bloque_con_datos, datos->bloque,
							datos->tamanio_bloque);

				}

			}
			close(fdNodo);

		}

//		bool nodoAMandarCopia(datosNodo* nodi) {
//
//			bool copiaEnNodo(infoCopia* copyAuxx) {
//				return copyAuxx->nombreNodo != nodi->nombreNodo;
//			}
//
//			return list_all_satisfy(bloqueBuscado->copias, (void*) copiaEnNodo)
//					&& tieneEspacioDisponible(nodi);
//
//		}
//
//		datosNodo* nodoAuxx = list_find(nodosEnSistema,
//				(void*) nodoAMandarCopia);
//
//		if (!nodoAuxx) {
//			loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
//					"No hay ningun nodo al cual pueda enviarsele una copia de ese bloque sin que se repitan copias en un mismo nodo");
//			pthread_mutex_unlock(mutexListasNodos);
//			return;
//		}

		int nombreNodoInt = atoi(nombreNodo);

		bool nodoConMismoNombre(datosNodo* nodito){
			return nodito->nombreNodo == nombreNodoInt;
		}

		datosNodo* nodoAuxx = list_find(nodosEnSistema, (void*) nodoConMismoNombre);

		if(!nodoAuxx){
			loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
								"Ese Nodo no existe en el sistema");
						pthread_mutex_unlock(mutexListasNodos);
						return;
		}

		if(!nodoAuxx->disponible){
			loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
											"Ese Nodo no esta disponible actualmente");
									pthread_mutex_unlock(mutexListasNodos);
									return;
		}

		if(!tieneEspacioDisponible(nodoAuxx)){
					loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
													"Ese Nodo no tiene espacio suficiente");
											pthread_mutex_unlock(mutexListasNodos);
											return;
				}


		bool noEstaEnEseNodo(infoCopia* copyy){
			return copyy->nombreNodo != nombreNodoInt;
		}

		if(!list_all_satisfy(bloqueBuscado->copias, (void*) noEstaEnEseNodo)){
			loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
																"Ese Nodo ya posee una copia de ese bloque");
														pthread_mutex_unlock(mutexListasNodos);
														return;
		}

		int bloqueLibreDeNodo = primerBloqueLibre(nodoAuxx);

		bloque* bloqueAEnviar2;

		armarBloque(bloque_con_datos, TAMANIO_BLOQUE);

		bloqueAEnviar2 = serializarBloqueNodo(bloque_con_datos,
				bloqueLibreDeNodo);

		enviarDatos(nodoAuxx->ip, nodoAuxx->puerto, bloqueAEnviar2);

		nodoAuxx->bloquesOcupados[bloqueLibreDeNodo] = 1;

		infoCopia* copiaNueva = malloc(sizeof(infoCopia));
		copiaNueva->ip = malloc(16);
		copiaNueva->puerto = malloc(6);
		copiaNueva->puerto_escucha = malloc(6);
		strcpy(copiaNueva->ip, nodoAuxx->ip);
		strcpy(copiaNueva->puerto, nodoAuxx->puerto);
		strcpy(copiaNueva->puerto_escucha, nodoAuxx->puerto_escucha);
		copiaNueva->nombreNodo = nodoAuxx->nombreNodo;
		copiaNueva->nroBloque = bloqueLibreDeNodo;

		list_add(bloqueBuscado->copias, copiaNueva);

		bloqueBuscado->numeroDeCopias++;

		loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_INFO,
				"Bloque copiado con éxito");
		pthread_mutex_unlock(mutexListasNodos);
		return;

	} else {
		loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
				"No existe el archivo buscado");
		pthread_mutex_unlock(mutexListasNodos);
		return;
	}
}

void eliminarBloque(char* nombreArchivo, char* directorioArchivo,
		char* numeroBloque, char* nombreNodo, t_list* archivos,
		t_list* nodosEnSistema, pthread_mutex_t* mutexListasNodos) {

	pthread_mutex_lock(mutexListasNodos);

	int directorioArchivoInt = atoi(directorioArchivo);

	bool archivoBuscado(archivo* arch) {
		return arch->directorio == directorioArchivoInt
				&& !strcmp(nombreArchivo, arch->nombre);
	}

	archivo* archivoBuscadoParaHacerLaMagia = list_find(archivos,
			(void*) archivoBuscado);

	if (archivoBuscadoParaHacerLaMagia) {

		int numeroBloqueInt = atoi(numeroBloque);

		bool esElBloqueBuscado(datoBloque* block) {
			return block->nrobloque == numeroBloqueInt;
		}

		datoBloque* bloqueBuscado = list_find(
				archivoBuscadoParaHacerLaMagia->listaBloques,
				(void*) esElBloqueBuscado);
		if (!bloqueBuscado) {
			loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
					"No existe el bloque buscado");
			pthread_mutex_unlock(mutexListasNodos);
			return;
		}

		if (list_size(bloqueBuscado->copias) <= 3) {
			loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
					"El bloque buscado no tiene mas de 3 copias. No se puede eliminar ninguna");
			pthread_mutex_unlock(mutexListasNodos);
			return;
		}

		int nombreNodoInt = atoi(nombreNodo);

		bool copiaDeNodo(infoCopia* copyAux) {
			return copyAux->nombreNodo == nombreNodoInt;
		}

		infoCopia* copy = list_remove_by_condition(bloqueBuscado->copias, (void*) copiaDeNodo);

		if (!copy) {
			loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
					"No existe ese bloque en ese nodo");
			pthread_mutex_unlock(mutexListasNodos);
			return;
		}

		bool nodoConNombreIgual(datosNodo* nodin){
			return nodin->nombreNodo == nombreNodoInt;
		}

		datosNodo* nodoAux = list_find(nodosEnSistema, (void*) nodoConNombreIgual);

		nodoAux->bloquesOcupados[copy->nroBloque] = 0;

		destruirCopia(copy);

		bloqueBuscado->numeroDeCopias--;

		loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_INFO,
							"Bloque eliminado de ese nodo con éxito");
					pthread_mutex_unlock(mutexListasNodos);
					return;

	} else {
		loguearme(PATH_LOG, "Proceso FileSystem", 1, LOG_LEVEL_WARNING,
				"No existe el archivo buscado");
		pthread_mutex_unlock(mutexListasNodos);
		return;
	}
}
