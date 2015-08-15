/*
 * hSerializadores.c
 *
 *  Created on: 7/6/2015
 *      Author: utnso
 */

#include "hSerializadores.h"

paquete *pack(bloque *block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	paquete *pack = malloc(sizeof(paquete));

	void *bloque = malloc(
			sizeof(block->longitud_datos) + block->longitud_datos);

	memcpy(bloque, &block->longitud_datos,
			tamanio_aux = sizeof(block->longitud_datos));
	desplazamiento = tamanio_aux;

	memcpy(bloque + desplazamiento, block->datos,
			tamanio_aux = block->longitud_datos);
	desplazamiento += tamanio_aux;

	pack->longitud_bloque = desplazamiento;
	pack->bloque = bloque;

	free(block->datos);
	free(block);

	return pack;

}

archivo* deserializarBloquesParaMarta(bloque *block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	archivo* archivoPedido = malloc(sizeof(archivo));

	t_list* listaBloques = list_create();

	infoCopia *obtenerCopia() {

		infoCopia *info = malloc(sizeof(infoCopia));

		uint32_t longitud_string;

		memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
				sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		info->ip = malloc(longitud_string);
		memcpy(info->ip, (block->datos) + desplazamiento, tamanio_aux =
				longitud_string);
		desplazamiento += tamanio_aux;

		memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
				sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		info->puerto = malloc(longitud_string);
		memcpy(info->puerto, (block->datos) + desplazamiento, tamanio_aux =
				longitud_string);
		desplazamiento += tamanio_aux;

		memcpy(&info->nroBloque, (block->datos) + desplazamiento, tamanio_aux =
				sizeof(info->nroBloque));
		desplazamiento += tamanio_aux;

		memcpy(&info->nombreNodo, (block->datos) + desplazamiento, tamanio_aux =
				sizeof(info->nombreNodo));
		desplazamiento += tamanio_aux;

		return info;

	}

	datoBloque *obtenerBloque() {

		datoBloque *data = malloc(sizeof(datoBloque));

		data->copias = list_create();

		memcpy(&data->nrobloque, (block->datos) + desplazamiento, tamanio_aux =
				sizeof(data->nrobloque));
		desplazamiento += tamanio_aux;

		data->numeroDeCopias = 0;
		memcpy(&data->numeroDeCopias, (block->datos) + desplazamiento,
				tamanio_aux = sizeof(int));
		desplazamiento += tamanio_aux;

		int x;
		for (x = 0; x < data->numeroDeCopias; x++) {
			infoCopia *info = obtenerCopia();
			list_add(data->copias, info);
		}

		return data;

	}

	/*----------------------------------*/

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	int cantidadDeBloques = 0;
	memcpy(&cantidadDeBloques, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(int));
	desplazamiento += tamanio_aux;

	int x;
	for (x = 0; x < cantidadDeBloques; x++) {
		datoBloque *dato_bloque = obtenerBloque();
		list_add(listaBloques, dato_bloque);
	}

	archivoPedido->ipJob = malloc(16);
	memcpy(archivoPedido->ipJob, (block->datos) + desplazamiento, tamanio_aux =
			16);
	desplazamiento += tamanio_aux;

	archivoPedido->puertoJob = malloc(6);
	memcpy(archivoPedido->puertoJob, (block->datos) + desplazamiento,
			tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	uint32_t longitud_string;
	memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	archivoPedido->direccionArchivo = malloc(longitud_string);
	memcpy(archivoPedido->direccionArchivo, (block->datos) + desplazamiento,
			tamanio_aux = longitud_string);
	desplazamiento += tamanio_aux;

	archivoPedido->listaBloques = listaBloques;

	free(block->datos);
	free(block);

	return archivoPedido;

}

caracteristicasJob* deserializarPedirTrabajoDeArchivoAMarta(bloque *block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	int longitud_string;

	caracteristicasJob* datos_job = malloc(sizeof(caracteristicasJob));
	datos_job->archivosTrabajo = list_create();

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&datos_job->combiner, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(int));
	desplazamiento += tamanio_aux;

	char* obtenerArchivo() {
		memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
				sizeof(int));
		desplazamiento += tamanio_aux;

		char* archivo = malloc(longitud_string);
		memcpy(archivo, (block->datos) + desplazamiento, tamanio_aux =
				longitud_string);
		desplazamiento += tamanio_aux;

		return archivo;

	}

	int cantidadDeArchivos = 0;
	memcpy(&cantidadDeArchivos, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(int));
	desplazamiento += tamanio_aux;

	int x;
	for (x = 0; x < cantidadDeArchivos; x++) {
		char* archivoTrabajo = obtenerArchivo();
		list_add(datos_job->archivosTrabajo, archivoTrabajo);
	}

	free(block->datos);
	free(block);

	return datos_job;

}

bloque* serializarMapJob(t_list* instruccionesMap, char* archivoTrabajo) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 1;

	tamanioBloqueAMandar = (sizeof(uint32_t) + sizeof(uint32_t)
			+ sizeof(uint32_t) + strlen(archivoTrabajo) + 1
			+ (list_size(instruccionesMap) * (16 + 6 + sizeof(int)))); // guarda la accion, la cantidad de bloques de la lista y la lista

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	uint32_t longitud_string = strlen(archivoTrabajo) + 1;
	memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, archivoTrabajo, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	uint32_t tamanio_lista = list_size(instruccionesMap);
	memcpy(datos + desplazamiento, &tamanio_lista, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	void almacenarBloque(instruccionMapReduce* instruccionMap) {
		memcpy(datos + desplazamiento, instruccionMap->ipNodo, tamanio_aux =
				16);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, instruccionMap->puertoNodo, tamanio_aux =
				6);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, &instruccionMap->bloqueNodo,
				tamanio_aux = sizeof(uint32_t));
		desplazamiento += tamanio_aux;
	}

	list_iterate(instruccionesMap, (void*) almacenarBloque);

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarMapJobReplan(instruccionMapReduce* nuevaInstruccionMap,
		char* archivoTrabajo) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 2;

	tamanioBloqueAMandar = (sizeof(uint32_t) + sizeof(uint32_t)
			+ strlen(archivoTrabajo) + 1 + (16 + 6 + sizeof(int)));

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	uint32_t longitud_string = strlen(archivoTrabajo) + 1;
	memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, archivoTrabajo, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, nuevaInstruccionMap->ipNodo, tamanio_aux =
			16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, nuevaInstruccionMap->puertoNodo,
			tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, &nuevaInstruccionMap->bloqueNodo,
			tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

void deserializarActualizacionNodosDisponibles(bloque* block,
		t_list* listaNodosEnElSitema, fileSystem* file_system) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&file_system->nodosMinimo, (block->datos) + desplazamiento,
			tamanio_aux = sizeof(int));
	desplazamiento += tamanio_aux;

	uint32_t cantidadDeNodos = 0;
	memcpy(&cantidadDeNodos, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	datosNodo* obtenerNodo() {
		datosNodo* nodo = malloc(sizeof(datosNodo));

		memcpy(&nodo->nombreNodo, (block->datos) + desplazamiento, tamanio_aux =
				sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		nodo->ip = malloc(16);
		memcpy(nodo->ip, (block->datos) + desplazamiento, tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		nodo->puerto = malloc(6);
		memcpy(nodo->puerto, (block->datos) + desplazamiento, tamanio_aux = 6);
		desplazamiento += tamanio_aux;

		memcpy(&nodo->disponible, (block->datos) + desplazamiento, tamanio_aux =
				sizeof(int));
		desplazamiento += tamanio_aux;

		return nodo;
	}

	int x;
	datosNodo* nodo;
	for (x = 0; x < cantidadDeNodos; x++) {
		nodo = obtenerNodo();
		list_add(listaNodosEnElSitema, nodo);
	}

	free(block->datos);
	free(block);

}

replanificacionMap* deserializarResultadoMapMarta(bloque* block) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	replanificacionMap* resultadoMap = malloc(sizeof(replanificacionMap));

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&resultadoMap->resultado, (block->datos) + desplazamiento,
			tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	resultadoMap->ipNodo = malloc(16);
	memcpy(resultadoMap->ipNodo, (block->datos) + desplazamiento, tamanio_aux =
			16);
	desplazamiento += tamanio_aux;

	resultadoMap->puertoNodo = malloc(6);
	memcpy(resultadoMap->puertoNodo, (block->datos) + desplazamiento,
			tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	memcpy(&resultadoMap->bloqueNodo, (block->datos) + desplazamiento,
			tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	uint32_t longitud_string;
	memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	resultadoMap->archivoTrabajo = malloc(longitud_string);
	memcpy(resultadoMap->archivoTrabajo, (block->datos) + desplazamiento,
			tamanio_aux = longitud_string);
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return resultadoMap;
}

bloque* serializarPedidoBloquesDeArchivo(job* datos_job) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 2;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ strlen(datos_job->direccionArchivo) + 1 + 16 + 6;

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	uint32_t tamanio_string = strlen(datos_job->direccionArchivo) + 1;
	memcpy(datos + desplazamiento, &tamanio_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, datos_job->direccionArchivo, tamanio_aux =
			tamanio_string);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, datos_job->ip, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, datos_job->puerto, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarReduceJobSinCombiner(char* ipNodoPrincipal,
		char* puertoNodoPrincipal, t_list* instrucciones, char* archivoTrabajo) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 3;

	tamanioBloqueAMandar = (sizeof(uint32_t) + sizeof(uint32_t)
			+ strlen(archivoTrabajo) + 1 + 16 + 6 + sizeof(uint32_t)
			+ ((list_size(instrucciones)) * (16 + 6 + sizeof(int))));

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	uint32_t longitud_string = strlen(archivoTrabajo) + 1;
	memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, archivoTrabajo, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, ipNodoPrincipal, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, puertoNodoPrincipal, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	uint32_t tamanio_lista = list_size(instrucciones);
	memcpy(datos + desplazamiento, &tamanio_lista, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	void almacenarInstruccion(instruccionMapReduce* instruccion) {
		memcpy(datos + desplazamiento, instruccion->ipNodo, tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, instruccion->puertoNodo, tamanio_aux =
				6);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, &instruccion->bloqueNodo, tamanio_aux =
				sizeof(uint32_t));
		desplazamiento += tamanio_aux;
	}

	list_iterate(instrucciones, (void*) almacenarInstruccion);

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

bloque* serializarReduceParcialJobConCombiner(char* ipNodo, char* puertoNodo,
		int* bloques, int tamanioBloque, char* archivoTrabajo) {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 4;

	tamanioBloqueAMandar = (sizeof(uint32_t) + sizeof(uint32_t)
			+ strlen(archivoTrabajo) + 1 + 16 + 6 + sizeof(uint32_t)
			+ (tamanioBloque * sizeof(int)));

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	uint32_t longitud_string = strlen(archivoTrabajo) + 1;
	memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, archivoTrabajo, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, ipNodo, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, puertoNodo, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, &tamanioBloque, tamanio_aux = sizeof(int));
	desplazamiento += tamanio_aux;

	int x;
	for (x = 0; x < tamanioBloque; x++) {
		memcpy(datos + desplazamiento, &bloques[x], tamanio_aux = sizeof(int));
		desplazamiento += tamanio_aux;
	}

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarReduceFinalJobConCombiner(char* ipNodoPrincipal,
		char* puertoNodoPrincipal, t_list* instrucciones, char* archivoTrabajo) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 5;

	tamanioBloqueAMandar = (sizeof(uint32_t) + sizeof(uint32_t)
			+ strlen(archivoTrabajo) + 1 + 16 + 6 + sizeof(uint32_t)
			+ ((list_size(instrucciones)) * (16 + 6)));

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	uint32_t longitud_string = strlen(archivoTrabajo) + 1;
	memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, archivoTrabajo, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, ipNodoPrincipal, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, puertoNodoPrincipal, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	uint32_t tamanio_lista = list_size(instrucciones);
	memcpy(datos + desplazamiento, &tamanio_lista, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	void almacenarInstruccion(reducesParcialesParaJob* instruccion) {
		memcpy(datos + desplazamiento, instruccion->ipNodo, tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, instruccion->puertoNodo, tamanio_aux =
				6);
		desplazamiento += tamanio_aux;
	}

	list_iterate(instrucciones, (void*) almacenarInstruccion);

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

bloque* serializarFileSystemNoDisponible() {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 8;

	tamanioBloqueAMandar = sizeof(uint32_t);

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarFileSystemNoConectado() {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 12;

	tamanioBloqueAMandar = sizeof(uint32_t);

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarNoExisteArchivoPedido(char* archivo_buscado) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 7;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ strlen(archivo_buscado) + 1;

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	uint32_t longitud_string = strlen(archivo_buscado) + 1;
	memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, archivo_buscado, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

void deserializarArchivoPedidoNoExiste(bloque* block, t_list* jobs) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	int fdJob;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	uint32_t longitud_string;
	memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	char* archivo_buscado = malloc(longitud_string);
	memcpy(archivo_buscado, (block->datos) + desplazamiento, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	char* ipJob = malloc(16);
	memcpy(ipJob, (block->datos) + desplazamiento, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	char* puertoJob = malloc(6);
	memcpy(puertoJob, (block->datos) + desplazamiento, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	fdJob = obtenerDescriptorDeFichero(ipJob, puertoJob);

	avisarNoExisteArchivoPedidoJob(fdJob, jobs, archivo_buscado);

}

int deserializarResultadoReduceSinCombinerMarta(bloque* block) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	int resultado;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&resultado, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	return resultado;
}

t_list* deserializarResultadoReduceParcial(bloque* block) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	t_list* listaResultadoReduceParcial = list_create();

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	uint32_t tamanio_lista;
	memcpy(&tamanio_lista, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	replanificacionMap* obtenerInstruccionReduce() {

		replanificacionMap* instruccionReduce = malloc(
				sizeof(replanificacionMap));

		instruccionReduce->ipNodo = malloc(16);
		memcpy(instruccionReduce->ipNodo, (block->datos) + desplazamiento,
				tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		instruccionReduce->puertoNodo = malloc(6);
		memcpy(instruccionReduce->puertoNodo, (block->datos) + desplazamiento,
				tamanio_aux = 6);
		desplazamiento += tamanio_aux;

		memcpy(&instruccionReduce->bloqueNodo, (block->datos) + desplazamiento,
				tamanio_aux = sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		memcpy(&instruccionReduce->resultado, (block->datos) + desplazamiento,
				tamanio_aux = sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		return instruccionReduce;

	}

	int x;
	replanificacionMap* instruccion;
	for (x = 0; x < tamanio_lista; x++) {
		instruccion = obtenerInstruccionReduce();
		list_add(listaResultadoReduceParcial, instruccion);
	}

	free(block->datos);
	free(block);

	return listaResultadoReduceParcial;

}

bloque* serializarFalloOperacion(char* ipNodo, char* puertoNodo) {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 9;

	tamanioBloqueAMandar = sizeof(uint32_t) + 16 + 6;

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, ipNodo, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, puertoNodo, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarFalloOperacionJob(falloNodo* fallo_nodo) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 10;

	tamanioBloqueAMandar = sizeof(uint32_t) + 16 + 6;

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, fallo_nodo->ipNodo, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, fallo_nodo->puertoNodo, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

trabajoJobTerminado* deserializarResultadoReduceExitoso(bloque* block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	trabajoJobTerminado* trabajo = malloc(sizeof(trabajoJobTerminado));

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&trabajo->nombreJob, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	uint32_t longitud_string;
	memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	trabajo->resultadoOperacion = malloc(longitud_string);
	memcpy(trabajo->resultadoOperacion, (block->datos) + desplazamiento,
			tamanio_aux = longitud_string);
	desplazamiento += tamanio_aux;

	trabajo->ipNodo = malloc(16);
	memcpy(trabajo->ipNodo, (block->datos) + desplazamiento, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	trabajo->puertoNodo = malloc(6);
	memcpy(trabajo->puertoNodo, (block->datos) + desplazamiento, tamanio_aux =
			6);
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return trabajo;

}

bloque* serializarOperacionJobExitosa() {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 6;

	tamanioBloqueAMandar = sizeof(uint32_t);

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarGuardarResultadoFileSystem(
		trabajoJobTerminado* trabajo_terminado) {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 3;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t) + 16 + 6
			+ sizeof(uint32_t) + strlen(trabajo_terminado->resultadoOperacion)
			+ 1 + 16 + 6;

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &trabajo_terminado->nombreJob, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, trabajo_terminado->ipJob, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, trabajo_terminado->puertoJob, tamanio_aux =
			6);
	desplazamiento += tamanio_aux;

	uint32_t longitud_string = strlen(trabajo_terminado->resultadoOperacion)
			+ 1;
	memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, trabajo_terminado->resultadoOperacion,
			tamanio_aux = longitud_string);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, trabajo_terminado->ipNodo, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, trabajo_terminado->puertoNodo, tamanio_aux =
			6);
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

resultado_fileSystem* deserializarGuardadoResultadoFileSystem(bloque* block) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	resultado_fileSystem* resultadoFS = malloc(sizeof(resultado_fileSystem));

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&resultadoFS->resultadoGuardado, (block->datos) + desplazamiento,
			tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	resultadoFS->ipJob = malloc(16);
	memcpy(resultadoFS->ipJob, (block->datos) + desplazamiento, tamanio_aux =
			16);
	desplazamiento += tamanio_aux;

	resultadoFS->puertoJob = malloc(6);
	memcpy(resultadoFS->puertoJob, (block->datos) + desplazamiento,
			tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return resultadoFS;
}

bloque* serializarNoSePuedeReplanificarMap() {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 11;

	tamanioBloqueAMandar = sizeof(uint32_t);

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

falloNodo* deserializarResultadoReducerFallo(bloque* block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	falloNodo* nodo = malloc(sizeof(falloNodo));

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	nodo->ipNodo = malloc(16);
	memcpy(nodo->ipNodo, (block->datos) + desplazamiento, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	nodo->puertoNodo = malloc(6);
	memcpy(nodo->puertoNodo, (block->datos) + desplazamiento, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return nodo;
}
