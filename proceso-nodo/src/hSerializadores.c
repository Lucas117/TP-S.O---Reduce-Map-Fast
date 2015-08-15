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

void deserializarBloqueNodo(bloque *block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	char *bloqueNodo = malloc(20971520); //bloque de 20MB
	int nroBloque;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&nroBloque, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(bloqueNodo, (block->datos) + desplazamiento, 20971520);

	free(block->datos);
	free(block);

	setBloque(bloqueNodo, nroBloque);
	free(bloqueNodo);

}

bloque* serializarNombreYCapacidadNodo(int nombreNodo, int capacidadNodo,
		char* puerto_local) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 1;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ sizeof(uint32_t) + sizeof(uint32_t) + strlen(puerto_local) + 1;
	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nombreNodo, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, &capacidadNodo, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	uint32_t tamanio_string = strlen(puerto_local) + 1;
	memcpy(datos + desplazamiento, &tamanio_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, puerto_local, tamanio_aux = tamanio_string);
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarResultadoMap(int resultado) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 1;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t);
	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &resultado, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

void deserializarNombreNodo(bloque* block) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	int nombreNodo;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&nombreNodo, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));

	free(block->datos);
	free(block);

	cambiarNombre(nombreNodo);

	char * mostrar_nombre=malloc(strlen("El nombre del Nodo dentro del FileSytem es: ") + 4 + 1);
	strcpy(mostrar_nombre,"El nombre del Nodo dentro del FileSytem es: ");
	char * numero_a_mostrar = string_itoa(nombreNodo);
	strcat(mostrar_nombre,numero_a_mostrar);
	free(numero_a_mostrar);
	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO,mostrar_nombre);
	free(mostrar_nombre);

}

void deserializarMapNodo(bloque* block, trabajoMap* trabajo) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&trabajo->nombreJob, block->datos + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(&trabajo->nroBloque, block->datos + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(&trabajo->tamanioScriptMap, block->datos + desplazamiento,
			tamanio_aux = sizeof(trabajo->tamanioScriptMap));
	desplazamiento += tamanio_aux;

	trabajo->instruccionMap = malloc(trabajo->tamanioScriptMap);
	memcpy(trabajo->instruccionMap, block->datos + desplazamiento, tamanio_aux =
			trabajo->tamanioScriptMap);
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

}

bloque_mapeado* deserializarSolicitarBloqueArchivo(bloque* block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	int nroBloque;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&nroBloque, block->datos + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return getBloque(nroBloque);

}

bloque* serializarBloqueArchivoSolicitado(bloque_mapeado* bloqueMapeado) {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 1;

	tamanioBloqueAMandar = sizeof(uint32_t)
			+ sizeof(bloqueMapeado->tamanio_bloque)
			+ bloqueMapeado->tamanio_bloque;

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &bloqueMapeado->tamanio_bloque, tamanio_aux =
			sizeof(bloqueMapeado->tamanio_bloque));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, bloqueMapeado->bloque, tamanio_aux =
			bloqueMapeado->tamanio_bloque);
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	limpiarBloqueMapeado(bloqueMapeado);

	return block;

}

void deserializarReduceFinal(bloque* block, trabajoReduce* trabajo) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	trabajo->datosBloqueArchivo = list_create();

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&trabajo->nombreJob, block->datos + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(&trabajo->tamanioScriptReduce, block->datos + desplazamiento,
			tamanio_aux = sizeof(trabajo->tamanioScriptReduce));
	desplazamiento += tamanio_aux;

	trabajo->instruccionReduce = malloc(trabajo->tamanioScriptReduce);
	memcpy(trabajo->instruccionReduce, block->datos + desplazamiento,
			tamanio_aux = trabajo->tamanioScriptReduce);
	desplazamiento += tamanio_aux;

	trabajo->ipLocal = malloc(16);
	memcpy(trabajo->ipLocal, block->datos + desplazamiento, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	trabajo->puertoLocal = malloc(6);
	memcpy(trabajo->puertoLocal, block->datos + desplazamiento, tamanio_aux =
			6);
	desplazamiento += tamanio_aux;

	uint32_t tamanio_lista;
	memcpy(&tamanio_lista, block->datos + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	datoBloqueArchivo* obtenerTrabajo() {
		datoBloqueArchivo* instruccionReduce = malloc(
				sizeof(datoBloqueArchivo));

		instruccionReduce->ipNodo = malloc(16);
		memcpy(instruccionReduce->ipNodo, block->datos + desplazamiento,
				tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		instruccionReduce->puertoNodo = malloc(6);
		memcpy(instruccionReduce->puertoNodo, block->datos + desplazamiento,
				tamanio_aux = 6);
		desplazamiento += tamanio_aux;

		return instruccionReduce;
	}

	int x;
	for (x = 0; x < tamanio_lista; x++) {
		datoBloqueArchivo* dato = obtenerTrabajo();
		list_add(trabajo->datosBloqueArchivo, dato);
	}

	free(block->datos);
	free(block);

}

void deserializarReduceParcialConCombinerNodo(bloque* block,
		trabajoReduce* trabajo) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&trabajo->nombreJob, block->datos + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(&trabajo->tamanioScriptReduce, block->datos + desplazamiento,
			tamanio_aux = sizeof(trabajo->tamanioScriptReduce));
	desplazamiento += tamanio_aux;

	trabajo->instruccionReduce = malloc(trabajo->tamanioScriptReduce);
	memcpy(trabajo->instruccionReduce, block->datos + desplazamiento,
			tamanio_aux = trabajo->tamanioScriptReduce);
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

}

bloque* serializarPedidoMapRealizado(int nombreJob, int nroBloque) {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 5;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ sizeof(uint32_t);

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nombreJob, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, &nroBloque, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarResultadoReduceFinal(int resultado) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = resultado;

	tamanioBloqueAMandar = sizeof(uint32_t);
	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

void deserializarPedidoMapRealizado(bloque* block, pedidoMap* pedido_map) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&pedido_map->nombreJob, block->datos + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(&pedido_map->nroBloque, block->datos + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);
}

bloque_mapeado* deserializarBloqueArchivoSolicitado(bloque* block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque_mapeado* datos = malloc(sizeof(bloque_mapeado));

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&datos->tamanio_bloque, (block->datos) + desplazamiento,
			tamanio_aux = sizeof(datos->tamanio_bloque));
	desplazamiento += tamanio_aux;

	datos->bloque = malloc(datos->tamanio_bloque);
	memcpy(datos->bloque, (block->datos) + desplazamiento,
			tamanio_aux = datos->tamanio_bloque);
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return datos;

}

bloque* serializarPedidoReduceRealizado(int nombreJob) {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 6;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t);
	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nombreJob, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

int deserializarPedidoReduceRealizado(bloque* block) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	int nombreJob;

	bloque_mapeado* datos = malloc(sizeof(bloque_mapeado));

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&nombreJob, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return nombreJob;

}

bloque* serializarResultadoReduceParcialConCombiner(int resultado) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 1;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t);
	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &resultado, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

int deserializarPedidoOperacionJobFinalizada(bloque* block) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	int nombreJob;

	bloque_mapeado* datos = malloc(sizeof(bloque_mapeado));

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&nombreJob, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return nombreJob;

}

void deserializarReduceSinCombinerNodo(bloque* block, trabajoReduce* trabajo) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	trabajo->datosBloqueArchivo = list_create();

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&trabajo->nombreJob, block->datos + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(&trabajo->tamanioScriptReduce, block->datos + desplazamiento,
			tamanio_aux = sizeof(trabajo->tamanioScriptReduce));
	desplazamiento += tamanio_aux;

	trabajo->instruccionReduce = malloc(trabajo->tamanioScriptReduce);
	memcpy(trabajo->instruccionReduce, block->datos + desplazamiento,
			tamanio_aux = trabajo->tamanioScriptReduce);
	desplazamiento += tamanio_aux;

	trabajo->ipLocal = malloc(16);
	memcpy(trabajo->ipLocal, block->datos + desplazamiento, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	trabajo->puertoLocal = malloc(6);
	memcpy(trabajo->puertoLocal, block->datos + desplazamiento, tamanio_aux =
			6);
	desplazamiento += tamanio_aux;

	uint32_t tamanio_lista;
	memcpy(&tamanio_lista, block->datos + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	datoBloqueArchivo* obtenerTrabajo() {
		datoBloqueArchivo* instruccionReduce = malloc(
				sizeof(datoBloqueArchivo));

		instruccionReduce->ipNodo = malloc(16);
		memcpy(instruccionReduce->ipNodo, block->datos + desplazamiento,
				tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		instruccionReduce->puertoNodo = malloc(6);
		memcpy(instruccionReduce->puertoNodo, block->datos + desplazamiento,
				tamanio_aux = 6);
		desplazamiento += tamanio_aux;

		memcpy(&instruccionReduce->nroBloque, block->datos + desplazamiento,
				tamanio_aux = sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		return instruccionReduce;
	}

	int x;
	for (x = 0; x < tamanio_lista; x++) {
		datoBloqueArchivo* dato = obtenerTrabajo();
		list_add(trabajo->datosBloqueArchivo, dato);
	}

	free(block->datos);
	free(block);

}

