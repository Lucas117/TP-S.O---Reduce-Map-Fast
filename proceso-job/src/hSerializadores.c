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

	free(block);

	return pack;

}

bloque* serializarPedirTrabajoDeArchivoAMarta(char** direccionesArchivo,
		int combiner) {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;
	int longitud_string;
	int cantArchivos = 0;
	int cantidad_archivos;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 1;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ sizeof(uint32_t);

	while (direccionesArchivo[cantArchivos] != NULL) {
		tamanioBloqueAMandar += sizeof(uint32_t)
				+ strlen(direccionesArchivo[cantArchivos]) + 1;
		cantArchivos++;
	}

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &combiner, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	cantidad_archivos = cantArchivos;
	memcpy(datos + desplazamiento, &cantidad_archivos, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	cantArchivos = 0;

	while (direccionesArchivo[cantArchivos] != NULL) {

		longitud_string = strlen(direccionesArchivo[cantArchivos]) + 1;
		memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
				sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, direccionesArchivo[cantArchivos],
				tamanio_aux = longitud_string);
		desplazamiento += tamanio_aux;

		cantArchivos++;
	}

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

char* deserializarMapJob(bloque* block, t_list* instruccionesMap) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	uint32_t tamanio_lista;
	uint32_t longitud_string;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	char* archivoTrabajo = malloc(longitud_string);
	memcpy(archivoTrabajo, (block->datos) + desplazamiento, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	memcpy(&tamanio_lista, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	instruccionMap* obtenerInstruccion() {

		instruccionMap* instruccion = malloc(sizeof(instruccionMap));

		instruccion->ipNodo = malloc(16);
		memcpy(instruccion->ipNodo, (block->datos) + desplazamiento,
				tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		instruccion->puertoNodo = malloc(6);
		memcpy(instruccion->puertoNodo, (block->datos) + desplazamiento,
				tamanio_aux = 6);
		desplazamiento += tamanio_aux;

		memcpy(&instruccion->bloqueNodo, (block->datos) + desplazamiento,
				tamanio_aux = sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		return instruccion;

	}

	int x;
	for (x = 0; x < tamanio_lista; x++) {
		instruccionMap* instruccionMap = obtenerInstruccion();
		list_add(instruccionesMap, instruccionMap);
	}

	free(block->datos);
	free(block);

	return archivoTrabajo;

}

bloque* serializarMapNodo(script* datos_script, int nroBloque, int nombreJob) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 1;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ sizeof(uint32_t) + sizeof(uint32_t)
			+ datos_script->tamanioScriptMap;

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos + desplazamiento, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nombreJob, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, &nroBloque, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, &datos_script->tamanioScriptMap,
			tamanio_aux = sizeof(datos_script->tamanioScriptMap));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, datos_script->instruccionMap, tamanio_aux =
			datos_script->tamanioScriptMap);
	desplazamiento += tamanio_aux;

	block->datos = datos;
	block->longitud_datos = desplazamiento;

	return block;

}

int deserializarResultadoMapNodo(bloque* block) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	int resultado;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&resultado, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));

	free(block->datos);
	free(block);

	return resultado;
}

bloque* serializarResultadoMapMarta(instruccionMap* instruccion_Map,
		int resultado, char* archivoTrabajo) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 3;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t) + 16 + 6
			+ sizeof(uint32_t) + sizeof(uint32_t) + strlen(archivoTrabajo) + 1;

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos + desplazamiento, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &resultado, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, instruccion_Map->ipNodo, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, instruccion_Map->puertoNodo, tamanio_aux =
			6);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, &instruccion_Map->bloqueNodo, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	uint32_t longitud_string = strlen(archivoTrabajo) + 1;
	memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, archivoTrabajo, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	block->datos = datos;
	block->longitud_datos = desplazamiento;

	return block;

}

char* deserializarMapJobReplan(bloque* block, instruccionMap* instruccion) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	uint32_t longitud_string;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	char* archivoTrabajo = malloc(longitud_string);
	memcpy(archivoTrabajo, (block->datos) + desplazamiento, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	instruccion->ipNodo = malloc(16);
	memcpy(instruccion->ipNodo, (block->datos) + desplazamiento, tamanio_aux =
			16);
	desplazamiento += tamanio_aux;

	instruccion->puertoNodo = malloc(6);
	memcpy(instruccion->puertoNodo, (block->datos) + desplazamiento,
			tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	memcpy(&instruccion->bloqueNodo, (block->datos) + desplazamiento,
			tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return archivoTrabajo;
}

char* deserializarReduceJobSinCombiner(bloque* block,
		instruccionReduce* instrucciones_Reduce) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	uint32_t tamanio_lista;
	uint32_t longitud_string;

	instrucciones_Reduce->instruccionesReduce = list_create();

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	char* archivoTrabajo = malloc(longitud_string);
	memcpy(archivoTrabajo, (block->datos) + desplazamiento, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	instrucciones_Reduce->ipNodoPrincipal = malloc(16);
	memcpy(instrucciones_Reduce->ipNodoPrincipal,
			(block->datos) + desplazamiento, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	instrucciones_Reduce->puertoNodoPrincipal = malloc(6);
	memcpy(instrucciones_Reduce->puertoNodoPrincipal,
			(block->datos) + desplazamiento, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	memcpy(&tamanio_lista, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	datosInstruccionReduce* obtenerInstruccion() {
		datosInstruccionReduce* instruccion = malloc(sizeof(instruccionMap));

		instruccion->ipNodo = malloc(16);
		memcpy(instruccion->ipNodo, (block->datos) + desplazamiento,
				tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		instruccion->puertoNodo = malloc(6);
		memcpy(instruccion->puertoNodo, (block->datos) + desplazamiento,
				tamanio_aux = 6);
		desplazamiento += tamanio_aux;

		memcpy(&instruccion->bloqueNodo, (block->datos) + desplazamiento,
				tamanio_aux = sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		return instruccion;
	}

	int x;
	for (x = 0; x < tamanio_lista; x++) {
		datosInstruccionReduce* datoInstruccion = obtenerInstruccion();
		list_add(instrucciones_Reduce->instruccionesReduce, datoInstruccion);
	}

	free(block->datos);
	free(block);

	return archivoTrabajo;

}

char* deserializarReduceParcialJobConCombiner(bloque* block,
		instruccionReduce* instrucciones_Reduce) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	uint32_t tamanio_lista;
	uint32_t longitud_string;

	instrucciones_Reduce->instruccionesReduce = list_create();

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	char* archivoTrabajo = malloc(longitud_string);
	memcpy(archivoTrabajo, (block->datos) + desplazamiento, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	instrucciones_Reduce->ipNodoPrincipal = malloc(16);
	memcpy(instrucciones_Reduce->ipNodoPrincipal,
			(block->datos) + desplazamiento, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	instrucciones_Reduce->puertoNodoPrincipal = malloc(6);
	memcpy(instrucciones_Reduce->puertoNodoPrincipal,
			(block->datos) + desplazamiento, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	memcpy(&tamanio_lista, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	datosInstruccionReduce* obtenerInstruccion() {
		datosInstruccionReduce* instruccion = malloc(sizeof(instruccionMap));

		memcpy(&instruccion->bloqueNodo, (block->datos) + desplazamiento,
				tamanio_aux = sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		return instruccion;
	}

	int x;
	for (x = 0; x < tamanio_lista; x++) {
		datosInstruccionReduce* datoInstruccion = obtenerInstruccion();
		list_add(instrucciones_Reduce->instruccionesReduce, datoInstruccion);
	}

	free(block->datos);
	free(block);

	return archivoTrabajo;
}

bloque* serializarReduceSinCombinerNodo(script* datos_script,
		instruccionReduce* instruccion_reduce, int nombreJob) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 2;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ sizeof(datos_script->tamanioScriptReduce)
			+ datos_script->tamanioScriptReduce + 16 + 6 + sizeof(uint32_t)
			+ (list_size(instruccion_reduce->instruccionesReduce)
					* (16 + 6 + sizeof(int)));

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos + desplazamiento, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nombreJob, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, &datos_script->tamanioScriptReduce,
			tamanio_aux = sizeof(datos_script->tamanioScriptReduce));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, datos_script->insruccionReduce, tamanio_aux =
			datos_script->tamanioScriptReduce);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, instruccion_reduce->ipNodoPrincipal,
			tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, instruccion_reduce->puertoNodoPrincipal,
			tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	uint32_t tamanio_lista = list_size(instruccion_reduce->instruccionesReduce);
	memcpy(datos + desplazamiento, &tamanio_lista, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	void almacenarInstruccion(datosInstruccionReduce* instruccion) {
		memcpy(datos + desplazamiento, instruccion->ipNodo, tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, instruccion->puertoNodo, tamanio_aux =
				6);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, &instruccion->bloqueNodo, tamanio_aux =
				sizeof(uint32_t));
		desplazamiento += tamanio_aux;
	}

	list_iterate(instruccion_reduce->instruccionesReduce,
			(void*) almacenarInstruccion);

	block->datos = datos;
	block->longitud_datos = desplazamiento;

	return block;

}

bloque* serializarReduceParcialConCombinerNodo(script* datos_script,
		instruccionReduce* instruccion_reduce, int nombreJob) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 3;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ sizeof(datos_script->tamanioScriptReduce)
			+ datos_script->tamanioScriptReduce;

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos + desplazamiento, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nombreJob, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, &datos_script->tamanioScriptReduce,
			tamanio_aux = sizeof(datos_script->tamanioScriptReduce));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, datos_script->insruccionReduce, tamanio_aux =
			datos_script->tamanioScriptReduce);
	desplazamiento += tamanio_aux;

	block->datos = datos;
	block->longitud_datos = desplazamiento;

	return block;

}

bloque* serializarReduceFinalConCombinerNodo(script* datos_script,
		instruccionReduce* instruccion_reduce, int nombreJob) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 4;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ sizeof(datos_script->tamanioScriptReduce)
			+ datos_script->tamanioScriptReduce + 16 + 6 + sizeof(uint32_t)
			+ (list_size(instruccion_reduce->instruccionesReduce) * (16 + 6));

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos + desplazamiento, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nombreJob, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, &datos_script->tamanioScriptReduce,
			tamanio_aux = sizeof(datos_script->tamanioScriptReduce));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, datos_script->insruccionReduce, tamanio_aux =
			datos_script->tamanioScriptReduce);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, instruccion_reduce->ipNodoPrincipal,
			tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, instruccion_reduce->puertoNodoPrincipal,
			tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	uint32_t tamanio_lista = list_size(instruccion_reduce->instruccionesReduce);
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

	list_iterate(instruccion_reduce->instruccionesReduce,
			(void*) almacenarInstruccion);

	block->datos = datos;
	block->longitud_datos = desplazamiento;

	return block;

}

char* deserializarNoExisteArchivoPedido(bloque* block) {
	int desplazamiento = 0;
	int tamanio_aux = 0;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	uint32_t longitud_string;
	memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	char* archivo_buscado = malloc(longitud_string);
	memcpy(archivo_buscado, (block->datos) + desplazamiento, tamanio_aux =
			longitud_string);

	free(block->datos);
	free(block);

	return archivo_buscado;
}

int deserializarResultadoReduceParcialConCombiner(bloque* block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	int resultado;

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&resultado, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return resultado;
}

bloque* serializarResultadoReduceExitoso(int nombreJob,
		char *resultado_operacion, char* ipNodo, char* puertoNodo) {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 5;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ sizeof(uint32_t) + strlen(resultado_operacion) + 1 + 16 + 6;

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos + desplazamiento, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nombreJob, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	uint32_t longitud_string = strlen(resultado_operacion) + 1;
	memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, resultado_operacion, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, ipNodo, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, puertoNodo, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	block->datos = datos;
	block->longitud_datos = desplazamiento;

	return block;
}

bloque* serializarResultadoReduceSinCombinerFallo(char* ipNodoPrincipal,
		char* puertoNodoPrincipal) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 7;

	tamanioBloqueAMandar = sizeof(uint32_t) + 16 + 6;

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos + desplazamiento, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, ipNodoPrincipal, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, puertoNodoPrincipal, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	block->datos = datos;
	block->longitud_datos = desplazamiento;

	return block;
}

bloque* serializarResultadoReduceParcial(int resultado,
		instruccionReduce* instruccion_reduce) {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 4;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ (list_size(instruccion_reduce->instruccionesReduce)
					* (16 + 6 + sizeof(uint32_t) + sizeof(uint32_t)));

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos + desplazamiento, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	uint32_t tamanio_lista = list_size(instruccion_reduce->instruccionesReduce);
	memcpy(datos + desplazamiento, &tamanio_lista, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	void almacenarInstruccion(datosInstruccionReduce* instruccion) {

		memcpy(datos + desplazamiento, instruccion_reduce->ipNodoPrincipal,
				tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, instruccion_reduce->puertoNodoPrincipal,
				tamanio_aux = 6);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, &instruccion->bloqueNodo, tamanio_aux =
				sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, &resultado, tamanio_aux =
				sizeof(uint32_t));
		desplazamiento += tamanio_aux;
	}

	list_iterate(instruccion_reduce->instruccionesReduce,
			(void*) almacenarInstruccion);

	block->datos = datos;
	block->longitud_datos = desplazamiento;

	return block;

}

falloNodo* deserializarFalloOperacion(bloque* block) {
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

bloque* serializarResultadoReduceConCombinerFallo(char* ipNodoPrincipal,
		char* puertoNodoPrincipal) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 8;

	tamanioBloqueAMandar = sizeof(uint32_t) + 16 + 6;

	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos + desplazamiento, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, ipNodoPrincipal, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, puertoNodoPrincipal, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	block->datos = datos;
	block->longitud_datos = desplazamiento;

	return block;

}

char* deserializarReduceFinalJobConCombiner(bloque* block,
		instruccionReduce* instrucciones_Reduce) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	uint32_t tamanio_lista;
	uint32_t longitud_string;

	instrucciones_Reduce->instruccionesReduce = list_create();

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&longitud_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	char* archivoTrabajo = malloc(longitud_string);
	memcpy(archivoTrabajo, (block->datos) + desplazamiento, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	instrucciones_Reduce->ipNodoPrincipal = malloc(16);
	memcpy(instrucciones_Reduce->ipNodoPrincipal,
			(block->datos) + desplazamiento, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	instrucciones_Reduce->puertoNodoPrincipal = malloc(6);
	memcpy(instrucciones_Reduce->puertoNodoPrincipal,
			(block->datos) + desplazamiento, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	memcpy(&tamanio_lista, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	reducesParcialesParaJob* obtenerInstruccion() {
		reducesParcialesParaJob* instruccion = malloc(sizeof(instruccionMap));

		instruccion->ipNodo = malloc(16);
		memcpy(instruccion->ipNodo, (block->datos) + desplazamiento,
				tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		instruccion->puertoNodo = malloc(6);
		memcpy(instruccion->puertoNodo, (block->datos) + desplazamiento,
				tamanio_aux = 6);
		desplazamiento += tamanio_aux;

		return instruccion;
	}

	int x;
	for (x = 0; x < tamanio_lista; x++) {
		datosInstruccionReduce* datoInstruccion = obtenerInstruccion();
		list_add(instrucciones_Reduce->instruccionesReduce, datoInstruccion);
	}

	free(block->datos);
	free(block);

	return archivoTrabajo;

}

