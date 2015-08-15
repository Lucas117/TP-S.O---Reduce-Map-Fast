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

bloque* serializarNombreNodo(int nombreNodo) {

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

	memcpy(datos + desplazamiento, &nombreNodo, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

bloque *serializarBloqueNodo(char* bloqueNodo, int nroBloque) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 2;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ (20 * 1024 * 1024); // accion y bloque de 20MB
	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nroBloque, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, bloqueNodo, tamanio_aux = 20 * 1024 * 1024);
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

bloque *serializarDesconexionNodo() {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 0;

	tamanioBloqueAMandar = sizeof(uint32_t);
	datos = malloc(tamanioBloqueAMandar);

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

bloque *serializarBloquesParaMarta(t_list* listaBloques,
		pedidoArchivo* pedido_archivo, t_list* nodosEnSistema) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 2;

	void leerCopia(infoCopia* copia) {

		bool nodoBuscadito(datosNodo* nodo) {
			return nodo->nombreNodo == copia->nombreNodo;
		}

		datosNodo* nodoAux = list_find(nodosEnSistema, (void*) nodoBuscadito);

		tamanioBloqueAMandar += sizeof(uint32_t) + strlen(copia->ip) + 1
				+ sizeof(uint32_t) + strlen(nodoAux->puerto_escucha) + 1
				+ sizeof(copia->nroBloque) + sizeof(copia->nombreNodo);
	}

	void leerBloque(datoBloque* bloque) {
		tamanioBloqueAMandar += sizeof(bloque->nrobloque) + sizeof(uint32_t); //guarda tambien la cantidad de copias

		list_iterate(bloque->copias, (void*) leerCopia);
	}

	tamanioBloqueAMandar = (sizeof(uint32_t) + sizeof(uint32_t) + 16 + 6
			+ sizeof(uint32_t) + strlen(pedido_archivo->archivoBuscado) + 1); // guarda la accion y la cantidad de bloques de la lista
	list_iterate(listaBloques, (void*) leerBloque); // calcula el tamaño a la estructura a almacenar en memoria

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	int tamanio_lista = list_size(listaBloques);
	memcpy(datos + desplazamiento, &tamanio_lista, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	void almacenarCopia(infoCopia* copia) {

		bool nodoBuscaditito(datosNodo* nodo) {
			return nodo->nombreNodo == copia->nombreNodo;
		}

		datosNodo* nodoAux = list_find(nodosEnSistema, (void*) nodoBuscaditito);

		uint32_t longitud_string = strlen(copia->ip) + 1;
		memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
				sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, copia->ip, tamanio_aux =
				longitud_string);
		desplazamiento += tamanio_aux;

		longitud_string = strlen(nodoAux->puerto_escucha) + 1;
		memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
				sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, nodoAux->puerto_escucha, tamanio_aux =
				longitud_string);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, &copia->nroBloque, tamanio_aux =
				sizeof(copia->nroBloque));
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, &copia->nombreNodo, tamanio_aux =
				sizeof(copia->nombreNodo));
		desplazamiento += tamanio_aux;

	}

	void almacenarBloque(datoBloque* bloque) {
		memcpy(datos + desplazamiento, &bloque->nrobloque, tamanio_aux =
				sizeof(bloque->nrobloque));
		desplazamiento += tamanio_aux;

		int tamanio_lista = list_size(bloque->copias);
		memcpy(datos + desplazamiento, &tamanio_lista, tamanio_aux =
				sizeof(int));
		desplazamiento += tamanio_aux;

		list_iterate(bloque->copias, (void*) almacenarCopia);
	}

	list_iterate(listaBloques, (void*) almacenarBloque);

	memcpy(datos + desplazamiento, pedido_archivo->ipJob, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, pedido_archivo->puertoJob, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	uint32_t longitud_string = strlen(pedido_archivo->archivoBuscado) + 1;
	memcpy(datos + desplazamiento, &longitud_string, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, pedido_archivo->archivoBuscado, tamanio_aux =
			longitud_string);
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

caracteristicasNodo* deserializarConexionNodo(bloque* block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	caracteristicasNodo* nodoNuevo = malloc(sizeof(caracteristicasNodo));

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&nodoNuevo->nombreNodo, (block->datos) + desplazamiento,
			tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(&nodoNuevo->capacidadNodo, (block->datos) + desplazamiento,
			tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	uint32_t tamanio_string;
	memcpy(&tamanio_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	nodoNuevo->puerto_escucha = malloc(tamanio_string);
	memcpy(nodoNuevo->puerto_escucha, (block->datos) + desplazamiento,
			tamanio_aux = tamanio_string);
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return nodoNuevo;

}

bloque* serializarActualizacionNodosDisponibles(t_list* listaNodosEnElSitema,
		int nodosMinimo) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 1;

	tamanioBloqueAMandar = (sizeof(uint32_t) + sizeof(uint32_t) + 4
			+ (list_size(listaNodosEnElSitema)
					* (sizeof(int) + 16 + 6 + sizeof(int)))); // guarda la accion, la cantidad de bloques de la lista y la lista

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nodosMinimo, tamanio_aux = sizeof(int));
	desplazamiento += tamanio_aux;

	uint32_t tamanio_lista = list_size(listaNodosEnElSitema);
	memcpy(datos + desplazamiento, &tamanio_lista, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	void almacenarNodo(datosNodo* nodo) {

		memcpy(datos + desplazamiento, &nodo->nombreNodo, tamanio_aux =
				sizeof(uint32_t));
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, nodo->ip, tamanio_aux = 16);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, nodo->puerto_escucha, tamanio_aux = 6);
		desplazamiento += tamanio_aux;

		memcpy(datos + desplazamiento, &nodo->disponible, tamanio_aux =
				sizeof(int));
		desplazamiento += tamanio_aux;

	}

	list_iterate(listaNodosEnElSitema, (void*) almacenarNodo);

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarSolicitarBloqueArchivo(int nroBloque) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = -2;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t);

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nroBloque, tamanio_aux = sizeof(int));
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}

bloqueMapeado* deserializarBloqueArchivoSolicitado(bloque* block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloqueMapeado* datos = malloc(sizeof(bloqueMapeado));

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

pedidoArchivo* deserializarPedidoBloquesDeArchivo(bloque* block) {
	int desplazamiento = 0;
	int tamanio_aux = 0;
	pedidoArchivo* pedido_archivo = malloc(sizeof(pedidoArchivo));

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	uint32_t tamanio_string;
	memcpy(&tamanio_string, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	pedido_archivo->archivoBuscado = malloc(tamanio_string);
	memcpy(pedido_archivo->archivoBuscado, (block->datos) + desplazamiento,
			tamanio_aux = tamanio_string);
	desplazamiento += tamanio_aux;

	pedido_archivo->ipJob = malloc(16);
	memcpy(pedido_archivo->ipJob, (block->datos) + desplazamiento, tamanio_aux =
			16);
	desplazamiento += tamanio_aux;

	pedido_archivo->puertoJob = malloc(6);
	memcpy(pedido_archivo->puertoJob, (block->datos) + desplazamiento,
			tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	free(block->datos);
	free(block);

	return pedido_archivo;
}

bloque* serializarArchivoNoExiste(pedidoArchivo* archivo_buscado) {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 6;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t)
			+ strlen(archivo_buscado->archivoBuscado) + 1 + 16 + 6;

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	uint32_t longitud_lista = strlen(archivo_buscado->archivoBuscado) + 1;
	memcpy(datos + desplazamiento, &longitud_lista, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, archivo_buscado->archivoBuscado,
			tamanio_aux = longitud_lista);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, archivo_buscado->ipJob, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, archivo_buscado->puertoJob, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarSaludoMarta() {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = -2;

	tamanioBloqueAMandar = sizeof(uint32_t);

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

trabajoJobTerminado* deserializarGuardarResultadoFileSystem(bloque* block) {

	int desplazamiento = 0;
	int tamanio_aux = 0;

	trabajoJobTerminado* trabajo = malloc(sizeof(trabajoJobTerminado));

	tamanio_aux = sizeof(uint32_t);
	desplazamiento = tamanio_aux; // para saltear la accion de los datos

	memcpy(&trabajo->nombreJob, (block->datos) + desplazamiento, tamanio_aux =
			sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	trabajo->ipJob = malloc(16);
	memcpy(trabajo->ipJob, (block->datos) + desplazamiento, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	trabajo->puertoJob = malloc(6);
	memcpy(trabajo->puertoJob, (block->datos) + desplazamiento, tamanio_aux =
			6);
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

bloque* serializarGuardadoResultadoFileSystem(int resultadoGuardado,
		trabajoJobTerminado* trabajo) {

	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 9;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t) + 16 + 6;

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &resultadoGuardado, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, trabajo->ipJob, tamanio_aux = 16);
	desplazamiento += tamanio_aux;

	memcpy(datos + desplazamiento, trabajo->puertoJob, tamanio_aux = 6);
	desplazamiento += tamanio_aux;

	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;
}

bloque* serializarPedidoOperacionJobFinalizada(int nombreJob) {
	int tamanioBloqueAMandar = 0;
	void *datos;
	int desplazamiento = 0;
	int tamanio_aux = 0;

	bloque *block = malloc(sizeof(bloque));

	uint32_t accion = 7;

	tamanioBloqueAMandar = sizeof(uint32_t) + sizeof(uint32_t);

	datos = malloc(tamanioBloqueAMandar); //reserva memoria para el bloque a mandar

	memcpy(datos, &accion, tamanio_aux = sizeof(uint32_t));
	desplazamiento = tamanio_aux;

	memcpy(datos + desplazamiento, &nombreJob, tamanio_aux = sizeof(uint32_t));
	desplazamiento += tamanio_aux;


	block->longitud_datos = desplazamiento;
	block->datos = datos;

	return block;

}
