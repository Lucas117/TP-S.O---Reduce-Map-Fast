/*
 * hSerializadores.h
 *
 *  Created on: 7/6/2015
 *      Author: utnso
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <stdint.h>

#include"EstructurasFS.h"

#ifndef HSERIALIZADORES_H_
#define HSERIALIZADORES_H_


typedef struct _Paquete{
	uint32_t longitud_bloque;
	void *bloque;

} paquete;

typedef struct _Bloque{
	uint32_t longitud_datos;
	void *datos;
} bloque;

paquete *pack(bloque *block);

bloque* serializarNombreNodo(int nombreNodo);

bloque *serializarBloqueNodo (char* bloqueNodo, int nroBloque);

bloque *serializarDesconexionNodo();

bloque *serializarBloquesParaMarta(t_list* listaBloques, pedidoArchivo* pedido_archivo, t_list* nodosEnSistema);

bloque* serializarActualizacionNodosDisponibles(t_list* listaNodosEnElSitema, int nodosMinimo);

caracteristicasNodo* deserializarConexionNodo(bloque* block);

void almacenarBloque(datoBloque* bloque);

void almacenarCopia(infoCopia* copia);

void leerBloque(datoBloque* bloque);

void leerCopia(infoCopia* copia);

bloqueMapeado* deserializarBloqueArchivoSolicitado(bloque* block);

pedidoArchivo* deserializarPedidoBloquesDeArchivo(bloque* block);

trabajoJobTerminado* deserializarGuardarResultadoFileSystem(bloque* block);

bloque* serializarGuardadoResultadoFileSystem(int resultadoGuardado, trabajoJobTerminado* trabajo);

bloque* serializarArchivoNoExiste(pedidoArchivo* archivo_buscado);

bloque* serializarPedidoOperacionJobFinalizada(int nombreJob);


#endif /* HSERIALIZADORES_H_ */
