/*
 * funcionesNodo.h
 *
 *  Created on: 4/6/2015
 *      Author: utnso
 */

#include <commons/config.h>
#include <commons/string.h>
#include <commons/log.h>
#include <commons/txt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "estructurasNodo.h"
#define PATH "archivoNODO.conf"
#define PATH_LOG "LogNodo.log"

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

#define TAMANIO_BLOQUE 20*1024*1024

#ifndef FUNCIONESNODO_H_
#define FUNCIONESNODO_H_


/*
Descripcion: recibe un path donde está el archivo y devuelve una estructura valoresConfig para leer
Precondición: el archivo debe existir sí o sí en el path
 */

lectura* leerConfiguracion();

/*crea directorio temporal con el nombre del job y una carpeta adentro con el nombre recibidos adentro*/
rutas_temporales *crearDirectorioTemporal(char* path_dir,int nombre_job);

/*libera el puntero recibido de crearDirectorioTemporal*/
void liberarRutasTemporales(rutas_temporales* estructura);

void liberarLectura(lectura* lectura);

/*Crea un archivo vacio con el nombre y con el tamanio pasado por parametro*/
void crearArchivoVacio(char *data_nombre, long data_size);

/*Copia un bloque en el archivo data.dat*/
void setBloque (char *bloque_recibido,int desplazamiento);

/*Saca del archivo data.dat la informacion de un bloque pasado por parametro*/
bloque_mapeado *getBloque(int numero_bloque);

/*Limpia los ceros que tiene el bloque extraido por la funcion getBloque*/
bloque_mapeado *limpiarCerosDelBloque(char *bloque_a_limpiar);

/*Ejecuta el script map a un bloque*/
void aplicarMapABloque(char *script_mapper, int nro_bloque, char *ruta_tmp,int nombre_job);

/*Ejecuta el script con la llamada al sistema y lo guarda en el archivo pasado por parametro para reduce*/
void ejecutarScriptReduce(char* path, char* in, char* out);

/*Ejecuta el script con la llamada al sistema y lo guarda en el archivo pasado por parametro para map*/
void ejecutarScriptMap(char* path, bloque_mapeado* in, char* out);

/*Inicializa variable global lista trabajo bloques*/
void inicializarListaTrabajoBloques(int nombre_job);

/*setea la variable global lista trabajo bloques con NULL */
void vaciarListaTrabajoBloques(trabajoPorJob *un_job);

/*Mapea a memoria la variable global archivo_data_bin*/
void mapearDataBinAMemoria(char *ruta_bin);

/*Baja el script a plano, crea directorio temporal y ejecuta el script con todo ello*/
int ejecutarMap(trabajoMap *datos_map);

/*Baja el script recibido a un archivo plano*/
char *bajarScriptAPlano(char *datos_script,int tamanio_script,char *ruta_temp,int map_reduce,int nom_jo); //si es 0 es un map y si es 1 es un reduce

/*Crea archivo data.dat en el nodo*/
void crearArchivoData(char *data_nombre, long data_size);

/*Setea variables globales y crea directorio con el nombre del nodo en el directorio tmp. Si el dir temp existe, lo elimina*/
void inicializarNodo(long capacidad_nodo, char *path_temp);

/* Crea un string con el comando necesario para eliminar carpeta temporal */
char *crearDirKiller(char *path_temp);

/*Actualiza estado de los bloques en la lista global de trabajos*/
void actualizarEstadoBloques(char *resultado_script,int nro_bloque,int nombre_job);

/*Hace frees a toda la estructura global*/
void destruirListaGlobal();

/*Intefraz con los sockets para ejecutar reduce local*/
int llamameParaEjecutarReduceLocal(trabajoReduce *trabajo_reduce);

/*Ejecuta reduce local*/
void aplicarReduceLocal(char* ruta_reducer,char* ruta_temp,int nombreJob);

/*busca de la lista global los resultados mapeados del job, los agrupa en un mismo archivo y devuelve la ruta de ese archivo*/
char* juntarResultadosMapeados(char** bloques_mapeados,char *ruta_tmp,int nombre_job);

bloque_mapeado* enviarMap(int nombreJob, int num_map);

bloque_mapeado* enviarReduceLocal(int nombreJob);

bloque_mapeado* enviarResultadoFinal(int nombreJob);

char* juntarTodasLasRutasDeRecibidos(char* ruta_tmp, int nombreJob);

void guardarRecibido(bloque_mapeado * bloque_recibido, int nombreJob);

void loguearEntradaJob(int nombreJob);

void loguearSalidaJob(int nombreJob);

/*Hace el reduce totalSinCombiner, esta llamada en uno de los cases de sockets.h. Devuelve 1 si esta todo bien. */
int hacerReduceTotalSinCombiner(char* datos_script, int nombre_job,int tamanio_script);

/*Hace el reduce totalConCombiner, esta llamada en uno de los cases de sockets.h. Devuelve 1 si esta todo bien. */
int hacerReduceTotalConCombiner(char* datos_script, int nombre_job,int tamanio_script);

/*Recibe por parametro: la ruta,
 * 						el nombre del proceso que queres loguear,
 * 						un booleano que me dice si quiero que el log salga por pantalla o no,
 * 						el nivel del log asi como esta, sin comillas ni dobles:
 * 						LOG_LEVEL_ERROR , LOG_LEVEL_DEBUG , LOG_LEVEL_INFO ,
 * 						LOG_LEVEL_TRACE , LOG_LEVEL_WARNING
 * 						y el mensaje a mostrar*/

void loguearme(char *ruta, char *nombre_proceso,bool mostrar_por_consola ,t_log_level nivel, const char *mensaje);

/*Cambia el nombre del nodo en el archivo de configuracion*/
void cambiarNombre(int);

/*limpia la estructura bloque_mapeado*/
void limpiarBloqueMapeado(bloque_mapeado * limpiar);

#endif /* FUNCIONESNODO_H_ */
