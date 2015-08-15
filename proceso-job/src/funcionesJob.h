/*
 * funcionesJob.h
 *
 *  Created on: 1/7/2015
 *      Author: utnso
 */

#include<commons/config.h>
#include<commons/string.h>
#include <commons/log.h>
#include <commons/txt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>


#include "estructurasJob.h"
#define PATH "archivoJOB.conf"
#define PATH_LOG "LogJob.log"

#ifndef FUNCIONESJOB_H_
#define FUNCIONESJOB_H_

lectura* leerConfiguracion();

void liberarLectura(lectura* lectura);

script* mapearScriptsAMemoria(char *ruta_mapper, char *ruta_reducer);


#endif /* FUNCIONESJOB_H_ */
