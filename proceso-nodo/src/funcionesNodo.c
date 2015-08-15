#include "funcionesNodo.h"

pthread_mutex_t mutex_actualizacion_listas = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t mutex_bajar_script = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t mutex_para_logs = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t mutex_mmap = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t mutex_munmap = PTHREAD_MUTEX_INITIALIZER;

t_list* trabajos_de_jobs;

char *data_bin_mapeado;

int cantidad_bloques;

void mapearDataBinAMemoria(char *ruta_bin){

	int file_descriptor,tamanio_archivo;

	file_descriptor = open(ruta_bin, O_RDWR);

	struct stat file;

	fstat(file_descriptor, &file);

	tamanio_archivo = file.st_size;

	data_bin_mapeado = mmap((caddr_t) 0, tamanio_archivo,PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0);

	close(file_descriptor);
}


void unmapearDataBinAMemoria(){

	pthread_mutex_lock(&mutex_munmap);

	lectura *resultado_lectura = leerConfiguracion();

	int file_descriptor = open(resultado_lectura->nombreBin,O_RDWR);

	struct stat file;

	fstat(file_descriptor, &file);

	int tamanio_archivo = file.st_size;

	munmap(data_bin_mapeado,tamanio_archivo);

	close(file_descriptor);

	liberarLectura(resultado_lectura);

	pthread_mutex_unlock(&mutex_munmap);
}

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

		tam = string_length(config_get_string_value(archivo, "PUERTO_FS"));
		a->puertoFs = malloc(tam + 1);
		strcpy(a->puertoFs, config_get_string_value(archivo, "PUERTO_FS"));

		tam = string_length(config_get_string_value(archivo, "IP_FS"));
		a->ipFS = malloc(tam + 1);
		strcpy(a->ipFS, config_get_string_value(archivo, "IP_FS"));

		tam = string_length(config_get_string_value(archivo, "ARCHIVO_BIN"));
		a->nombreBin = malloc(tam + 1);
		strcpy(a->nombreBin, config_get_string_value(archivo, "ARCHIVO_BIN"));

		tam = string_length(config_get_string_value(archivo, "DIR_TEMP"));
		a->dirTemp = malloc(tam + 1);
		strcpy(a->dirTemp, config_get_string_value(archivo, "DIR_TEMP"));

		a->nombreNodo = config_get_int_value(archivo, "NOMBRE_NODO");
		a->tamanioNodo = config_get_long_value(archivo, "TAMANIO_NODO");

		tam = string_length(config_get_string_value(archivo, "PUERTO_NODO"));
		a->puertoEscucha = malloc(tam + 1);
		strcpy(a->puertoEscucha,
				config_get_string_value(archivo, "PUERTO_NODO"));

		config_destroy(archivo);
		return a;
	} else {
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_ERROR,
				"El archivo .conf no existe");

	}

	return NULL;
}

void liberarLectura(lectura* leido) {
	free(leido->puertoFs);
	free(leido->ipFS);
	free(leido->nombreBin);
	free(leido->dirTemp);
	free(leido->puertoEscucha);

	free(leido);

}

void inicializarNodo(long capacidad_nodo, char *path_temp) {

	struct stat st = { 0 };

	cantidad_bloques = (int)(capacidad_nodo / (TAMANIO_BLOQUE));

	trabajos_de_jobs = list_create();


	if (stat(path_temp, &st) == -1) {

		mkdir(path_temp, 0700);

		return;
	}

	char *comando_rmdir = crearDirKiller(path_temp);

	system(comando_rmdir);

	mkdir(path_temp, 0700);

	free(comando_rmdir);

	return;
}

char *crearDirKiller(char *path_temp) {

	char *comando_dir_killer = malloc(strlen(path_temp) + 7); //rm -r (cantidad de caracteres 6)

	strcpy(comando_dir_killer, "rm -r ");

	strcat(comando_dir_killer, path_temp);

	return comando_dir_killer;
}

void crearArchivoVacio(char *data_nombre, long data_size) {

	FILE *archivo_data = fopen(data_nombre, "ab");

	//char *crear_archivo_enorme = string_from_format("fallocate -l %l %s",data_size,data_nombre);

	//system(crear_archivo_enorme);

	ftruncate(fileno(archivo_data), data_size);

	fclose(archivo_data);

	//free(crear_archivo_enorme);
}

void crearArchivoData(char *data_nombre, long data_size) {
	FILE *fp = fopen(data_nombre, "r");
	if (!fp) {

		crearArchivoVacio(data_nombre, data_size);
		loguearme(PATH_LOG, "Proceso Nodo", 0, 2, "Se creo el archivo .dat");

	}

	else {
		loguearme(PATH_LOG, "Proceso Nodo", 0, 2, "El archivo .dat ya existe.");

		fclose(fp);
	}

}

void setBloque(char *bloque_recibido, int numero_bloque) {

	pthread_mutex_lock(&mutex_mmap);

	int desplazamiento = numero_bloque * TAMANIO_BLOQUE;

	memcpy(data_bin_mapeado + desplazamiento, bloque_recibido, TAMANIO_BLOQUE);

	msync(data_bin_mapeado, TAMANIO_BLOQUE, MS_SYNC);

	pthread_mutex_unlock(&mutex_mmap);

	loguearme(PATH_LOG, "Proceso Nodo", 0, LOG_LEVEL_INFO,"Bloque recibido del FileSystem");

}

bloque_mapeado *getBloque(int numero_bloque) {

	pthread_mutex_lock(&mutex_mmap);

	char *bloque_obtenido;

	int desplazamiento = numero_bloque * TAMANIO_BLOQUE;

	bloque_obtenido = (data_bin_mapeado + desplazamiento);

	pthread_mutex_unlock(&mutex_mmap);

	return limpiarCerosDelBloque(bloque_obtenido);

}

bloque_mapeado *limpiarCerosDelBloque(char *bloque_a_limpiar) {

	bloque_mapeado *bloque = malloc(sizeof(bloque_mapeado));

	//char *bloque_limpio;

	int i = TAMANIO_BLOQUE;

	int contador = 0;

	while (bloque_a_limpiar[i - 1] == '0') {

		contador++;
		i--;

	}

	//bloque_limpio = malloc(TAMANIO_BLOQUE - contador);

	//memcpy(bloque_limpio, bloque_a_limpiar, TAMANIO_BLOQUE - contador);

	bloque->bloque = bloque_a_limpiar;

	bloque->tamanio_bloque = TAMANIO_BLOQUE - contador;

	//free(bloque_a_limpiar);

	return bloque;
}

//LOG
void loguearme(char *ruta, char *nombre_proceso, bool mostrar_por_consola,
		t_log_level nivel, const char *mensaje) {
	pthread_mutex_lock(&mutex_para_logs);
	t_log *log;

	switch (nivel) {
	case (LOG_LEVEL_ERROR):
		log = log_create(ruta, nombre_proceso, mostrar_por_consola, nivel);
		log_error(log, mensaje);
		log_destroy(log);
		pthread_mutex_unlock(&mutex_para_logs);
		break;

	case (LOG_LEVEL_DEBUG):
		log = log_create(ruta, nombre_proceso, mostrar_por_consola, nivel);
		log_debug(log, mensaje);
		log_destroy(log);
		pthread_mutex_unlock(&mutex_para_logs);
		break;

	case (LOG_LEVEL_INFO):
		log = log_create(ruta, nombre_proceso, mostrar_por_consola, nivel);
		log_info(log, mensaje);
		log_destroy(log);
		pthread_mutex_unlock(&mutex_para_logs);
		break;

	case (LOG_LEVEL_TRACE):
		log = log_create(ruta, nombre_proceso, mostrar_por_consola, nivel);
		log_trace(log, mensaje);
		log_destroy(log);
		pthread_mutex_unlock(&mutex_para_logs);
		break;

	case (LOG_LEVEL_WARNING):
		log = log_create(ruta, nombre_proceso, mostrar_por_consola, nivel);
		log_warning(log, mensaje);
		pthread_mutex_unlock(&mutex_para_logs);
		log_destroy(log);
		break;
	}
}

rutas_temporales *crearDirectorioTemporal(char *path_dir, int nombre_job) {

	rutas_temporales *rutas = malloc(sizeof(rutas_temporales));

	struct stat st = { 0 };

	char *path_temp = malloc(strlen(path_dir) + strlen("/Job-") + 4);

	strcpy(path_temp, path_dir);

	strcat(path_temp, "/Job-");

	char *p = string_itoa(nombre_job);

	strcat(path_temp, p);

	free(p);

	if (stat(path_temp, &st) == -1) {

		mkdir(path_temp, 0700);
	}

	char * recibido = malloc(strlen(path_temp) + strlen("/Recibido") + 1);

	strcpy(recibido, path_temp);

	strcat(recibido, "/Recibido");

	struct stat sta = { 0 };

	if (stat(recibido, &sta) == -1) {
		mkdir(recibido, 0700);
	}

	rutas->ruta_recibido = recibido;

	rutas->ruta_temp = path_temp;

	return rutas;

}

void cambiarNombre(int numero) {

	char *archivo_mapeado;

	int tamanio_archivo = 0;

	int file_descriptor;

	char*convertido;

	file_descriptor = open(PATH, O_RDWR);
	struct stat file;

	fstat(file_descriptor, &file);

	tamanio_archivo = file.st_size;

	archivo_mapeado = mmap((caddr_t) 0, tamanio_archivo, PROT_WRITE, MAP_SHARED,
			file_descriptor, 0);

	convertido = string_itoa(numero);

	archivo_mapeado[tamanio_archivo - 2] = convertido[0];

	archivo_mapeado[tamanio_archivo - 1] = convertido[1];

	free(convertido);

	munmap(archivo_mapeado, tamanio_archivo);

}

void actualizarEstadoBloques(char *resultado_script, int nro_bloque,
		int nombre_job) {

	bool estaEnLaLista(trabajoPorJob *job_aux) {

		return job_aux->nombre_job == nombre_job;

	}

	trabajoPorJob* job_aux_esta = list_find(trabajos_de_jobs,
			(void *) estaEnLaLista);

	job_aux_esta->trabajo_bloques[nro_bloque] = resultado_script;

	return;
}

void aplicarMapABloque(char *script_mapper, int nro_bloque, char *ruta_tmp,
		int nombre_job) {

	bloque_mapeado *datos_bloque = getBloque(nro_bloque);

	char *comando_map, *resultado_script, *info_map;

	char *p = string_itoa(nro_bloque);

	char *pepe = string_itoa(nombre_job);

	comando_map = malloc(strlen(script_mapper) + strlen(" |sort") + 1); //el +7 es por el | sort

	resultado_script = malloc(strlen(ruta_tmp) + strlen("/resultadoMapBloque-") + 4	+ strlen(".txt") + 1);

	strcpy(resultado_script, ruta_tmp);

	strcat(resultado_script, "/resultadoMapBloque-");

	strcat(resultado_script, p);

	strcat(resultado_script, ".txt");

	strcpy(comando_map, script_mapper);

	strcat(comando_map, " |sort");

	info_map = malloc(strlen("Finalizacion con exito Map en bloque ") + 4 +strlen(" -> Job-")+4+1);

	strcpy(info_map, "Finalizacion con exito Map en bloque ");

	strcat(info_map, p);

	strcat(info_map," -> Job-");

	strcat(info_map,pepe);

	free(pepe);

	free(p);

	ejecutarScriptMap(comando_map, datos_bloque, resultado_script);

	pthread_mutex_lock(&mutex_actualizacion_listas);

	actualizarEstadoBloques(resultado_script, nro_bloque, nombre_job);

	pthread_mutex_unlock(&mutex_actualizacion_listas);

	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_TRACE, info_map);

	free(datos_bloque);

	free(comando_map);

	free(info_map);

	return;

}

void ejecutarScriptMap(char* command, bloque_mapeado* in, char* out) { //TODO que lea de mmap en lugar de un archivo

	char* magiaAEjecutar = malloc(strlen(command) + strlen(" > ") + (strlen(out) + 1));
	strcpy(magiaAEjecutar, command);

	strcat(magiaAEjecutar, " > ");

	strcat(magiaAEjecutar, out);

	FILE* magic = popen(magiaAEjecutar, "w");

	fwrite(in->bloque, 1, in->tamanio_bloque, magic);

	free(magiaAEjecutar);

	pclose(magic);

}

void ejecutarScriptReduce(char* command, char* in, char* out) { //TODO que lea de mmap en lugar de un archivo

	char* magiaAEjecutar = malloc(strlen(command) + strlen(" > ") + (strlen(out) + 1));
	strcpy(magiaAEjecutar, command);

	strcat(magiaAEjecutar, " > ");

	strcat(magiaAEjecutar, out);

	int file_descriptor;

	file_descriptor = open(in, O_RDWR);

	struct stat file;

	fstat(file_descriptor, &file);

	char *inMapeado = mmap((caddr_t) 0, file.st_size,PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0);

	FILE* magic = popen(magiaAEjecutar, "w");

	fwrite(inMapeado, 1, file.st_size, magic);

	munmap(inMapeado, file.st_size);

	close(file_descriptor);

	pclose(magic);

}

void inicializarListaTrabajoBloques(int nombre_jobBigote) {

	bool estaEnLaListaa(trabajoPorJob *job_aux) {

		return (job_aux->nombre_job == nombre_jobBigote);

	}
	trabajoPorJob* sacateLaGorra = list_find(trabajos_de_jobs, (void *) estaEnLaListaa);

	if (sacateLaGorra) {

		return;
	}

	trabajoPorJob *un_job = malloc(sizeof(trabajoPorJob));

	int i = 0;

	un_job->trabajo_bloques = malloc((cantidad_bloques) * 4 + 4); //el otro 4 es por el resultado del reduce

	un_job->nombre_job = nombre_jobBigote;

	un_job->recibido=0;

	un_job->ya_mostre_que_estoy=0;

	un_job->script_map_bajado = 0;

	un_job->script_reduce_bajado = 0;

	while (i <= (cantidad_bloques)) {

		un_job->trabajo_bloques[i] = NULL;

		i++;

	}

	list_add(trabajos_de_jobs, un_job);

	return;
}

void vaciarListaTrabajoBloques(trabajoPorJob *un_job) { //cant de bloques +1 por el reduce

	int i = 0;

	while (i <= cantidad_bloques) {

		if (un_job->trabajo_bloques[i] != NULL) {

			free(un_job->trabajo_bloques[i]);

		}

		i++;

	}

	free(un_job->trabajo_bloques);

	free(un_job);

	return;
}

void loguearEntradaJob(int nombreJob){

	bool estaEnLaLista(trabajoPorJob *job_aux) {

			return job_aux->nombre_job == nombreJob;

		}

		trabajoPorJob* resultado = list_find(trabajos_de_jobs,
				(void*) estaEnLaLista);
	if(resultado->ya_mostre_que_estoy==0){

		char * mostrar_salida = malloc(strlen("Comienzo trabajo Job-")+4+1);
		strcpy(mostrar_salida,"Comienzo trabajo Job-");
		char*p=string_itoa(nombreJob);
		strcat(mostrar_salida,p);
		free(p);
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO,mostrar_salida);
		free(mostrar_salida);
		resultado->ya_mostre_que_estoy=1;
	}

	return;

}
void loguearSalidaJob(int nombreJob){

	bool estaEnLaLista(trabajoPorJob *job_aux) {

			return job_aux->nombre_job == nombreJob;

		}

		trabajoPorJob* resultado = list_find(trabajos_de_jobs,
				(void*) estaEnLaLista);
	if(resultado->ya_mostre_que_estoy==1){

		char * mostrar_salida = malloc(strlen("Finalizacion trabajo -> Job-")+4+1);
		strcpy(mostrar_salida,"Finalizacion trabajo -> Job-");
		char*p=string_itoa(nombreJob);
		strcat(mostrar_salida,p);
		free(p);
		loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO,mostrar_salida);
		free(mostrar_salida);
		resultado->ya_mostre_que_estoy=0;

	}

	return;
}

int ejecutarMap(trabajoMap *datos_map) {

	int resultado;

	char *ruta_mapper;

	lectura *leido = leerConfiguracion();

	char * p;

	p = string_itoa(datos_map->nroBloque);

	char * info_comienzo_map = malloc(strlen("Comienzo map en bloque ") + 4 + strlen(" -> Job-") +4 + 1);

	strcpy(info_comienzo_map,"Comienzo map en bloque ");

	strcat(info_comienzo_map,p);

	strcat(info_comienzo_map," -> Job-");

	char * pepe = string_itoa(datos_map->nombreJob);

	strcat(info_comienzo_map,pepe);

	pthread_mutex_lock(&mutex_actualizacion_listas);

	inicializarListaTrabajoBloques(datos_map->nombreJob);

	loguearEntradaJob(datos_map->nombreJob);

	pthread_mutex_unlock(&mutex_actualizacion_listas);

	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_TRACE, info_comienzo_map);

	free(info_comienzo_map);

	rutas_temporales *ruta_temp = crearDirectorioTemporal(leido->dirTemp,
			datos_map->nombreJob);

	ruta_mapper = bajarScriptAPlano(datos_map->instruccionMap,
			datos_map->tamanioScriptMap, ruta_temp->ruta_temp, 0,datos_map->nombreJob);

	aplicarMapABloque(ruta_mapper, datos_map->nroBloque, ruta_temp->ruta_temp,
			datos_map->nombreJob);

	liberarLectura(leido);

	liberarRutasTemporales(ruta_temp);

	free(ruta_mapper);

	free(datos_map->instruccionMap);

	free(datos_map);

	free(p);

	free(pepe);

	return resultado = 1;
}

char *bajarScriptAPlano(char *datos_script, int tamanio_script, char *ruta_temp,
		int map_reduce, int nom_job) {
	pthread_mutex_lock(&mutex_bajar_script);
	char *ruta_script;

	int file_descriptor;

	bool estaEnLaLista(trabajoPorJob *job_aux) {

				return job_aux->nombre_job == nom_job;

			}

	trabajoPorJob* resultado = list_find(trabajos_de_jobs,(void*) estaEnLaLista);

	if (map_reduce == 0) {

		ruta_script = malloc(strlen(ruta_temp) + 15); // /mapper.script (14 lugares)

		strcpy(ruta_script, ruta_temp);

		strcat(ruta_script, "/mapper.script");

		if(resultado->script_map_bajado == 0){

			crearArchivoVacio(ruta_script, tamanio_script);

			file_descriptor = open(ruta_script, O_RDWR);

			char *archivo_script = mmap((caddr_t) 0, tamanio_script,
			PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0);

			memcpy(archivo_script, datos_script, tamanio_script);

			msync(archivo_script, tamanio_script, MS_SYNC);

			munmap(archivo_script, tamanio_script);

			close(file_descriptor);

			char *magico = malloc(11 + strlen(ruta_script)); //tengo que meter chmod 777 .(10 lugares)

			strcpy(magico, "chmod 777 ");

			strcat(magico, ruta_script);

			system(magico);

			free(magico);
			pthread_mutex_lock(&mutex_actualizacion_listas);
			resultado->script_map_bajado ++;
			pthread_mutex_unlock(&mutex_actualizacion_listas);

			pthread_mutex_unlock(&mutex_bajar_script);

			return ruta_script;

		}

		pthread_mutex_unlock(&mutex_bajar_script);

		return ruta_script;

	}

	else {

		ruta_script = malloc(strlen(ruta_temp) + 16); // /reducer.script (14 lugares)

		strcpy(ruta_script, ruta_temp);

		strcat(ruta_script, "/reducer.script");

		if(resultado->script_reduce_bajado == 0){

			crearArchivoVacio(ruta_script, tamanio_script);

			file_descriptor = open(ruta_script, O_RDWR);

			char *archivo_script = mmap((caddr_t) 0, tamanio_script,PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0);

			memcpy(archivo_script, datos_script, tamanio_script);

			msync(archivo_script, tamanio_script, MS_SYNC);

			munmap(archivo_script, tamanio_script);

			close(file_descriptor);

			char *magico = malloc(11 + strlen(ruta_script)); //tengo que meter chmod 777 .(10 lugares)

			strcpy(magico, "chmod 777 ");

			strcat(magico, ruta_script);

			system(magico);

			free(magico);

			pthread_mutex_lock(&mutex_actualizacion_listas);

			resultado->script_reduce_bajado ++;

			pthread_mutex_unlock(&mutex_actualizacion_listas);

			pthread_mutex_unlock(&mutex_bajar_script);

			return ruta_script;

		 }

		pthread_mutex_unlock(&mutex_bajar_script);

		return ruta_script;

	}
}

void destruirListaGlobal() {

	list_destroy_and_destroy_elements(trabajos_de_jobs,
			(void *) vaciarListaTrabajoBloques);
}

int llamameParaEjecutarReduceLocal(trabajoReduce *trabajo_reduce) {

	char *ruta_reducer;

	lectura *leido = leerConfiguracion();

	rutas_temporales *rutas_temp = crearDirectorioTemporal(leido->dirTemp,
			trabajo_reduce->nombreJob);

	ruta_reducer = bajarScriptAPlano(trabajo_reduce->instruccionReduce,
			trabajo_reduce->tamanioScriptReduce, rutas_temp->ruta_temp, 1,trabajo_reduce->nombreJob);

	aplicarReduceLocal(ruta_reducer, rutas_temp->ruta_temp,
			trabajo_reduce->nombreJob);

	free(ruta_reducer);

	liberarRutasTemporales(rutas_temp);

	return 1;
}

void liberarRutasTemporales(rutas_temporales* estructura) {
	free(estructura->ruta_recibido);
	free(estructura->ruta_temp);
	free(estructura);
}

void aplicarReduceLocal(char* ruta_reducer, char* ruta_temp, int nombreJob) {

	bool estaEnLaLista(trabajoPorJob *job_aux) {

		return job_aux->nombre_job == nombreJob;

	}

	trabajoPorJob* resultado = list_find(trabajos_de_jobs,
			(void*) estaEnLaLista);

	char* total = juntarResultadosMapeados(resultado->trabajo_bloques,
			ruta_temp, nombreJob);

	char *numJob = string_itoa(nombreJob);

	char *log_inicio_reduce_local = malloc(strlen("Comienzo Reduce Local -> Job-") + 4 + 1);

	strcpy(log_inicio_reduce_local,"Comienzo Reduce Local -> Job-");

	strcat(log_inicio_reduce_local,numJob);

	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO,log_inicio_reduce_local);

	char *resultadoScript = malloc(
			strlen(ruta_temp) + strlen("/reduceLocal.txt") + 1);

	strcpy(resultadoScript, ruta_temp);

	strcat(resultadoScript, "/reduceLocal.txt");

	char * comando = malloc(strlen("sort | ") + strlen(ruta_reducer) + 1);

	strcpy(comando, "sort | ");

	strcat(comando, ruta_reducer);

	ejecutarScriptReduce(comando, total, resultadoScript);

	pthread_mutex_lock(&mutex_actualizacion_listas);

	actualizarEstadoBloques(resultadoScript, (cantidad_bloques), nombreJob);

	pthread_mutex_unlock(&mutex_actualizacion_listas);

	char *log_reduce_local = malloc(strlen("Finalizacion con exito Reduce Local -> Job-") + 4 +1);

	strcpy(log_reduce_local,"Finalizacion con exito Reduce Local -> Job-");

	strcat(log_reduce_local, numJob);

	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO, log_reduce_local);

	free(log_reduce_local);

	free(numJob);

	remove(total);

	free(total);

	free(comando);

	return;

}

char* juntarResultadosMapeados(char** bloques_mapeados, char *ruta_tmp,
		int nombre_job) {

	char * resultadoTotal;

	//rutas_temporales *path_directorios = crearDirectorioTemporal(ruta_tmp,nombre_job);

	char *path_resultado = malloc(
			strlen(ruta_tmp) + strlen("/sumatoriaMap.txt") + 1);

	int tamanio_string = 0;

	int i = 0;

	while (i < cantidad_bloques) {

		if (bloques_mapeados[i] != NULL) {

			tamanio_string = tamanio_string + strlen(bloques_mapeados[i]) + 2;

		}
		i++;
	}

	resultadoTotal = malloc(
			tamanio_string + strlen("cat ") + strlen(ruta_tmp)
					+ strlen("/sumatoriaMap.txt") + 4); //cat

	strcpy(resultadoTotal, "cat ");

	i = 0;

	while (i < cantidad_bloques) {

		if (bloques_mapeados[i] != NULL) {

			strcat(resultadoTotal, bloques_mapeados[i]);

			strcat(resultadoTotal, " ");

		}
		i++;
	}

	strcat(resultadoTotal, "> ");

	strcpy(path_resultado, ruta_tmp);

	strcat(path_resultado, "/sumatoriaMap.txt");

	strcat(resultadoTotal, path_resultado);

	system(resultadoTotal);

	free(resultadoTotal);

	//liberarRutasTemporales(path_directorios);

	return path_resultado;
}

bloque_mapeado* enviarReduceLocal(int nombreJob) {

	pthread_mutex_lock(&mutex_actualizacion_listas);
	loguearSalidaJob(nombreJob);
	pthread_mutex_unlock(&mutex_actualizacion_listas);

	bloque_mapeado *bloque = malloc(sizeof(bloque_mapeado));

	bool estaEnLaLista(trabajoPorJob *job_aux) {

		return job_aux->nombre_job == nombreJob;

	}

	trabajoPorJob* resultado = list_find(trabajos_de_jobs,
			(void*) estaEnLaLista);

	//todo para MMAP
	int file_descriptor;

	file_descriptor = open(resultado->trabajo_bloques[cantidad_bloques],
	O_RDWR);
	struct stat file;
	fstat(file_descriptor, &file);
	bloque->tamanio_bloque = file.st_size;

	char* envio = malloc(file.st_size);

	char *bloque_mapeado;

	bloque_mapeado = mmap((caddr_t) 0, bloque->tamanio_bloque, PROT_WRITE,
	MAP_SHARED, file_descriptor, 0);

	memcpy(envio, bloque_mapeado, file.st_size);

	bloque->bloque = envio;

	munmap(bloque_mapeado, bloque->tamanio_bloque);

	loguearme(PATH_LOG, "Proceso Nodo", 0, LOG_LEVEL_INFO,"Enviado Reduce Local al Nodo Principal");


	return bloque;
}

bloque_mapeado* enviarResultadoFinal(int nombreJob) {

	lectura* resultado_leido = leerConfiguracion();

	rutas_temporales* rutas = crearDirectorioTemporal(resultado_leido->dirTemp,
			nombreJob);

	bloque_mapeado *bloque = malloc(sizeof(bloque_mapeado));

	char* ruta_resultado_final = malloc(
			strlen(rutas->ruta_temp) + strlen("/resultadoTotal.txt") + 1);
	//todo para MMAP

	strcpy(ruta_resultado_final, rutas->ruta_temp);
	strcat(ruta_resultado_final, "/resultadoTotal.txt");
	int file_descriptor;

	file_descriptor = open(ruta_resultado_final, O_RDWR);
	struct stat file;
	fstat(file_descriptor, &file);
	bloque->tamanio_bloque = file.st_size;

	char* envio = malloc(file.st_size);

	char *bloque_mapeado;

	bloque_mapeado = mmap((caddr_t) 0, bloque->tamanio_bloque, PROT_WRITE,
	MAP_SHARED, file_descriptor, 0);

	memcpy(envio, bloque_mapeado, file.st_size);

	bloque->bloque = envio;

	munmap(bloque_mapeado, bloque->tamanio_bloque);

	/*char * borro_carpeta_termino_enviar = malloc(strlen("rm -r ")+ strlen(rutas->ruta_temp)+1);
	strcpy(borro_carpeta_termino_enviar,"rm -r ");
	strcat(borro_carpeta_termino_enviar,rutas->ruta_temp);

	system(borro_carpeta_termino_enviar);

	free(borro_carpeta_termino_enviar);*/

	liberarLectura(resultado_leido);
	liberarRutasTemporales(rutas);

	free(ruta_resultado_final);

	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO,"Resultado Final enviado al FileSystem");
	char* ultimo_log = malloc(strlen("Trabajo Job-")+4+strlen(" finalizo exitosamente")+1);
	strcpy(ultimo_log,"Trabajo Job-");
	char *p = string_itoa(nombreJob);
	strcat(ultimo_log,p);
	free(p);
	strcat(ultimo_log," finalizo exitosamente");
	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO,ultimo_log);
	free(ultimo_log);

	return bloque;

}

bloque_mapeado* enviarMap(int nombreJob, int num_map) {

	pthread_mutex_lock(&mutex_actualizacion_listas);
	loguearSalidaJob(nombreJob);
	pthread_mutex_unlock(&mutex_actualizacion_listas);

	bloque_mapeado* bloque = malloc(sizeof(bloque_mapeado));

	bool estaEnLaLista(trabajoPorJob *job_aux) {

		return job_aux->nombre_job == nombreJob;

	}

	trabajoPorJob* resultado = list_find(trabajos_de_jobs,
			(void*) estaEnLaLista);

	int file_descriptor;
	file_descriptor = open(resultado->trabajo_bloques[num_map], O_RDWR);
	struct stat file;
	fstat(file_descriptor, &file);
	bloque->tamanio_bloque = file.st_size;

	char* envio = malloc(file.st_size);

	char *bloque_mapeado;

	bloque_mapeado = mmap((caddr_t) 0, bloque->tamanio_bloque, PROT_WRITE,
	MAP_SHARED, file_descriptor, 0);

	memcpy(envio, bloque_mapeado, file.st_size);

	bloque->bloque = envio;

	munmap(bloque_mapeado, bloque->tamanio_bloque);

	char * logueo_map_enviado=malloc(strlen("Envio Map ") + 4 + strlen(" al Nodo Principal")+1);
	strcpy(logueo_map_enviado,"Envio Map ");
	char * p = string_itoa(num_map);
	strcat(logueo_map_enviado,p);
	free(p);
	strcat(logueo_map_enviado," al Nodo Principal");


	loguearme(PATH_LOG, "Proceso Nodo", 0, LOG_LEVEL_INFO,logueo_map_enviado);

	free(logueo_map_enviado);

	return bloque;
}

void guardarRecibido(bloque_mapeado * bloque_recibido, int nombreJob) {

	lectura * resultado_lectura = leerConfiguracion();

	rutas_temporales * rutas = crearDirectorioTemporal(
			resultado_lectura->dirTemp, nombreJob);

	char* ruta_archivo_recibido = malloc(
			strlen(rutas->ruta_recibido) + strlen("/recibido-") + 4
					+ strlen(".txt")); //

	strcpy(ruta_archivo_recibido, rutas->ruta_recibido);

	strcat(ruta_archivo_recibido, "/recibido-");

	bool estaEnLaLista(trabajoPorJob *job_aux) {
			return job_aux->nombre_job == nombreJob;

		}

		trabajoPorJob* job_aux = list_find(trabajos_de_jobs,
				(void *) estaEnLaLista);

	char* r = string_itoa(job_aux->recibido);

	strcat(ruta_archivo_recibido, r);

	free(r);

	strcat(ruta_archivo_recibido, ".txt");

	crearArchivoVacio(ruta_archivo_recibido, bloque_recibido->tamanio_bloque);

	int file_descriptor;

	file_descriptor = open(ruta_archivo_recibido, O_RDWR);

	char *archivo_mapeado = mmap((caddr_t) 0, bloque_recibido->tamanio_bloque,
	PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0);

	memcpy(archivo_mapeado, bloque_recibido->bloque,
			bloque_recibido->tamanio_bloque);

	msync(archivo_mapeado, bloque_recibido->tamanio_bloque, MS_SYNC);

	munmap(archivo_mapeado, bloque_recibido->tamanio_bloque);

	close(file_descriptor);

	free(ruta_archivo_recibido);

	pthread_mutex_lock(&mutex_actualizacion_listas);

    job_aux->recibido = (job_aux->recibido +1);

	pthread_mutex_unlock(&mutex_actualizacion_listas);

	loguearme(PATH_LOG, "Proceso Nodo", 0, LOG_LEVEL_INFO,	"Recibo archivo para Trabajo Local");


	liberarRutasTemporales(rutas);

	liberarLectura(resultado_lectura);

	limpiarBloqueMapeado(bloque_recibido);

	return;
}

char* juntarTodasLasRutasDeRecibidos(char* ruta_tmp, int nombreJob) {

	rutas_temporales * rutas = crearDirectorioTemporal(ruta_tmp, nombreJob);


	bool estaEnLaLista(trabajoPorJob *job_aux) {
			return job_aux->nombre_job == nombreJob;

		}

		trabajoPorJob* job_aux = list_find(trabajos_de_jobs,
				(void *) estaEnLaLista);
	char* ruta_para_cat = malloc(
			(strlen(rutas->ruta_recibido) + strlen("/recibido-") + 5
					+ strlen(".txt ") + 1) * job_aux->recibido);

	strcpy(ruta_para_cat,"");

	int i = 0;
	while (i < job_aux->recibido) {
		strcat(ruta_para_cat, rutas->ruta_recibido);
		strcat(ruta_para_cat, "/recibido-");
		char*r = string_itoa(i);
		strcat(ruta_para_cat, r);
		free(r);
		strcat(ruta_para_cat, ".txt ");
		i++;
	}

	return ruta_para_cat;
}

int hacerReduceTotalSinCombiner(char* datos_script, int nombre_job,
		int tamanio_script) {

	char* comando_cat;

	lectura *resultado_lectura = leerConfiguracion();

	char *numJob = string_itoa(nombre_job);

	char *log_inicio_reduce_total = malloc(strlen("Comienzo ejecucion Reduce Total Sin Combiner -> Job-") + 4 + 1);

	strcpy(log_inicio_reduce_total,"Comienzo ejecucion Reduce Total Sin Combiner -> Job-");

	strcat(log_inicio_reduce_total,numJob);

	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO,log_inicio_reduce_total);

	free(log_inicio_reduce_total);

	rutas_temporales *rutas = crearDirectorioTemporal(resultado_lectura->dirTemp,nombre_job);

	char* cantidad_recibidos = juntarTodasLasRutasDeRecibidos(
			resultado_lectura->dirTemp, nombre_job);

	bool estaEnLaLista(trabajoPorJob *job_aux) {
		return job_aux->nombre_job == nombre_job;

	}

	trabajoPorJob* job_aux = list_find(trabajos_de_jobs,
			(void *) estaEnLaLista);

	int tamanio_string = 0;

	int i = 0;

	while (i < cantidad_bloques) {

		if (job_aux->trabajo_bloques[i] != NULL) {

			tamanio_string = tamanio_string
					+ strlen(job_aux->trabajo_bloques[i]) + 2;

		}
		i++;
	}

	comando_cat = malloc(
			strlen("cat ") + strlen(cantidad_recibidos) + tamanio_string
					+ strlen("> ") + strlen("/resultadoMapTotal.txt") + strlen(rutas->ruta_temp)+strlen ("|sort ") + 1);
	strcpy(comando_cat, "cat ");
	strcat(comando_cat, cantidad_recibidos);

	i = 0;

	while (i < cantidad_bloques) {

		if (job_aux->trabajo_bloques[i] != NULL) {

			strcat(comando_cat, job_aux->trabajo_bloques[i]);

			strcat(comando_cat, " ");

		}
		i++;
	}

	strcat(comando_cat,"|sort ");

	strcat(comando_cat, "> ");

	strcat(comando_cat,rutas->ruta_temp);

	strcat(comando_cat,"/todosLosMaps.txt");

	system(comando_cat);

	free(cantidad_recibidos);

	free(comando_cat);

//	rutas_temporales * rutas = crearDirectorioTemporal(
//			resultado_lectura->dirTemp, nombre_job);

	char *ruta_reducer = bajarScriptAPlano(datos_script, tamanio_script,
			rutas->ruta_temp, 1,nombre_job);

	char * ruta_total_maps = malloc(
			strlen(rutas->ruta_temp) + strlen("/todosLosMaps.txt") + 1);

	strcpy(ruta_total_maps, rutas->ruta_temp);

	strcat(ruta_total_maps, "/todosLosMaps.txt");

	char * ruta_resultado = malloc(
			strlen(rutas->ruta_temp) + strlen("/resultadoTotal.txt") + 1);

	strcpy(ruta_resultado,rutas->ruta_temp);

	strcat(ruta_resultado,"/resultadoTotal.txt");

	ejecutarScriptReduce(ruta_reducer, ruta_total_maps, ruta_resultado);

	remove(ruta_total_maps);

	free(ruta_total_maps);

	free(ruta_resultado);

	liberarRutasTemporales(rutas);

	liberarLectura(resultado_lectura);

	char *log_reduce_fin = malloc(strlen("Finalizacion con exito Reduce Total Sin Combiner -> Job-") + 4 +1);

	strcpy(log_reduce_fin,"Finalizacion con exito Reduce Total Sin Combiner -> Job-");

	strcat(log_reduce_fin, numJob);

	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO, log_reduce_fin);

	free(log_reduce_fin);

	free(numJob);

	return 1;
}

int hacerReduceTotalConCombiner(char* datos_script, int nombre_job,
		int tamanio_script) {

	lectura * resultado_lectura = leerConfiguracion();

	char *log_reduce_inicio = malloc(strlen("Comienzo ejecucion Reduce Total Con Combiner -> Job-") + 4 +1);

	char *numJob = string_itoa(nombre_job);

	strcpy(log_reduce_inicio,"Comienzo ejecucion Reduce Total Con Combiner -> Job-");

	strcat(log_reduce_inicio, numJob);

	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO, log_reduce_inicio);

	free(log_reduce_inicio);

	rutas_temporales * rutas = crearDirectorioTemporal(
			resultado_lectura->dirTemp, nombre_job);

	char* cantidad_recibidos = juntarTodasLasRutasDeRecibidos(
			resultado_lectura->dirTemp, nombre_job);

	char *ruta_reducer = bajarScriptAPlano(datos_script, tamanio_script,
			rutas->ruta_temp, 1,nombre_job);

	char * ruta_resultado = malloc(
			strlen(rutas->ruta_temp) + strlen("/resultadoTotal.txt") + 1);

	char *comando_reduce = malloc(strlen(ruta_reducer) + strlen("sort | ") + 1);

	bool estaEnLaLista(trabajoPorJob *job_aux) {
		return job_aux->nombre_job == nombre_job;

	}

	trabajoPorJob* job_aux = list_find(trabajos_de_jobs,
			(void *) estaEnLaLista);

	char * todosLosRecibidosYLocal = malloc(
			strlen("cat ") + strlen(cantidad_recibidos)
					+ strlen(job_aux->trabajo_bloques[cantidad_bloques])
					+ strlen("> ") + strlen(rutas->ruta_temp)
					+ strlen("/todosLosReduces.txt") + 1);

	strcpy(todosLosRecibidosYLocal, "cat ");

	strcat(todosLosRecibidosYLocal, cantidad_recibidos);

	strcat(todosLosRecibidosYLocal, job_aux->trabajo_bloques[cantidad_bloques]);

	strcat(todosLosRecibidosYLocal, ">");

	char * entradaScript = malloc(
			strlen(rutas->ruta_temp) + strlen("/todosLosReduces.txt") + 1);

	strcpy(entradaScript, rutas->ruta_temp);

	strcat(entradaScript, "/todosLosReduces.txt");

	strcat(todosLosRecibidosYLocal, entradaScript);

	system(todosLosRecibidosYLocal);

	free(todosLosRecibidosYLocal);

	strcpy(comando_reduce, "sort | ");

	strcat(comando_reduce, ruta_reducer);

	strcpy(ruta_resultado,rutas->ruta_temp);

	strcat(ruta_resultado,"/resultadoTotal.txt");

	ejecutarScriptReduce(comando_reduce, entradaScript, ruta_resultado);

	remove(entradaScript);

	free(entradaScript);

	liberarLectura(resultado_lectura);

	liberarRutasTemporales(rutas);

	free(ruta_reducer);

	free(ruta_resultado);

	free(comando_reduce);

	char *log_reduce_fin = malloc(strlen("Finalizacion con exito Reduce Total Con Combiner -> Job-") + 4 +1);

	strcpy(log_reduce_fin,"Finalizacion con exito Reduce Total Con Combiner -> Job-");

	strcat(log_reduce_fin, numJob);

	loguearme(PATH_LOG, "Proceso Nodo", 1, LOG_LEVEL_INFO, log_reduce_fin);

	free(log_reduce_fin);

	free(numJob);

	return 1;
}

void limpiarBloqueMapeado(bloque_mapeado * limpiar) {
	//free(limpiar->bloque);
	free(limpiar);

	return;
}

