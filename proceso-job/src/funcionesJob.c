#include "funcionesJob.h"

pthread_mutex_t mutexLogJOB = PTHREAD_MUTEX_INITIALIZER;

lectura* leerConfiguracion()
{

FILE *fp;

	fp = fopen(PATH, "r");
	lectura* a;
	if (fp) {
		a = malloc(sizeof(lectura));
		unsigned int tam;
		fclose(fp);
		t_config *archivo;
		archivo = config_create(PATH);


		tam=string_length(config_get_string_value(archivo,"IP_MARTA"));
		a->ipMarta=malloc(tam+1);
		strcpy(a->ipMarta,config_get_string_value(archivo,"IP_MARTA"));

		tam=string_length(config_get_string_value(archivo,"PUERTO_MARTA"));
		a->puertoMarta=malloc(tam+1);
		strcpy(a->puertoMarta,config_get_string_value(archivo, "PUERTO_MARTA"));

		tam=string_length(config_get_string_value(archivo,"PUERTO_SALIDA"));
		a->puertoSalida=malloc(tam+1);
		strcpy(a->puertoSalida,config_get_string_value(archivo, "PUERTO_SALIDA"));

		tam=string_length(config_get_string_value(archivo, "MAPPER"));
		a->mapper=malloc(tam+1);
		strcpy(a->mapper,config_get_string_value(archivo, "MAPPER"));

		tam=string_length(config_get_string_value(archivo, "REDUCER"));
		a->reducer=malloc(tam+1);
		strcpy(a->reducer,config_get_string_value(archivo, "REDUCER"));


		a->combiner=config_get_int_value(archivo, "COMBINER");

		a->nombreJob=config_get_int_value(archivo, "NOMBRE_JOB");

		a->archivos = string_split(config_get_string_value(archivo,"ARCHIVOS"),",");


		tam=string_length(config_get_string_value(archivo,"RESULTADO"));
		a->resultado=malloc(tam+1);
		strcpy(a->resultado,config_get_string_value(archivo,"RESULTADO"));


		config_destroy(archivo);
		return a;
	} else {
		printf("El archivo no existe\n");
	}

	return NULL;
}

void liberarLectura(lectura* leido)
{
	int i = 0;

	free(leido->ipMarta);
	free(leido->puertoMarta);
	free(leido->mapper);
	free(leido->reducer);

	while(leido->archivos[i]!=NULL){

		free(leido->archivos[i]);

		i++;
	}
	free(leido->puertoSalida);
	free(leido->archivos);
	free(leido->resultado);

}

void loguearme(char *ruta, char *nombre_proceso, bool mostrar_por_consola,
		t_log_level nivel, const char *mensaje) {

	pthread_mutex_lock(&mutexLogJOB);
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
	pthread_mutex_unlock(&mutexLogJOB);
}

script* mapearScriptsAMemoria(char *ruta_mapper, char *ruta_reducer){

	script *resultado=malloc(sizeof(script));

	struct stat mapper;
	struct stat reducer;

	int file_descriptor_mapper, file_descriptor_reducer, tamanio_mapper, tamanio_reducer;

	char *mapper_mapeado, *reducer_mapeado;

	file_descriptor_mapper = open(ruta_mapper, O_RDONLY);
	file_descriptor_reducer = open(ruta_reducer, O_RDONLY);


	fstat(file_descriptor_mapper, &mapper);
	fstat(file_descriptor_reducer, &reducer);

	tamanio_mapper = mapper.st_size;

	tamanio_reducer = reducer.st_size;

	mapper_mapeado = mmap((caddr_t) 0, tamanio_mapper, PROT_READ,
			MAP_SHARED, file_descriptor_mapper, 0);

	reducer_mapeado = mmap((caddr_t) 0, tamanio_mapper, PROT_READ,
				MAP_SHARED, file_descriptor_reducer, 0);


	resultado->insruccionReduce=reducer_mapeado;
	resultado->instruccionMap=mapper_mapeado;

	resultado->tamanioScriptMap=tamanio_mapper;
	resultado->tamanioScriptReduce=tamanio_reducer;

	close (file_descriptor_mapper);
	close (file_descriptor_reducer);


	return resultado;
}



