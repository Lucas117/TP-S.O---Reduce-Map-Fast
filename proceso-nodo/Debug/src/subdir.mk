################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/Proceso\ Nodo.c \
../src/funcionesNodo.c \
../src/hSerializadores.c \
../src/hSockets.c 

OBJS += \
./src/Proceso\ Nodo.o \
./src/funcionesNodo.o \
./src/hSerializadores.o \
./src/hSockets.o 

C_DEPS += \
./src/Proceso\ Nodo.d \
./src/funcionesNodo.d \
./src/hSerializadores.d \
./src/hSockets.d 


# Each subdirectory must supply rules for building sources it contributes
src/Proceso\ Nodo.o: ../src/Proceso\ Nodo.c
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"src/Proceso Nodo.d" -MT"src/Proceso\ Nodo.d" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '

src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


