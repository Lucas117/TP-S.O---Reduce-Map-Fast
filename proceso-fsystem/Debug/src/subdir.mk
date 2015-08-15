################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/FuncionesFS.c \
../src/Proceso\ FileSystem.c \
../src/hSerializadores.c \
../src/hSockets.c 

OBJS += \
./src/FuncionesFS.o \
./src/Proceso\ FileSystem.o \
./src/hSerializadores.o \
./src/hSockets.o 

C_DEPS += \
./src/FuncionesFS.d \
./src/Proceso\ FileSystem.d \
./src/hSerializadores.d \
./src/hSockets.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '

src/Proceso\ FileSystem.o: ../src/Proceso\ FileSystem.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"src/Proceso FileSystem.d" -MT"src/Proceso\ FileSystem.d" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


