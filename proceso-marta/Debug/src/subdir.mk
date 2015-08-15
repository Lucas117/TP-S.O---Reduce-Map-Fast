################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/FuncionesMARTA.c \
../src/Proceso\ Marta.c \
../src/hSerializadores.c \
../src/hSockets.c 

OBJS += \
./src/FuncionesMARTA.o \
./src/Proceso\ Marta.o \
./src/hSerializadores.o \
./src/hSockets.o 

C_DEPS += \
./src/FuncionesMARTA.d \
./src/Proceso\ Marta.d \
./src/hSerializadores.d \
./src/hSockets.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '

src/Proceso\ Marta.o: ../src/Proceso\ Marta.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"src/Proceso Marta.d" -MT"src/Proceso\ Marta.d" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


