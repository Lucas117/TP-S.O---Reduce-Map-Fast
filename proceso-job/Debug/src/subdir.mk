################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/Proceso\ Job.c \
../src/funcionesJob.c \
../src/hSerializadores.c \
../src/hSockets.c 

OBJS += \
./src/Proceso\ Job.o \
./src/funcionesJob.o \
./src/hSerializadores.o \
./src/hSockets.o 

C_DEPS += \
./src/Proceso\ Job.d \
./src/funcionesJob.d \
./src/hSerializadores.d \
./src/hSockets.d 


# Each subdirectory must supply rules for building sources it contributes
src/Proceso\ Job.o: ../src/Proceso\ Job.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"src/Proceso Job.d" -MT"src/Proceso\ Job.d" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '

src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


