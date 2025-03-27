package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	// Conectar a RabbitMQ
	conn, err := amqp.Dial("amqp://admin:password@54.197.14.45:5672/")
	if err != nil {
		log.Fatalf("Error conectando a RabbitMQ: %s", err)
	}
	defer conn.Close()

	// Crear un canal
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error al abrir un canal: %s", err)
	}
	defer ch.Close()

	// Declarar una cola
	queueName := "mi_cola"
	q, err := ch.QueueDeclare(
		queueName, // nombre
		true,      // durable
		false,     // eliminar cuando no se use
		false,     // exclusiva
		false,     // no-wait
		nil,       // argumentos
	)
	if err != nil {
		log.Fatalf("Error al declarar la cola: %s", err)
	}

	// Configurar QoS (calidad de servicio)
	err = ch.Qos(
		1,     // prefetch count - número de mensajes por consumidor
		0,     // prefetch size - tamaño en bytes
		false, // global
	)
	if err != nil {
		log.Fatalf("Error al configurar QoS: %s", err)
	}

	// Registrar consumidor
	msgs, err := ch.Consume(
		q.Name, // cola
		"",     // consumidor (vacío genera uno aleatorio)
		false,  // auto-ack (false para reconocimiento manual)
		false,  // exclusivo
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Error al registrar el consumidor: %s", err)
	}

	// Canal para capturar señales de sistema
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	// Canal para sincronizar el cierre
	doneChan := make(chan struct{})

	// Procesar mensajes en goroutine
	go func() {
		for msg := range msgs {
			log.Printf("Mensaje recibido: %s", msg.Body)

			// Simular procesamiento
			time.Sleep(1 * time.Second)

			// Confirmar el mensaje como procesado
			err := msg.Ack(false)
			if err != nil {
				log.Printf("Error al confirmar mensaje: %s", err)
			}
		}
		close(doneChan)
	}()

	log.Printf("Consumidor iniciado. Esperando mensajes en la cola: %s", queueName)
	log.Println("Presiona CTRL+C para salir")

	// Esperar señal de terminación
	<-stopChan
	log.Println("Cerrando consumidor...")

	// Cerrar canal y conexión
	if err := ch.Close(); err != nil {
		log.Printf("Error al cerrar canal: %s", err)
	}
	if err := conn.Close(); err != nil {
		log.Printf("Error al cerrar conexión: %s", err)
	}

	// Esperar a que la goroutine termine
	<-doneChan
	log.Println("Consumidor cerrado correctamente")
}
