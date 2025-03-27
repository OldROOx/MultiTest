package main

import (
	"fmt"
	"log"
	"os"
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

	// Obtener cantidad de mensajes a enviar
	numMessages := 5
	if len(os.Args) > 1 {
		fmt.Sscanf(os.Args[1], "%d", &numMessages)
	}

	// Publicar mensajes
	for i := 1; i <= numMessages; i++ {
		body := fmt.Sprintf("Mensaje #%d - %s", i, time.Now().Format(time.RFC3339))

		// Publicar mensaje
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent, // mensaje persistente
				ContentType:  "text/plain",
				Body:         []byte(body),
			})
		if err != nil {
			log.Fatalf("Error al publicar mensaje: %s", err)
		}

		log.Printf("Mensaje enviado: %s", body)
		time.Sleep(500 * time.Millisecond)
	}

	log.Printf("Enviados %d mensajes", numMessages)
}
