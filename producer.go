package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

func main() {
	// Cargar variables de entorno
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error cargando el archivo .env: %s", err)
	}

	// Obtener configuración de RabbitMQ desde variables de entorno
	host := os.Getenv("RABBITMQ_HOST")
	port := os.Getenv("RABBITMQ_PORT")
	user := os.Getenv("RABBITMQ_USER")
	password := os.Getenv("RABBITMQ_PASSWORD")
	exchange := os.Getenv("RABBITMQ_EXCHANGE")

	// Elegir una cola basada en un argumento de línea de comandos o usar MQ135 por defecto
	queueName := os.Getenv("RABBITMQ_QUEUE_MQ135")
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "ky026":
			queueName = os.Getenv("RABBITMQ_QUEUE_KY026")
		case "mq2":
			queueName = os.Getenv("RABBITMQ_QUEUE_MQ2")
		case "mq135":
			queueName = os.Getenv("RABBITMQ_QUEUE_MQ135")
		case "dht22":
			queueName = os.Getenv("RABBITMQ_QUEUE_DHT22")
		default:
			log.Printf("Sensor no reconocido, usando mq135 por defecto")
		}
	}

	// Construir URL de conexión
	amqpURL := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, password, host, port)

	// Conectar a RabbitMQ
	log.Printf("Conectando a RabbitMQ en %s", amqpURL)
	conn, err := amqp.Dial(amqpURL)
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

	// Declarar el exchange
	err = ch.ExchangeDeclare(
		exchange, // nombre
		"topic",  // tipo
		true,     // durable
		false,    // auto-delete
		false,    // internal
		false,    // no-wait
		nil,      // argumentos
	)
	if err != nil {
		log.Fatalf("Error al declarar el exchange: %s", err)
	}

	// Declarar una cola
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

	// Enlazar la cola al exchange
	err = ch.QueueBind(
		q.Name,   // nombre de la cola
		q.Name,   // routing key (mismo que el nombre de la cola)
		exchange, // exchange
		false,    // no-wait
		nil,      // argumentos
	)
	if err != nil {
		log.Fatalf("Error al enlazar la cola con el exchange: %s", err)
	}

	// Obtener cantidad de mensajes a enviar
	numMessages := 5
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &numMessages)
	}

	// Publicar mensajes
	for i := 1; i <= numMessages; i++ {
		body := fmt.Sprintf("Mensaje #%d para %s - %s", i, queueName, time.Now().Format(time.RFC3339))

		// Publicar mensaje
		err = ch.Publish(
			exchange, // exchange
			q.Name,   // routing key
			false,    // mandatory
			false,    // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent, // mensaje persistente
				ContentType:  "text/plain",
				Body:         []byte(body),
			})
		if err != nil {
			log.Fatalf("Error al publicar mensaje: %s", err)
		}

		log.Printf("Mensaje enviado a %s: %s", queueName, body)
		time.Sleep(500 * time.Millisecond)
	}

	log.Printf("Enviados %d mensajes a la cola %s", numMessages, queueName)
}
