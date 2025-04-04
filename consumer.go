package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
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
