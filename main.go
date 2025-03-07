package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// OrderUpdate representa la estructura del mensaje que enviaremos a la API
type OrderUpdate struct {
	ID     int    `json:"id"`
	Status string `json:"status"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func updateOrderAPI(orderData []byte) (string, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Verificar que el mensaje tiene el formato JSON esperado
	var orderUpdate OrderUpdate
	if err := json.Unmarshal(orderData, &orderUpdate); err != nil {
		return "", err
	}

	// Crear la petición PUT con los datos recibidos
	req, err := http.NewRequest("PUT", "http://localhost:8000/orders/consumer", bytes.NewBuffer(orderData))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func main() {
	conn, err := amqp.Dial("amqp://max:123@54.172.185.28:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	msgs, err := ch.Consume(
		"orders.created", // queue
		"",               // consumer
		true,             // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf(" [x] Mensaje recibido: %s", d.Body)

			// Enviar los datos recibidos a la API
			response, err := updateOrderAPI(d.Body)
			if err != nil {
				log.Printf(" [!] Error al actualizar la orden: %s", err)
				continue
			}

			log.Printf(" [✓] Respuesta de la API: %s", response)
		}
	}()

	log.Printf(" [*] Esperando mensajes. Para salir presiona CTRL+C")
	<-forever
}
