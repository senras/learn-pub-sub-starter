package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connection_str := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connection_str)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ successfully.")

	channel, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to open a channel:", err)
		return
	}
	defer channel.Close()
	fmt.Println("Channel opened successfully.")

	err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})
	if err != nil {
		fmt.Println("Failed to publish JSON:", err)
		return
	}

	// Create a channel to receive OS signals
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	// Wait for an interrupt signal
	<-sigchan
	fmt.Println("Shutting down Peril server...")
	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			fmt.Println("Sending a pause message..")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
		case "resume":
			fmt.Println("Sending a resume message..")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "quit":
			fmt.Println("Quitting the server...")
			return
		default:
			fmt.Println("Unknown command. Please use 'pause', 'resume', or 'quit'.")
		}
	}

}
