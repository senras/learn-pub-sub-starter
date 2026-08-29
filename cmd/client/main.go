package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connection_str := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connection_str)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to welcome client: %v", err)
	}

	_, pause_queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.SimpleQueueTypeTransient,
	)
	if err != nil {
		log.Fatalf("Failed to declare and bind queue: %v", err)
	}
	fmt.Printf("Pause Queue %v declared and bound to Peril exchange with routing key %v\n", pause_queue.Name, routing.PauseKey+"."+username)

	gameState := gamelogic.NewGameState(username)
	for {
		input := gamelogic.GetInput()
		switch input[0] {
		case "spawn":
			gameState.CommandSpawn(input)
		case "move":
			gameState.CommandMove(input)
			fmt.Printf("Command \"%s %s %s\" was successful", input[0], input[1], input[2])
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command.")
		}
	}

	/* 	// Wait for ctrl+c
	   	sigchan := make(chan os.Signal, 1)
	   	signal.Notify(sigchan, os.Interrupt)
	   	<-sigchan
	   	fmt.Println("Shutting down Peril client	...") */

}
