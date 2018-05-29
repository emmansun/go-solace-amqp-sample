package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	cli "gopkg.in/urfave/cli.v1"
	"pack.ag/amqp"
)

type MessageListener func(message *amqp.Message) error

type Consumer struct {
	Client   *amqp.Client
	Session  *amqp.Session
	Receiver *amqp.Receiver
	Context  context.Context
}

var consumer *Consumer

var (
	AmqpUrlFlag = cli.StringFlag{
		Name:  "url",
		Usage: "amqp url, for example: amqp://devuser:devpwd@10.222.49.29:5672",
	}
	DestinationFlag = cli.StringFlag{
		Name:  "destination",
		Usage: "destination, for example: Q/testQueue",
	}
)

func (consumer *Consumer) receiveMsg(callback MessageListener, forceAck bool) {
	log.Println("Start listening ...")
	for {
		log.Println("Waiting ...")
		// Receive next message
		msg, err := consumer.Receiver.Receive(consumer.Context)
		if err != nil {
			log.Fatalln("Reading message from AMQP: ", err)
		}

		err = callback(msg)
		if err != nil {
			log.Println("Message handle failure: ", err)
		}
		if forceAck || err == nil {
			// Accept message
			msg.Accept()
		}
	}
}

func (consumer *Consumer) close(seconds time.Duration) {
	if consumer.Receiver != nil {
		ctx, cancel := context.WithTimeout(consumer.Context, time.Second*seconds)
		consumer.Receiver.Close(ctx)
		cancel()
	}
	if consumer.Client != nil {
		consumer.Client.Close()
	}
}

func processMessage(msg *amqp.Message) error {
	log.Printf("Message received: %s\n", msg.GetData())
	return nil
}

func serveListener(ctx *cli.Context) error {
	if !ctx.GlobalIsSet(AmqpUrlFlag.Name) || !ctx.GlobalIsSet(DestinationFlag.Name) {
		cli.ShowAppHelpAndExit(ctx, -1)
	}
	consumer = new(Consumer)
	consumer.Context = context.Background()
	client, err := amqp.Dial(ctx.GlobalString(AmqpUrlFlag.Name))
	if err != nil {
		log.Printf("Dialing AMQP server: %v\n", err)
		return err
	}
	consumer.Client = client
	// Open a session
	session, err := client.NewSession()
	if err != nil {
		log.Printf("Creating AMQP session: %v\n", err)
		return err
	}
	consumer.Session = session
	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress(ctx.GlobalString(DestinationFlag.Name)),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Printf("Creating receiver link: %v\n", err)
		return err
	}
	consumer.Receiver = receiver
	go consumer.receiveMsg(processMessage, true)
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "sample amqp consumer application."
	app.Flags = []cli.Flag{AmqpUrlFlag, DestinationFlag}
	app.Action = serveListener
	err := app.Run(os.Args)
	if err != nil {
		if consumer != nil {
			consumer.close(3)
		}
		log.Fatalln(err)
	}
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	defer signal.Stop(sigchan)
	log.Printf("Got interrupt, shutting down...singal=%v\n", <-sigchan)
	if consumer != nil {
		consumer.close(3)
	}
	log.Println("AMQP Listener gracefully stopped")
}
