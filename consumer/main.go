package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	cli "gopkg.in/urfave/cli.v1"
	"pack.ag/amqp"
)

type MessageListener func(recv *receiver, message *amqp.Message) error

type receiver struct {
	Receiver *amqp.Receiver
	Context  context.Context
	index    int
}

type Consumer struct {
	url           string
	destination   string
	reconnect     chan struct{}
	reconnectOnce *sync.Once
	Client        *amqp.Client
	Session       *amqp.Session
	Recievers     []receiver
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

func (consumer *receiver) receiveMsg(reconnect chan struct{}, callback MessageListener, forceAck bool) {
	log.Printf("[%v-%v] Start listening ...\n", consumer.Receiver.Address(), consumer.index)
	for {
		log.Println("Waiting ...")
		// Receive next message
		msg, err := consumer.Receiver.Receive(consumer.Context)
		if err != nil {
			if err == amqp.ErrLinkClosed || err == amqp.ErrSessionClosed {
				break
			} else if _, ok := err.(net.Error); ok {
				select {
				case reconnect <- struct{}{}:
					log.Println("Notified to reconnect.")
					break
				default:
				}
				log.Printf("[%v-%v] Reading message from AMQP: %v\n", consumer.Receiver.Address(), consumer.index, err)
				break
			}
		}

		err = callback(consumer, msg)
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
	for _, recv := range consumer.Recievers {
		ctx, cancel := context.WithTimeout(recv.Context, time.Second*seconds)
		recv.Receiver.Close(ctx)
		cancel()
	}
	if consumer.Session != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*seconds)
		consumer.Session.Close(ctx)
		cancel()
	}
	if consumer.Client != nil {
		consumer.Client.Close()
	}
}

func processMessage(consumer *receiver, msg *amqp.Message) error {
	log.Printf("[%v-%v] Message received: %s\n", consumer.Receiver.Address(), consumer.index, msg.GetData())
	return nil
}

func (consumer *Consumer) prepareConsumer() error {
	consumer.close(2)
	consumer.Recievers = make([]receiver, 5)
	consumer.reconnect = make(chan struct{}, 5)
	consumer.reconnectOnce = &sync.Once{}
	//TODO: set 0 time out future once the issue fixed.
	client, err := amqp.Dial(consumer.url)
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
	for i := 0; i < len(consumer.Recievers); i++ {
		recv, err := session.NewReceiver(
			amqp.LinkSourceAddress(consumer.destination),
			amqp.LinkCredit(2),
		)
		if err != nil {
			log.Printf("Creating receiver link: %v\n", err)
			return err
		}
		consumer.Recievers[i] = receiver{Receiver: recv, Context: context.Background(), index: i}
		go consumer.Recievers[i].receiveMsg(consumer.reconnect, processMessage, true)
	}
	go func(consumer *Consumer) {
		<-consumer.reconnect
		consumer.reconnectOnce.Do(func() {
			consumer.close(3)
			for {
				log.Println("re-connecting after 3 sends ...")
				err := consumer.prepareConsumer()
				if err != nil {
					time.Sleep(5 * time.Second)
					continue
				}
				break
			}

		})
	}(consumer)
	return nil
}

func serveListener(ctx *cli.Context) error {
	if !ctx.GlobalIsSet(AmqpUrlFlag.Name) || !ctx.GlobalIsSet(DestinationFlag.Name) {
		cli.ShowAppHelpAndExit(ctx, -1)
	}
	consumer = new(Consumer)
	consumer.url = ctx.GlobalString(AmqpUrlFlag.Name)
	consumer.destination = ctx.GlobalString(DestinationFlag.Name)
	return consumer.prepareConsumer()
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
