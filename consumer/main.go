package main

import (
	"context"
	"log"
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
	Client        *amqp.Client
	Session       *amqp.Session
	Recievers     []receiver
	reconnect     chan struct{}
	reconnectOnce *sync.Once
	shutdown      chan struct{}
	done          sync.WaitGroup
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
			log.Printf("[%v-%v] Got excpetion %v\n", consumer.Receiver.Address(), consumer.index, err)
			if err == amqp.ErrLinkClosed || err == amqp.ErrSessionClosed {
				break
			} else {
				select {
				case reconnect <- struct{}{}:
					log.Printf("[%v-%v] Notified to reconnect.\n", consumer.Receiver.Address(), consumer.index)
					break
				default:
				}
				break
			}
		} else {
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
	log.Printf("[%v-%v] Receiver go routine exit.", consumer.Receiver.Address(), consumer.index)
}

func (consumer *Consumer) close(seconds time.Duration) {
	if consumer.Recievers != nil {
		for _, recv := range consumer.Recievers {
			if recv.Context != nil {
				ctx, cancel := context.WithTimeout(recv.Context, time.Second*seconds)
				recv.Receiver.Close(ctx)
				cancel()
			}
		}
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
	if msg != nil {
		log.Printf("[%v-%v] Message received: %s\n", consumer.Receiver.Address(), consumer.index, msg.GetData())
	}
	return nil
}

func (consumer *Consumer) prepareConsumer() error {
	consumer.close(2)
	consumer.Recievers = make([]receiver, 5)
	consumer.reconnect = make(chan struct{}, 5)
	consumer.reconnectOnce = &sync.Once{}
	//TODO: set 0 time out future once the issue fixed.
	client, err := amqp.Dial(consumer.url, amqp.ConnIdleTimeout(0))
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
		consumer.done.Add(1)
		defer func() {
			consumer.done.Done()
		}()
		select {
		case <-consumer.reconnect:
			break
		case <-consumer.shutdown:
			log.Println("Received shutdown instruction, re-connect go routine exit.")
			return
		}
		consumer.reconnectOnce.Do(func() {
			consumer.close(3)
		reconnect_loop:
			for {
				log.Println("re-connecting after 5 sends ...")
				err := consumer.prepareConsumer()
				if err != nil {
					select {
					case <-time.After(5 * time.Second):
						continue reconnect_loop
					case <-consumer.shutdown:
						log.Println("Received shutdown instruction, re-connect go routine exit.")
						return
					}
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
	consumer.shutdown = make(chan struct{})
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
		if consumer.shutdown != nil {
			close(consumer.shutdown)
		}
		consumer.close(3)
		consumer.done.Wait()
	}
	log.Println("AMQP Listener gracefully stopped")
}
