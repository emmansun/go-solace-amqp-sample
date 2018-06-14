package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	cli "gopkg.in/urfave/cli.v1"
	"pack.ag/amqp"
)

type Producer struct {
	url         string
	destination string
	Client      *amqp.Client
	Session     *amqp.Session
	Sender      *amqp.Sender
	Context     context.Context
}

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

var producer *Producer

func (producer *Producer) close(seconds time.Duration) {
	if producer.Sender != nil {
		ctx, cancel := context.WithTimeout(producer.Context, time.Second*seconds)
		producer.Sender.Close(ctx)
		cancel()

	}
	if producer.Client != nil {
		producer.Client.Close()
	}
}

func (producer *Producer) sendMsg(message string) {
	if producer.Sender == nil {
		err := producer.prepareSender()
		if err != nil {
			log.Println("Failed to connect to server!")
			time.Sleep(1 * time.Second)
			return
		}
	}
	// Send message
	err := producer.Sender.Send(producer.Context, amqp.NewMessage([]byte(message)))
	if err != nil {
		log.Printf("Sending message: %v\n, Re connecting ...", err)
		producer.Sender = nil
	} else {
		log.Println("Send message to queue completed.")
	}
}

func readLine(reader io.Reader, f func(string)) {
	fmt.Print("Please input message >>>>>>")
	buf := bufio.NewReader(reader)
	line, err := buf.ReadBytes('\n')
	for err == nil {
		line = bytes.TrimRight(line, "\n")
		if len(line) > 0 {
			if line[len(line)-1] == 13 { //'\r'
				line = bytes.TrimRight(line, "\r")
			}
			f(string(line))
		}
		fmt.Print("\nPlease input message >>>>>>")
		line, err = buf.ReadBytes('\n')
	}

	if len(line) > 0 {
		f(string(line))
	}
}

func (producer *Producer) prepareSender() error {
	producer.Client = nil
	producer.Session = nil
	producer.Sender = nil
	producer.Context = context.Background()
	client, err := amqp.Dial(producer.url, amqp.ConnIdleTimeout(0))
	if err != nil {
		log.Printf("Dialing AMQP server: %v\n", err)
		return err
	}
	producer.Client = client
	// Open a session
	session, err := client.NewSession()
	if err != nil {
		log.Printf("Creating AMQP session: %v\n", err)
		return err
	}
	producer.Session = session
	sender, err := session.NewSender(
		amqp.LinkTargetAddress(producer.destination),
	)
	if err != nil {
		log.Printf("Creating sender link: %v\n", err)
		return err
	}
	producer.Sender = sender
	return nil
}

func serveSender(ctx *cli.Context) error {
	if !ctx.GlobalIsSet(AmqpUrlFlag.Name) || !ctx.GlobalIsSet(DestinationFlag.Name) {
		cli.ShowAppHelpAndExit(ctx, -1)
	}
	producer = new(Producer)
	producer.url = ctx.GlobalString(AmqpUrlFlag.Name)
	producer.destination = ctx.GlobalString(DestinationFlag.Name)
	err := producer.prepareSender()
	if err != nil {
		return err
	}
	go func() {
		readLine(os.Stdin, func(line string) {
			producer.sendMsg(line)
		})
	}()
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "sample amqp producer application."
	app.Flags = []cli.Flag{AmqpUrlFlag, DestinationFlag}
	app.Action = serveSender
	err := app.Run(os.Args)
	if err != nil {
		if producer != nil {
			producer.close(5)
		}
		log.Fatalln(err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	defer signal.Stop(sigchan)
	log.Printf("Got interrupt, shutting down...singal=%v\n", <-sigchan)
	if producer != nil {
		producer.close(5)
	}
	log.Println("AMQP Producer gracefully stopped")
}
