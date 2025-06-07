package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker  = "localhost:9092"
	topic        = "financial_transactions"
	numMessages  = 0
	sendInterval = 100 * time.Millisecond
)

type FinancialTransaction struct {
	TransactionID string    `json:"transaction_id"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	Type          string    `json:"type"`
	Timestamp     time.Time `json:"timestamp"`
	Description   string    `json:"description,omitempty"`
	AccountID     string    `json:"account_id"`
}

func main() {
	log.Println("Starting Kafka Producer for financial_transactions...")

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-ctx.Done():
				log.Println("Producer received shutdown signal. Exiting message loop.")
				return
			default:
				if numMessages > 0 && i >= numMessages {
					log.Printf("Finished sending %d messages. Exiting.", numMessages)
					cancel()
					return
				}

				transaction := generateDummyTransaction(i)
				msgValue, err := json.Marshal(transaction)
				if err != nil {
					log.Printf("Error marshalling transaction: %v", err)
					continue
				}

				msg := kafka.Message{
					Key:   []byte(transaction.TransactionID),
					Value: msgValue,
					Time:  transaction.Timestamp,
				}

				err = writer.WriteMessages(ctx, msg)
				if err != nil {
					log.Printf("Error writing message to Kafka: %v", err)
					if ctx.Err() != nil {
						return
					}
					time.Sleep(sendInterval)
					continue
				}

				log.Printf("Sent message %d (ID: %s, Amount: %.2f) to topic %s", i+1, transaction.TransactionID, transaction.Amount, topic)
				i++
				time.Sleep(sendInterval)
			}
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	log.Println("Shutting down producer gracefully...")
	cancel()
	wg.Wait()

	log.Println("Producer stopped.")
}

func generateDummyTransaction(index int) FinancialTransaction {
	transactionTypes := []string{"purchase", "refund", "transfer", "withdrawal", "deposit"}
	currencies := []string{"USD", "EUR", "GBP", "TRY"}
	descriptions := []string{
		"Online shopping", "Utility bill payment", "Salary deposit",
		"ATM withdrawal", "Restaurant bill", "Subscription renewal",
		"Friend payment", "Loan repayment", "Investment", "Travel expense",
	}

	return FinancialTransaction{
		TransactionID: fmt.Sprintf("TXN-%d-%s", index, randSeq(8)),
		Amount:        float64(rand.Intn(100000)+1) / 100, // 0.01 to 1000.00
		Currency:      currencies[rand.Intn(len(currencies))],
		Type:          transactionTypes[rand.Intn(len(transactionTypes))],
		Timestamp:     time.Now().Add(-time.Duration(rand.Intn(3600)) * time.Second), // Last hour
		Description:   descriptions[rand.Intn(len(descriptions))],
		AccountID:     fmt.Sprintf("ACC-%04d", rand.Intn(1000)+1), // 1 to 1000 accounts
	}
}

func randSeq(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
