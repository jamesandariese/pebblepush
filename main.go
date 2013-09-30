package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"log"
	"time"
)

type PebbleMessage struct {
	Title string `json:"title"`
	Message string `json:"message"`
}

type PebbleResponse struct {
	Response interface{} `json:"response"`
	Drops int `json:"dropped",omitempty`
}

var (
	pebbleUsers map[string]chan *PebbleMessage = map[string]chan *PebbleMessage{}
	pebbleAlternate map[string]chan *PebbleMessage = map[string]chan *PebbleMessage{}
	pebbleUsersLock chan int = make(chan int, 1)
	pebbleWriteLock chan int = make(chan int, 1)
)

func getUserChan(user string) (chan *PebbleMessage, chan *PebbleMessage) {
	// lock the pebbleUsers map
	pebbleUsersLock <- 1
	// and unlock it when we exit
	defer func() {
		<- pebbleUsersLock
	}()
	if c, ok := pebbleUsers[user]; ok {
		return c, pebbleAlternate[user]
	}
	pebbleUsers[user] = make(chan *PebbleMessage, 1) // buffer up to 100 messages
	pebbleUsers[user] <- &PebbleMessage{"message title", "Here's my message to you!"}
	pebbleAlternate[user] = make(chan *PebbleMessage) // these are alternates which are also listening at the same time.
	return pebbleUsers[user], pebbleAlternate[user]
}

func puller(w http.ResponseWriter, r *http.Request) {
	c, alt := getUserChan(r.FormValue("user"))
	select {
	case msg := <-c:
		// FIXME: this is a race condition if multiple threads are enabled.
		// FIXME: this shouldn't be a global lock since it's a per user requirement
		pebbleWriteLock <- 1
		defer func() {
			<-pebbleWriteLock
		}()
		b, _ := json.Marshal(&PebbleResponse{Response: msg})
		w.Write(b)
		outer: for {
			select {
			case alt <- msg:
			default:
				break outer
			}
		}
	case msg := <-alt:
		// this is reached if another puller got the message first but we were
		// waiting at the same time.
		// this is NOT meant to allow multiple clients to read from the same queue
		// it is meant to protect somewhat against connections which die early and
		// are immediately restarted during long waits.
		// this is required because the err response from RequestWriter is unreliable.
		b, _ := json.Marshal(&PebbleResponse{Response: msg})
		w.Write(b)
	case <- time.After(30 * time.Second):
		b, _ := json.Marshal(&PebbleResponse{Response: "timeout"})
		w.Write(b)
	}
}

func pusher(w http.ResponseWriter, r *http.Request) {
	// FIXME: this shouldn't be a global lock since it's a per user requirement
	pebbleWriteLock <- 1
	defer func() {
		<-pebbleWriteLock
	}()
     	msg := &PebbleMessage{r.FormValue("title"), r.FormValue("message")}
	c, _ := getUserChan(r.FormValue("user"))
	drops := 0
	outer: for {
		select {
		case c <- msg:
			b, _ := json.Marshal(&PebbleResponse{Response: "success", Drops: drops})
			w.Write(b)
			break outer
		default:
			<-c
			drops += 1
		}
	}
}

func main() {
	http.HandleFunc("/pull", puller)
	http.HandleFunc("/push", pusher)
	err := http.ListenAndServe(":8085", nil)
	if err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}
