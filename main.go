package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"
)

type PebbleMessage struct {
	Title   string `json:"title"`
	Message string `json:"message"`
}

type PebbleResponse struct {
	Response interface{} `json:"response"`
	Drops    int         `json:"dropped,omitempty"`
}

var (
	pebbleUsers     map[string]chan *PebbleMessage = map[string]chan *PebbleMessage{}
	pebbleAlternate map[string]chan *PebbleMessage = map[string]chan *PebbleMessage{}
	pebbleUsersLock chan int                       = make(chan int, 1)
	pebbleWriteLock chan int                       = make(chan int, 1)
)

func getUserChan(user string) (chan *PebbleMessage, chan *PebbleMessage) {
	// lock the pebbleUsers map
	pebbleUsersLock <- 1
	// and unlock it when we exit
	defer func() {
		<-pebbleUsersLock
	}()
	if c, ok := pebbleUsers[user]; ok {
		return c, pebbleAlternate[user]
	}
	pebbleUsers[user] = make(chan *PebbleMessage, 100) // buffer up to 100 messages
	pebbleUsers[user] <- &PebbleMessage{"message title", "Here's my message to you!"}
	pebbleAlternate[user] = make(chan *PebbleMessage) // these are alternates which are also listening at the same time.
	return pebbleUsers[user], pebbleAlternate[user]
}

func puller(w http.ResponseWriter, r *http.Request) {
	c, alt := getUserChan(r.FormValue("user"))
	select {
	case msg := <-c:
		// FIXME: this is a race condition if multiple threads are enabled.
		//        if there are two messages on this queue and two readers appear,
		//        this block will run in parallel causing one message to go to one
		//        and the other to go to the other.  This will be followed by this
		//        locked section.  Essentially, if the behavior which this block
		//        is meant to protect against is actually seen (a failed read followed
		//        by a good read which occur in a short time frame), then if the
		//        channel had two items on it, one item will be sunk into the bad read
		//        which will then block immediately while the good read finishes and
		//        then it will acquire the lock and proceed to do nothing with the
		//        message, discarding it into a dead ResponseWriter.
		//        Maybe FIXME is a bit strong of a term considering the fact that
		//        I don't plan to fix it unless I completely change how this works.
		//        It's *super* unlikely to happen and I'm fairly certain it's impossible
		//        unless golang becomes multithreaded by default.
		// FIXME: this shouldn't be a global lock since it's a per user requirement
		pebbleWriteLock <- 1
		defer func() {
			<-pebbleWriteLock
		}()
		b, _ := json.Marshal(&PebbleResponse{Response: msg})
		w.Write(b)
		// Forward the message on to the concurrent requests
	outer:
		for {
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
	case <-time.After(30 * time.Second):
		// timeout after 30 seconds
		b, _ := json.Marshal(&PebbleResponse{Response: "timeout"})
		w.WriteHeader(408)
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
	// Loop over the channel until there is room.  Count the number of drops so it can
	// be reported in case the caller cares (maybe wants to back off or report it to the user)
outer:
	for {
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

var addr = flag.String("addr", ":8088", "http service address")

func main() {
	flag.Parse()
	http.HandleFunc("/pull", puller)
	http.HandleFunc("/push", pusher)
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}
