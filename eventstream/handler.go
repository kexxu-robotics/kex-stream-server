package eventstream

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v4/pgxpool"
)

type Handler struct {
	Debug       bool
	BaseUrl     string
	StaticPath  string
	Conn        *pgxpool.Pool
	Secure      *Secure
	EventStream *EventStream
}

func setHeaders(w *http.ResponseWriter) {
	(*w).Header().Set("Content-Type", "application/json")
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
	(*w).Header().Set("Access-Control-Request-Method", "GET, POST, OPTIONS")
	(*w).Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
}

func (h *Handler) debugMsg(msg ...interface{}) {
	if h.Debug {
		fmt.Println(msg)
	}
}

func (h *Handler) AddEvent(w http.ResponseWriter, r *http.Request) {
	setHeaders(&w)
	if r.Method == "OPTIONS" {
		w.WriteHeader(200)
		return
	}

	originId := r.FormValue("id")
	destId := r.FormValue("destId")
	// if no destination is set, post to yourself
	if destId == "" {
		destId = originId
	}
	//apiVersion := r.FormValue("v")
	buildVersion := r.FormValue("build")
	if buildVersion == "" {
		buildVersion = "not set"
	}
	fmt.Println("AddEvent originId:", originId, "destId:", destId)
	secure, err, msg := h.Secure.Check(destId, r.FormValue("p"))
	if !secure {
		h.debugMsg(msg)
		http.Error(w, "not authorized", 401)
		return
	}
	if err != nil {
		h.debugMsg(err)
		http.Error(w, "authentication error", 500)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1*1024*1024) // max 1mb
	defer r.Body.Close()
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.debugMsg(err)
		http.Error(w, "error", http.StatusInternalServerError)
		return
	}

	fmt.Printf("payload: %s\n", data)
	event := EventMessage{}
	err = json.Unmarshal(data, &event)
	if err != nil {
		h.debugMsg("json error:", err)
		http.Error(w, "json error", http.StatusInternalServerError)
		return
	}

	event.OriginId = originId    // just making sure you post to the same origin as provided in the request
	event.DestinationId = destId // just making sure you post to the same destination as provided in the request
	eventSaved, err := h.EventStream.SaveMessage(event)
	if err != nil {
		h.debugMsg("error saving EventMessage:", err)
		http.Error(w, "error saving event", http.StatusInternalServerError)
		return
	}

	idObj := struct{ Id int64 }{Id: eventSaved.Id}
	js, _ := json.Marshal(idObj)
	w.Write(js)

}

// GetOriginEvents gets the events from the provided originId <id>
// Events are returned paginated, from new to old
// To get the next page, provide the last (lowest) <lastId> from the previous page
// All events are returned until <newestId> is reached.
//
//
// So in a typical situation:
//
// The client would already have events with ids 0, 1, 2, 3, 4, 5, 6
// where 6 here is the newestId.
// It would then try to get the the events it has not got yet,
// for instance 15, 14, 13, 12, 11, 10 (limit=6, newestId=6)
// Because the client has not reached its current newestId (6) it will get
// more (paginated) results:
// 9, 8, 7 (limit=6, newestId=6, lastId=10)
//
// if no <newestId> is provided, paginated results until the very first
// events are returned
func (h *Handler) GetOriginEvents(w http.ResponseWriter, r *http.Request) {
	setHeaders(&w)
	if r.Method == "OPTIONS" {
		w.WriteHeader(200)
		return
	}

	destId := r.FormValue("id")
	//apiVersion := r.FormValue("v")
	buildVersion := r.FormValue("build")
	if buildVersion == "" {
		buildVersion = "not set"
	}
	fmt.Println("GetOriginEvents destId: ", destId)
	secure, err, msg := h.Secure.Check(destId, r.FormValue("p"))
	if !secure {
		h.debugMsg(msg)
		http.Error(w, "not authorized", 401)
		return
	}
	if err != nil {
		h.debugMsg(err)
		http.Error(w, "authentication error", 500)
		return
	}

	eventType := r.FormValue("eventType")
	limit, _ := strconv.Atoi(r.FormValue("limit"))
	// if no limit is provided get the first 100 messages, to avoid spam
	if limit == 0 {
		limit = 100
	}
	// if limit > 1000, throw an error, in this case you should use pagination
	if limit > 1000 {
		h.debugMsg("limit was set above the maxium of 1000 event messages")
		http.Error(w, "limit cannot be more than 1000", 400)
		return
	}
	lastId, _ := strconv.Atoi(r.FormValue("lastId"))
	newestId, err := strconv.Atoi(r.FormValue("newestId"))
	if err != nil {
		newestId = -1 // if newestId is not set, get all messages from the start
	}

	ms := []EventMessage{}

	// if no eventType is provided, get all eventMessages from this device
	if eventType == "" {
		if lastId != 0 {
			ms, err = h.EventStream.GetByDestinationIdPage(destId, newestId, lastId, limit)
		} else {
			ms, err = h.EventStream.GetByDestinationId(destId, newestId, limit)
		}
	} else {
		if lastId != 0 {
			ms, err = h.EventStream.GetByDestinationIdAndEventTypePage(destId, eventType, newestId, lastId, limit)
		} else {
			ms, err = h.EventStream.GetByDestinationIdAndEventType(destId, eventType, newestId, limit)
		}
	}
	if err != nil {
		fmt.Println("error getting event messages:", err)
		http.Error(w, "error getting event messages", http.StatusInternalServerError)
		return
	}
	js, _ := json.Marshal(&ms)

	w.Write(js)
}
