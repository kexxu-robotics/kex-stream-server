package eventstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// EventMessage is the format used to send Events around
// to save Events in the database
type EventMessage struct {

	// message id
	Id                  int64
	EventId             string
	CreationTimeUnixSec int64

	// info about the origin
	OriginId           string
	OriginIter         int64
	OriginGroupId      string
	OriginBuildVersion string

	// the time this event happened
	EventTimeUnixSec int64

	// message type and versioning
	EventType    string
	EventVersion string

	// content of the message
	PayloadJson string
}

type EventStream struct {
	Conn       *pgxpool.Pool
	MqttClient *mqtt.Client // (optional) MQTT client to notify when a new event is added

	EventStreamId string
	eventIdIter   uint64
}

type Status struct {
	Id         int64
	OriginIter int64
	UnixSec    int64
}

// GenStreamEventId
// format:
// <unixtime nana>_<origin iterator uint64>_<origin id>
// this should usually happen already on the origin, to prevent duplicate inserts
func (es *EventStream) GenStreamEventId() string {
	return fmt.Sprint(time.Now().UnixNano(), "_", atomic.AddUint64(&es.eventIdIter, 1), "_", es.EventStreamId)
}

// SaveMessage
func (es *EventStream) SaveMessage(em EventMessage) (EventMessage, error) {
	// check if eventId is set
	if em.EventId == "" {
		return em, errors.New("EventId not set")
	}

	// check if origin information is set
	if em.OriginId == "" || em.OriginBuildVersion == "" {
		return em, errors.New("OriginId or OriginBuildVersion not set")
	}
	if em.EventType == "" || em.EventVersion == "" {
		return em, errors.New("EventType or EventVersion not set")
	}

	// generate EventStream values for in the database
	em.CreationTimeUnixSec = time.Now().Unix()

	// try to save into the database
	err := es.Conn.QueryRow(
		context.Background(),
		"INSERT INTO events (event_id, creation_time_unix_sec, origin_id, origin_iter, origin_group_id, origin_build_version, event_time_unix_sec, event_type, event_version, payload_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id;",

		em.EventId,
		em.CreationTimeUnixSec,
		em.OriginId,
		em.OriginIter,
		em.OriginGroupId,
		em.OriginBuildVersion,
		em.EventTimeUnixSec,
		em.EventType,
		em.EventVersion,
		em.PayloadJson,
	).Scan(
		&em.Id,
	)
	if err != nil {
		return em, err
	}
	if em.Id == 0 {
		return em, errors.New("error inserting event in database, no Id returned")
	}
	fmt.Println("inserted event with id:", em.Id)

	// If MQTT is used for live updates, publish the latest eventMessage
	if es.MqttClient != nil {
		data, _ := json.Marshal(&em)

		// publish under specific device
		mqttLocalPath := "eventstream/" + em.OriginId + "/lastEvent"
		(*es.MqttClient).Publish(mqttLocalPath, 1, true, string(data))

		// publish under this server
		mqttServerPath := "eventstream/" + es.EventStreamId + "/lastEvent"
		(*es.MqttClient).Publish(mqttServerPath, 1, true, string(data))
	}

	return em, err
}

func ParseRows(rows pgx.Rows) ([]EventMessage, error) {
	ms := []EventMessage{}

	defer rows.Close()

	for rows.Next() {
		m := EventMessage{}
		err := rows.Scan(
			&m.Id,
			&m.EventId,
			&m.CreationTimeUnixSec,
			&m.OriginId,
			&m.OriginIter,
			&m.OriginGroupId,
			&m.OriginBuildVersion,
			&m.EventTimeUnixSec,
			&m.EventType,
			&m.EventVersion,
			&m.PayloadJson,
		)
		if err != nil {
			return []EventMessage{}, err
		}
		ms = append(ms, m)
	}

	return ms, rows.Err()
}

// getByEventId

// getByEventType

// GetByOriginId
// use -1 for newestId if you start from zero
func (es *EventStream) GetByOriginId(originId string, newestId, limit int) ([]EventMessage, error) {

	ms := []EventMessage{}

	rows, err := es.Conn.Query(context.Background(),
		"SELECT id, event_id, COALESCE(creation_time_unix_sec, 0), COALESCE(origin_id, ''), COALESCE(origin_iter, 0), COALESCE(origin_group_id, ''), COALESCE(origin_build_version, ''), COALESCE(event_time_unix_sec, 0), COALESCE(event_type, ''), COALESCE(event_version, ''), COALESCE(payload_json, '{}') FROM events WHERE origin_id=$1 AND id > $2 ORDER BY id DESC LIMIT $3",
		originId,
		newestId,
		limit,
	)
	if err != nil {
		return ms, err
	}

	return ParseRows(rows)
}

// GetByOriginIdPage
// use -1 for newestId if you start from zero
func (es *EventStream) GetByOriginIdPage(originId string, newestId, lastId, limit int) ([]EventMessage, error) {

	ms := []EventMessage{}

	rows, err := es.Conn.Query(context.Background(),
		"SELECT id, event_id, COALESCE(creation_time_unix_sec, 0), COALESCE(origin_id, ''), COALESCE(origin_iter, 0), COALESCE(origin_group_id, ''), COALESCE(origin_build_version, ''), COALESCE(event_time_unix_sec, 0), COALESCE(event_type, ''), COALESCE(event_version, ''), COALESCE(payload_json, '{}') FROM events WHERE origin_id=$1 AND id > $2  AND id < $3 ORDER BY id DESC LIMIT $4",
		originId,
		newestId,
		lastId,
		limit,
	)
	if err != nil {
		return ms, err
	}

	return ParseRows(rows)
}

// GetByOriginIdAndEventType
// use -1 for newestId if you start from zero
func (es *EventStream) GetByOriginIdAndEventType(originId, eventType string, newestId, limit int) ([]EventMessage, error) {

	ms := []EventMessage{}

	rows, err := es.Conn.Query(context.Background(),
		"SELECT id, event_id, COALESCE(creation_time_unix_sec, 0), COALESCE(origin_id, ''), COALESCE(origin_iter, 0), COALESCE(origin_group_id, ''), COALESCE(origin_build_version, ''), COALESCE(event_time_unix_sec, 0), COALESCE(event_type, ''), COALESCE(event_version, ''), COALESCE(payload_json, '{}') FROM events WHERE origin_id=$1 AND event_type=$2 AND id > $3 ORDER BY id DESC LIMIT $4",
		originId,
		eventType,
		newestId,
		limit,
	)
	if err != nil {
		return ms, err
	}

	return ParseRows(rows)
}

// GetByOriginIdAndEventTypePage
// use -1 for newestId if you start from zero
func (es *EventStream) GetByOriginIdAndEventTypePage(originId, eventType string, newestId, lastId, limit int) ([]EventMessage, error) {

	ms := []EventMessage{}

	rows, err := es.Conn.Query(context.Background(),
		"SELECT id, event_id, COALESCE(creation_time_unix_sec, 0), COALESCE(origin_id, ''), COALESCE(origin_iter, 0), COALESCE(origin_group_id, ''), COALESCE(origin_build_version, ''), COALESCE(event_time_unix_sec, 0), COALESCE(event_type, ''), COALESCE(event_version, ''), COALESCE(payload_json, '{}') FROM events WHERE origin_id=$1 AND event_type=$2 AND id > $3 AND id < $4 ORDER BY id DESC LIMIT $5",
		originId,
		eventType,
		newestId,
		lastId,
		limit,
	)
	if err != nil {
		return ms, err
	}

	return ParseRows(rows)
}

// getByGroupId
