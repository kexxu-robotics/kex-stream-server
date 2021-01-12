package eventstream

/*
CREATE TABLE public.events
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    event_id character varying(256) COLLATE pg_catalog."default" NOT NULL,
    creation_time_unix_sec bigint,
    origin_id character varying(256) COLLATE pg_catalog."default" NOT NULL,
    origin_iter bigint,
    origin_group_id character varying(256) COLLATE pg_catalog."default",
    origin_build_version character varying(256) COLLATE pg_catalog."default",
    destination_id character varying(256) COLLATE pg_catalog."default" NOT NULL,
    destination_iter bigint,
    event_time_unix_sec bigint,
    event_type character varying(256) COLLATE pg_catalog."default",
    event_subtype character varying(256) COLLATE pg_catalog."default",
    event_version character varying(256) COLLATE pg_catalog."default",
    payload_json json,
    CONSTRAINT events_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE public.events
    OWNER to postgres;
*/

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
	DestinationId      string

	// the time this event happened
	EventTimeUnixSec int64

	// message type and versioning
	EventType    string
	EventSubtype string
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
	if em.DestinationId == "" {
		em.DestinationId = em.OriginId
	}

	// generate EventStream values for in the database
	em.CreationTimeUnixSec = time.Now().Unix()

	// try to save into the database
	err := es.Conn.QueryRow(
		context.Background(),
		"INSERT INTO events (event_id, creation_time_unix_sec, origin_id, origin_iter, origin_group_id, origin_build_version, destination_id, event_time_unix_sec, event_type, event_subtype, event_version, payload_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING id;",

		em.EventId,
		em.CreationTimeUnixSec,
		em.OriginId,
		em.OriginIter,
		em.OriginGroupId,
		em.OriginBuildVersion,
		em.DestinationId,
		em.EventTimeUnixSec,
		em.EventType,
		em.EventSubtype,
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
		mqttLocalPath := "eventstream/" + em.DestinationId + "/lastEvent"
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
			&m.DestinationId,
			&m.EventTimeUnixSec,
			&m.EventType,
			&m.EventSubtype,
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

// GetByDestinationId
// use -1 for newestId if you start from zero
func (es *EventStream) GetByDestinationId(destId string, newestId, limit int) ([]EventMessage, error) {

	ms := []EventMessage{}

	rows, err := es.Conn.Query(context.Background(),
		"SELECT id, event_id, COALESCE(creation_time_unix_sec, 0), COALESCE(origin_id, ''), COALESCE(origin_iter, 0), COALESCE(origin_group_id, ''), COALESCE(origin_build_version, ''), COALESCE(destination_id, ''), COALESCE(event_time_unix_sec, 0), COALESCE(event_type, ''), COALESCE(event_subtype, ''), COALESCE(event_version, ''), COALESCE(payload_json, '{}') FROM events WHERE destination_id=$1 AND id > $2 ORDER BY id DESC LIMIT $3",
		destId,
		newestId,
		limit,
	)
	if err != nil {
		return ms, err
	}

	return ParseRows(rows)
}

// GetByDestinationIdPage
// use -1 for newestId if you start from zero
func (es *EventStream) GetByDestinationIdPage(destId string, newestId, lastId, limit int) ([]EventMessage, error) {

	ms := []EventMessage{}

	rows, err := es.Conn.Query(context.Background(),
		"SELECT id, event_id, COALESCE(creation_time_unix_sec, 0), COALESCE(origin_id, ''), COALESCE(origin_iter, 0), COALESCE(origin_group_id, ''), COALESCE(origin_build_version, ''), COALESCE(destination_id, ''), COALESCE(event_time_unix_sec, 0), COALESCE(event_type, ''), COALESCE(event_subtype), COALESCE(event_version, ''), COALESCE(payload_json, '{}') FROM events WHERE destination_id=$1 AND id > $2  AND id < $3 ORDER BY id DESC LIMIT $4",
		destId,
		newestId,
		lastId,
		limit,
	)
	if err != nil {
		return ms, err
	}

	return ParseRows(rows)
}

// GetByDestinationIdAndEventType
// use -1 for newestId if you start from zero
func (es *EventStream) GetByDestinationIdAndEventType(destId, eventType string, newestId, limit int) ([]EventMessage, error) {

	ms := []EventMessage{}

	rows, err := es.Conn.Query(context.Background(),
		"SELECT id, event_id, COALESCE(creation_time_unix_sec, 0), COALESCE(origin_id, ''), COALESCE(origin_iter, 0), COALESCE(origin_group_id, ''), COALESCE(origin_build_version, ''), COALESCE(destination_id, ''), COALESCE(event_time_unix_sec, 0), COALESCE(event_type, ''), COALESCE(event_subtype, ''), COALESCE(event_version, ''), COALESCE(payload_json, '{}') FROM events WHERE destination_id=$1 AND event_type=$2 AND id > $3 ORDER BY id DESC LIMIT $4",
		destId,
		eventType,
		newestId,
		limit,
	)
	if err != nil {
		return ms, err
	}

	return ParseRows(rows)
}

// GetByDestinationIdAndEventTypePage
// use -1 for newestId if you start from zero
func (es *EventStream) GetByDestinationIdAndEventTypePage(destId, eventType string, newestId, lastId, limit int) ([]EventMessage, error) {

	ms := []EventMessage{}

	rows, err := es.Conn.Query(context.Background(),
		"SELECT id, event_id, COALESCE(creation_time_unix_sec, 0), COALESCE(origin_id, ''), COALESCE(origin_iter, 0), COALESCE(origin_group_id, ''), COALESCE(origin_build_version, ''), COALESCE(destination_id, ''), COALESCE(event_time_unix_sec, 0), COALESCE(event_type, ''), COALESCE(event_subtype, ''), COALESCE(event_version, ''), COALESCE(payload_json, '{}') FROM events WHERE destination_id=$1 AND event_type=$2 AND id > $3 AND id < $4 ORDER BY id DESC LIMIT $5",
		destId,
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
