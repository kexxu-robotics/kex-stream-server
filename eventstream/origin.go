package eventstream

/*
CREATE TABLE public.origins
(
    id character varying(256) COLLATE pg_catalog."default" NOT NULL,
    pass_hash character varying(256) COLLATE pg_catalog."default",
    CONSTRAINT origins_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE public.origins
    OWNER to postgres;

*/

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

type Origin struct {
	Id       string
	PassHash string
}

type Secure struct {
	sync.Mutex

	Conn              *pgxpool.Pool
	LastRefreshed     int64
	MaxRequestsPerMin int64

	Origins map[string]*SecureOrigin
}

type SecureOrigin struct {
	Id          string
	PassHash    string
	ReqsLastMin int64
}

func (s *Secure) ReloadOriginsChron() {
	for {
		start := time.Now()

		origins := make(map[string]*SecureOrigin)

		// load all origins from database into a SecureOrigin map
		rows, err := s.Conn.Query(context.Background(),
			"SELECT id, COALESCE(pass_hash, '') as pass_hash FROM origins")
		if err != nil {
			panic("could not load origins for security")
		}
		for rows.Next() {
			origin := SecureOrigin{}
			err := rows.Scan(
				&origin.Id,
				&origin.PassHash,
			)
			if err != nil {
				panic("could not load SecureOrigin")
			}
			origins[origin.Id] = &origin
		}

		// replace origins in with the refreshed list
		s.Lock()
		s.Origins = origins
		s.Unlock()
		s.LastRefreshed = time.Now().Unix()
		fmt.Println("refreshed origins for security, got", len(s.Origins), "origins in", time.Since(start))

		time.Sleep(60 * time.Second)
	}
	panic("origins reload chron stopped")
}

func (s *Secure) Check(id, pass string) (bool, error, string) {
	// check if this originId is valid
	d, ok := s.Origins[id]
	if !ok {
		return false, nil, "BLOCKED: unknown origin id"
	}
	// check if the origin has not reached its requests limit
	if d.ReqsLastMin > s.MaxRequestsPerMin {
		return false, errors.New("maximum requests reached"), "BLOCKED: maximum number of requests reached"
	}
	// check if the provided password is correct
	if d.PassHash != "" {
		h := sha256.New()
		h.Write([]byte(pass))
		if d.PassHash != fmt.Sprintf("%x", h.Sum(nil)) {
			return false, errors.New("invalid password"), "BLOCKED: invalid password"
		}
	}
	// add one request to the counter
	atomic.AddInt64(&d.ReqsLastMin, 1)
	// you are good to go
	return true, nil, ""
}
