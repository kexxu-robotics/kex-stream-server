# kex-stream-server
Kexxu Event Streaming Server


# Installing on Ubuntu 20.04

## install MQTT

MQTT is used as a socket protocol for real time communication. Here we will demonstrate running MQTT on the same server as the rest of the event stream, but you can also install MQTT on a seperate server or even multiple servers, forming a synchronized cluster.

### MQTT Broker

`sudo apt-get update`
`sudo apt-get install mosquitto`


### for testing

`sudo apt-get install mosquitto-clients`


### secure with password

`sudo mosquitto_passwd -c /etc/mosquitto/passwd nelly`
`Password: password`

save this username and password in conf.yaml (see conf.yaml.example on how to create your conf file)


### create the configuration file

`sudo nano /etc/mosquitto/conf.d/default.conf`


### paste this into the conf for basic setup

```
allow_anonymous false
password_file /etc/mosquitto/passwd

listener 1883

listener 3000
protocol websockets
```

save these hostnames with ports in conf.yaml (see conf.yaml.example on how to create your conf file) as **yourhostname:1883** for normal mqtt and **yourhostname:3000** for websockets mqtt


### restart mosquitto

`sudo systemctl restart mosquitto`


### test subscribe mosquitto

`mosquitto_sub -t "test" -u "nelly" -P "password"`


### test publish mosquitto

`mosquitto_pub -t "test" -u "nelly" -P "password" -m "this is a test message"`


### to listen in on the eventstream later you can use the command

`mosquitto_sub -t "eventstream/#" -u "nelly" -P "password"`



## install Postgresql

Postgresql is used as the database to power the eventstream. Here we set up postgresql to run locally, but you can also use a managed postgresql database at for instance Amazon AWS.


### install

`sudo apt update`
`sudo apt install postgresql postgresql-contrib`


### confirm postgresql is running

`sudo systemctl status postgresql`

`sudo pg_isready`


### to goto postgres command line

`sudo su - postgres`
`psql`


### create main user

```
CREATE USER postgres WITH PASSWORD '<<password here>>';
CREATE DATABASE postgres;
GRANT ALL PRIVILEGES ON DATABASE postgres TO postgres;
```

### (optionally) to quit postgres command line

`\q`


### (optionally) install pgAdmin4, a graphical user interface to interact with postgresql

`curl https://www.pgadmin.org/static/packages_pgadmin_org.pub | sudo apt-key add`

`sudo sh -c 'echo "deb https://ftp.postgresql.org/pub/pgadmin/pgadmin4/apt/$(lsb_release -cs) pgadmin4 main" > /etc/apt/sources.list.d/pgadmin4.list && apt update'`

`sudo apt install pgadmin4`



### create a database 'eventstream'

```
CREATE DATABASE eventstream
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
```

### create a user 'eventstream'

```
CREATE USER "eventstream" ENCRYPTED PASSWORD '<<password here>>';
```


### create events table

```
CREATE TABLE public.events
(
    id bigserial NOT NULL,
    event_id character varying(128) NOT NULL,
    creation_time_unix_sec bigint NOT NULL,
    payload_json jsonb,

    event_type character varying(128) NOT NULL,
    event_version character varying(128) NOT NULL,
    event_time_unix_sec bigint NOT NULL,

    origin_id character varying(128) NOT NULL,
    origin_build_version character varying(128) NOT NULL,
    origin_iter bigint NOT NULL,

    recipient_id character varying(128),
	recipient_iter bigint NOT NULL,
    recipient_ids_history character varying(128)[],

    PRIMARY KEY (id)
)

TABLESPACE pg_default;

GRANT ALL ON TABLE public.events TO "postgres";

GRANT INSERT, REFERENCES, TRIGGER, SELECT ON TABLE public.events TO "eventstream";
```



### create the origins table

```
CREATE TABLE public.origins
(
	id bigserial NOT NULL,
    origin_id character varying(128) NOT NULL,
    added_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    type character varying(128) NOT NULL,
    sub_type character varying(128) NOT NULL,
    version character varying(128) NOT NULL,
	origin_iter BIGINT DEFAULT 0 NOT NULL,
	recipient_iter BIGINT DEFAULT 0 NOT NULL,
    PRIMARY KEY (id)
);

TABLESPACE pg_default;

GRANT ALL ON TABLE public.origins TO "postgres";

GRANT INSERT, REFERENCES, TRIGGER, SELECT, UPDATE ON TABLE public.origins TO "eventstream";
```



## build go-server



## test





