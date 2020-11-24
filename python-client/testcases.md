Test cases
==========

Use the python client to test if the stream server is working correctly

### Get origin info

### Add an event e1

Check if we got an mqtt msg on
eventstream/<originId>/latestEvent

### Get latest event
  Check: should be e1
  Check: all fields are there correctly

### Add an event e2
  Check: e2.originIter should be e1.originIter+1


### Get events since e1.originIter
  Check: should be e2

### Add event with nonexisting originId
  Check: error

### Add more than 60 events in one minute:
  Check: should be blocked

Todo:
Malicious devices should have a crossed-ratelimiter with an ip address

Todo:
Block device
Block ip address

Maybe add ip address to event

### Add events: type test1
1, 2, 3, 4, 5
### Add events: type test2
a, b, c, d, e

### Get latest eventType test1
  Check: should be 5

### Check different query types if the sequence is ok

### Add an event with a payload that is too big
  Check: error

### Add an event on a password protected origin
No pass
Wrong pass
Right pass
