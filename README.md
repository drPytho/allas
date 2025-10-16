Allas - Server Side Event
=====

Introduction
------------

_allas_ is a connection pooler for [PostgreSQL](http://www.postgresql.org)
It muxes notify events out over http2 server side events

How to build
------------

Clone the repository, and run "go build" in the cloned directory.  This should
produce a binary called "allas".



Configuration
-------------
```
env BIND_ADDR=... # Where to listen for incoming http requests
env DATABASE_URL=... # How to connect to the DB
```


API
-------------
We are listening on `$BIND_ADDR/events?channels=ChannelA,ChannelB,ChannelC`

License: MIT
