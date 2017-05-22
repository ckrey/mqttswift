# A Minimal MQTT v5.0 Conforming Broker in Swift

## Purpose
- v5.0 evaluation and reference implementation

## Features
- Full v5.0 spec
- Feature Trace
- Conformance Trace
- optional features
- Basic Authentication
- Basic ACL

### CLI

```
Usage mqttswift [Options]
-a allow anonymous (default off)
-c do not restrict Client ID to charactes and letters (default on)
-i subscription identifiers available (default off)
-k server keep alive (default none)
-M server server moved (default none)
-p port listen to port (default 1883)
-P max maximum packet size (default 10.000)
-Q max QoS supported (default 0)
-r RETAIN is available (default not)
-R max receive (input) Maximum (default 1)
-s shared subscriptions available (default off)
-T max topic alias Maximum (default 0)
-u user[:password] add user with password (default none)
-U server use another server (default none)
-v verbose (default off)
-w wildcard subscriptions available (default off)
``

## Done
- V5 Life cyle including Session Expiry Interval, Server Keep Alive, Will Delay Interval, Clean Start, Disconnect Return Codes

## Todo
- SUBSCRIBE/UNSUBSCRIBE
- 



## Exclusions
- TLS
- Websockets
- other transports than TCP
- Persistence
- Bridging
- Optimizations for speed
- Specific Authentications

