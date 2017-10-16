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
-A server use Another server (default none)
-c do not restrict Client ID to charactes and letters (default on)
-e max session expiry interval (default 0)
-i subscription identifiers not available (default implicit true)
-I subscription identifiers available (default implicit true)
-k server keep alive (default none)
-M max receive (input) Maximum (default 1)
-O server server mOved (default none)
-p port listen to port (default 1883)
-P max maximum packet size (default 10.000)
-Q max QoS supported (default 0)
-r RETAIN is not vailable (default implicit true)
-R RETAIN is available (default implicit true)
-s shared subscriptions not available (default implicit true)
-S shared subscriptions available (default implicit true)
-T max topic alias Maximum (default 0)
-u user[:password] add user with password (default none)
-U name=value add user property
-v verbose (default off)
-w wildcard subscriptions not available (default implicit true)
-W wildcard subscriptions available (default implicit true)
-x provide response information (default false)
```

## Exclusions
- TLS
- Websockets
- other transports than TCP
- Persistence
- Bridging
- Optimizations for speed
- Specific Authentications

