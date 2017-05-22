enum MqttControlPacketType: UInt8 {
	case Reserved = 0
	case CONNECT = 1
	case CONNACK = 2
	case PUBLISH = 3
	case PUBACK = 4
	case PUBREC = 5
	case PUBREL = 6
	case PUBCOMP = 7
	case SUBSCRIBE = 8
	case SUBACK = 9
	case UNSUBSCRIBE = 10
	case UNSUBACK = 11
	case PINGREQ = 12
	case PINGRESP = 13
	case DISCONNECT = 14
	case AUTH = 15
}
