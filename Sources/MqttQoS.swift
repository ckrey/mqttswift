enum MqttQoS: UInt8 {
	case AtMostOnce = 0
	case AtLeastOnce = 1
	case ExactlyOnce = 2
    case Reserved = 3
}
