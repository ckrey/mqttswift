import Foundation

class MqttSessions {
    private static var theInstance: MqttSessions?

	var sessions: [String: MqttSession]
    var start: Date

    public class func sharedInstance() -> MqttSessions {
        if theInstance == nil {
            theInstance = MqttSessions()
        }
        return theInstance!
    }

    private init() {
        start = Date()
		self.sessions = [String: MqttSession]()
	}

    func remove(session: MqttSession) {
        self.sessions.removeValue(forKey: session.clientId)
    }

	func store(session: MqttSession) {
		self.sessions[session.clientId] = session
	}

	func get(clientId: String) -> MqttSession? {
        let mqttSession = self.sessions[clientId]
        if mqttSession != nil {
            if mqttSession!.deleted {
                return nil
            }
        }
		return mqttSession;
	}

	func summary() {
		print ("MqttSession Summary")
		for (clientId, session) in self.sessions {
            print("\(clientId): \(session.socket != nil ? "on" : "off")")
		}
	}

    class func processReceivedMessage(topic: String, payload: Data, retain: Bool, qos: MqttQoS) {

        if retain {
            MqttRetained.sharedInstance().store(topic: topic, data: payload)
        }

        for (clientId, mqttSession) in MqttSessions.sharedInstance().sessions {
            /* Todo check if allowed to read from here */
            /* Todo check if has subscriptions for here */
            if mqttSession.socket != nil {
                print ("Sending to \(clientId) \(topic) \(payload)")
                var publish = Data()
                var u : UInt8
                u = MqttControlPacketType.PUBLISH.rawValue << 4
                publish.append(u)
                let td = topic.data(using: String.Encoding.utf8)
                publish.append(MqttControlPacket.mqttVariableByte(variable: 2 + td!.count + 1 + payload.count))
                u = UInt8(td!.count / 256)
                publish.append(u)
                u = UInt8(td!.count % 256)
                publish.append(u)
                publish.append(td!)
                u = 0
                publish.append(u)
                publish.append(payload)
                do {
                    try mqttSession.socket!.write(from: publish)
                } catch {
                    //
                }
            } else {
                print ("Queueing to \(clientId) \(topic) \(payload)")
            }
        }
    }

    class func stats() {
        print("Stats")

        let now = Date()
        let seconds = Int(now.timeIntervalSince1970 - MqttSessions.sharedInstance().start.timeIntervalSince1970)

        MqttSessions.processReceivedMessage(topic: "$SYS/broker/uptime",
                                                                     payload: "\(seconds) seconds".data(using:String.Encoding.utf8)!,
                                                                     retain: true,
                                                                     qos: .AtMostOnce)
        MqttSessions.processReceivedMessage(topic: "$SYS/broker/clients/total",
                                                                     payload: "\(MqttSessions.sharedInstance().sessions.count)".data(using:String.Encoding.utf8)!,
                                                                     retain: true,
                                                                     qos: .AtMostOnce)
    }

}

