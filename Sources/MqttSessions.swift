import Foundation

class MqttSessions {
    private static var theInstance: MqttSessions?

	var sessions: [String: MqttSession]
    var start: Date

    var droppedQoS0Messages: Int
    var sentMessages: Int

    public class func sharedInstance() -> MqttSessions {
        if theInstance == nil {
            theInstance = MqttSessions()
        }
        return theInstance!
    }

    private init() {
        start = Date()
		self.sessions = [String: MqttSession]()
        self.droppedQoS0Messages = 0
        self.sentMessages = 0
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

    class func processReceivedMessage(message: MqttMessage, from: String) {

        if message.retain {
            MqttRetained.sharedInstance().store(message:message)
        }

        for (_, mqttSession) in MqttSessions.sharedInstance().sessions {
            let aMessage = MqttMessage()
            aMessage.topic = message.topic
            aMessage.data = message.data
            aMessage.qos = message.qos
            aMessage.retain = message.retain
            aMessage.payloadFormatIndicator = message.payloadFormatIndicator
            aMessage.publicationExpiryInterval = message.publicationExpiryInterval
            aMessage.responseTopic = message.responseTopic
            aMessage.correlationData = message.correlationData
            aMessage.userProperties = message.userProperties
            aMessage.contentType = message.contentType
            aMessage.topicAlias = message.topicAlias
            aMessage.contentType = message.contentType
            aMessage.subscriptionIdentifiers = message.subscriptionIdentifiers

            var matches = false;
            for (_, subscription) in mqttSession.subscriptions {
                if self.matches(topic:message.topic, topicFilter:subscription.topicFilter) {
                    if subscription.noLocal && from == mqttSession.clientId {
                        MqttCompliance.sharedInstance().log(target: "MQTT-3.8.3-3")
                        continue;
                    }
                    matches = true;
                    if aMessage.qos.rawValue > subscription.qos.rawValue {
                        MqttCompliance.sharedInstance().log(target: "MQTT-3.8.4-8")
                        aMessage.qos = subscription.qos
                    }
                    if subscription.subscriptionIdentifier != nil {
                        if aMessage.subscriptionIdentifiers == nil {
                            aMessage.subscriptionIdentifiers = [Int]()
                        }
                        aMessage.subscriptionIdentifiers!.append(subscription.subscriptionIdentifier!)
                    }
                }
            }

            if matches {
                DispatchQueue.main.async {
                    mqttSession.publish(message:aMessage)
                }
            }
        }
    }
    
    class func matches(topic: String, topicFilter: String) -> Bool {
        let topicComponents = topic.components(separatedBy: "/");
        let topicFilterComponents = topicFilter.components(separatedBy: "/");

        var topicComponent = 0;
        var topicFilterComponent = 0;

        while topicComponent < topicComponents.count &&
            topicFilterComponent < topicFilterComponents.count {

                if topicFilterComponents[topicFilterComponent] == "#" &&
                    !topicComponents[topicComponent].hasPrefix("$") {
                    topicComponent = topicComponent + 1

                } else if topicFilterComponents[topicFilterComponent] == "+" &&
                    !topicComponents[topicComponent].hasPrefix("$") {

                    topicComponent = topicComponent + 1
                    topicFilterComponent = topicFilterComponent + 1

                } else if topicComponents[topicComponent] == topicFilterComponents[topicFilterComponent] {
                    MqttCompliance.sharedInstance().log(target: "MQTT-4.7.3-4")

                    topicComponent = topicComponent + 1
                    topicFilterComponent = topicFilterComponent + 1

                } else {
                    return false
                }
        }

        return true
    }

/*        _             _
 *  ___  | |_    __ _  | |_   ___
 * / __| | __|  / _` | | __| / __|
 * \__ \ | |_  | (_| | | |_  \__ \
 * |___/  \__|  \__,_|  \__| |___/
 */
    class func stats() {
        let now = Date()
        let seconds = Int(now.timeIntervalSince1970 - MqttSessions.sharedInstance().start.timeIntervalSince1970)
        self.statMessage(topic: "$SYS/broker/uptime", payload: "\(seconds)")

        self.statMessage(topic: "$SYS/broker/clients/total", payload: "\(MqttSessions.sharedInstance().sessions.count)")
        self.statMessage(topic: "$SYS/broker/clients/total", payload: "\(MqttSessions.sharedInstance().sessions.count)")
        self.statMessage(topic: "$SYS/broker/clients/connected", payload: "\(MqttSessions.sharedInstance().sessions.count)")
        self.statMessage(topic: "$SYS/broker/clients/expired", payload: "\(MqttSessions.sharedInstance().sessions.count)")
        self.statMessage(topic: "$SYS/broker/clients/disconnected", payload: "\(MqttSessions.sharedInstance().sessions.count)")

        self.statMessage(topic: "$SYS/broker/clients/maximum", payload: "\(MqttSessions.sharedInstance().sessions.count)")

        self.statMessage(topic: "$SYS/broker/bytes/received", payload: "-1")
        self.statMessage(topic: "$SYS/load/bytes/received/1", payload: "-1")
        self.statMessage(topic: "$SYS/load/bytes/received/5", payload: "-1")
        self.statMessage(topic: "$SYS/load/bytes/received/15", payload: "-1")

        self.statMessage(topic: "$SYS/broker/bytes/sent", payload: "-1")
        self.statMessage(topic: "$SYS/load/bytes/sent/1", payload: "-1")
        self.statMessage(topic: "$SYS/load/bytes/sent/5", payload: "-1")
        self.statMessage(topic: "$SYS/load/bytes/sent/15", payload: "-1")

        var subscriptions = 0
        for (_, mqttSession) in MqttSessions.sharedInstance().sessions {
            for (_, _) in mqttSession.subscriptions {
                subscriptions = subscriptions + 1
            }
        }
        self.statMessage(topic: "$SYS/subscriptions/count", payload: "\(subscriptions)")

        self.statMessage(topic: "$SYS/retained messages/count", payload: "\(MqttRetained.sharedInstance().retained.count)")
        self.statMessage(topic: "$SYS/broker/messages/inflight", payload: "-1")
        self.statMessage(topic: "$SYS/broker/messages/stored", payload: "-1")
        self.statMessage(topic: "$SYS/broker/messages/received", payload: "-1")
        self.statMessage(topic: "$SYS/publish/messages/sent", payload: "\(MqttSessions.sharedInstance().sentMessages)")
        self.statMessage(topic: "$SYS/publish/messages/dropped", payload: "-1")
        self.statMessage(topic: "$SYS/publish/messages/droppedQoS0", payload: "\(MqttSessions.sharedInstance().droppedQoS0Messages)")
        self.statMessage(topic: "$SYS/publish/messages/received", payload: "-1")
        MqttCompliance.sharedInstance().stats()
    }

    class func statMessage(topic: String, payload: String) {
        MqttSessions.processReceivedMessage(message: MqttMessage(topic: topic,
                                                                 data: payload.data(using:String.Encoding.utf8),
                                                                 qos: .AtMostOnce,
                                                                 retain: true),
                                            from:"")
    }
}

