import Foundation
import Socket

class MqttSession {
    var clientId: String
    var socket: Socket?
    var user: String?
    var connectKeepAlive: Int
    var serverKeepAlive: Int
    var willDelayInterval: Int
    var sessionExpiryInterval: Int?
    var maximumSessionExpiryInterval: Int
    var maximumPacketSize: Int?
    var willFlag: Bool
    var willTopic: String?
    var willMessage: Data?
    var willQoS: MqttQoS?
    var willRetain: Bool?

    var shouldKeepRunning: Bool
    var returnCode: MqttReturnCode?
    var lastPacket: Date
    var subscriptions: [String: MqttSubscription]
    var topicAliasesIn : [Int: String]
    var topicAliasMaximumIn : Int
    var topicAliasesOut : [Int: String]
    var topicAliasMaximumOut : Int
    var queued: MqttQueued
    var inflightOut: MqttInflight
    var inflightIn: MqttInflight
    var present: Bool?
    var deleted: Bool

    private var stateTimer: DispatchSourceTimer
    private var pendingWillAt: Date?
    private var pendingDeleteAt: Date?

    private var lastPacketIdentifier: UInt16

    init(clientId: String) {
        self.clientId = clientId
        self.user = nil

        self.shouldKeepRunning = true
        self.subscriptions = [String: MqttSubscription]()
        self.topicAliasesIn = [Int: String]()
        self.topicAliasMaximumIn = 0
        self.topicAliasesOut = [Int: String]()
        self.topicAliasMaximumOut = 0

        self.queued = MqttQueued()
        self.inflightIn = MqttInflight()
        self.inflightOut = MqttInflight()
        self.connectKeepAlive = 60
        self.serverKeepAlive = 60
        self.willDelayInterval = 0
        self.sessionExpiryInterval = nil
        self.maximumSessionExpiryInterval = 0
        
        self.willFlag = false
        self.willTopic = nil
        self.willMessage = nil
        self.willQoS = nil
        self.willRetain = nil

        self.pendingWillAt = nil
        self.lastPacket = Date()
        self.deleted = false

        lastPacketIdentifier = 0

        let q = DispatchQueue.global()

        self.stateTimer = DispatchSource.makeTimerSource(queue: q)
        self.stateTimer.scheduleRepeating(deadline: .now(), interval: .seconds(1), leeway: .seconds(1))
        self.stateTimer.setEventHandler { [weak self] in
            self!.states()
        }
        self.stateTimer.resume()

    }

    private func nextPacketIdentifier() -> UInt16 {
        self.lastPacketIdentifier = self.lastPacketIdentifier + 1
        return self.lastPacketIdentifier
    }
    
    func states() {
        let now = Date()

        if !deleted {
            if socket != nil {
                let noPacketSince = Int(now.timeIntervalSince1970 - self.lastPacket.timeIntervalSince1970)

                if noPacketSince > self.serverKeepAlive + self.serverKeepAlive / 2 {
                    print ("Error____ \(self.clientId) KeepAliveTimeout")
                    self.shouldKeepRunning = false
                    self.returnCode = .KeepAliveTimeout
                    self.askForWill()
                }
            } else {
                if !deleted {
                    if self.sessionExpiryInterval != nil {
                        if self.pendingDeleteAt == nil {
                            self.pendingDeleteAt = Date(timeIntervalSinceNow: Double(self.sessionExpiryInterval!))
                            print ("Scheduled \(self.clientId) deletion in \(self.sessionExpiryInterval!)")
                        }
                    } else {
                        // will not be deleted
                    }
                }
            }

            if self.pendingDeleteAt != nil {
                if self.pendingDeleteAt! < Date() {
                    if self.pendingWillAt != nil {
                        self.sendWill()
                    }
                    print ("Deleting_ \(self.clientId)")
                    self.deleted = true
                    self.pendingDeleteAt = nil
                }
            }

        } else {
            print ("Removing_ \(self.clientId)")
            self.stateTimer.cancel()
            MqttSessions.sharedInstance().remove(session: self)
        }

        if self.pendingWillAt != nil {
            if self.pendingWillAt! < Date() {
                self.sendWill()
            }
        }
    }

    func askForWill() {
        if (self.willFlag) {
            self.pendingWillAt = Date(timeIntervalSinceNow: Double(self.willDelayInterval))
            print ("Scheduled \(self.clientId) will in \(self.willDelayInterval)")
        }
    }

    func sendWill() {
        print ("Sending__ \(self.clientId) Will \(self.willTopic!) \(self.willMessage!) \(self.willRetain!) \(self.willQoS!)")
        let message = MqttMessage(topic: self.willTopic!,
                                  data: self.willMessage!,
                                  qos: self.willQoS!,
                                  retain: self.willRetain!)
        MqttSessions.processReceivedMessage(message: message, from: self.clientId)
        self.pendingWillAt = nil
    }

    func store(subscription: MqttSubscription!) {

        let existingSubscription = self.get(topicFilter: subscription.topicFilter)
        if existingSubscription != nil {
            MqttCompliance.sharedInstance().log(target: "MQTT-3.8.4-3")
        }
        
        if subscription.retainHandling == MqttRetainHandling.SendRetained ||
            subscription.retainHandling == MqttRetainHandling.SendRetainedIfNotYetSubscribed && existingSubscription == nil {
            for (topic, message) in MqttRetained.sharedInstance().retained {
                if MqttSessions.matches(topic: topic, topicFilter: subscription.topicFilter) {
                    self.publish(message:message)
                }
            }
        }
        self.subscriptions[subscription.topicFilter] = subscription
        MqttSharedSubscriptions.sharedInstance().store(subscription:subscription)
    }

    func remove(topicFilter: String) {
        let existingSubscription = self.get(topicFilter:topicFilter)
        if existingSubscription != nil {
            MqttSharedSubscriptions.sharedInstance().remove(subscription:existingSubscription!)
            self.subscriptions[topicFilter] = nil
        }
    }

    func get(topicFilter: String) -> MqttSubscription? {
        return self.subscriptions[topicFilter];
    }

    func debug(s: String!, p: Int?) {
        let payloadString = "\(Int(Date.init().timeIntervalSince1970)): \(self.clientId) \(s!) \(p != nil ? "p\(p!)" : "")"
        print("DEBUG: \(payloadString)")
    }

    func publish(message: MqttMessage!) {
        self.debug(s: "Publishing to \(clientId) q\(message.qos) pid\(message.packetIdentifier) \(message.topic) \(message.data)", p:nil)

        /* try to find and existing topicAlias */
        for (topicAlias, topic) in self.topicAliasesOut {
            if topic == message.topic {
                MqttCompliance.sharedInstance().log(target: "Used already set topic alias out")
                message.topic = ""
                message.topicAlias = topicAlias
                break;
            }
        }

        /* if topicAlias has not been set, try to store new topic alias */
        if message.topicAlias == nil {
            if self.topicAliasMaximumOut > self.topicAliasesOut.count {
                MqttCompliance.sharedInstance().log(target: "Set new topic alias out")
                let newTopicAlias = self.topicAliasesOut.count + 1
                self.topicAliasesOut[newTopicAlias] = message.topic
                message.topicAlias = newTopicAlias
            }
        }

        if message.qos == MqttQoS.AtMostOnce {
            if self.socket != nil {
                self.send(message:message)
            } else {
                self.debug(s: "Dropping message \(clientId) \(message.topic) \(message.data)", p:nil)
                MqttSessions.sharedInstance().droppedQoS0Messages = MqttSessions.sharedInstance().droppedQoS0Messages + 1
            }

        } else {
            message.packetIdentifier = Int(self.nextPacketIdentifier())
            if self.socket != nil {
                self.send(message:message)
                self.inflightOut.store(message: message)
            } else {
                self.debug(s: "Queueing to \(clientId) \(message.topic) \(message.data)", p:nil)
                self.queued.append(message:message)
            }
        }
    }

    func send(message: MqttMessage!) {
        self.debug(s: "Sending to \(clientId) q\(message.qos) pid\(message.packetIdentifier) \(message.topic) \(message.data) pFI=\(message.payloadFormatIndicator) pEI=\(message.publicationExpiryInterval) tA=\(message.topicAlias) sI=\(message.subscriptionIdentifiers)", p:nil)
        var publish = Data()
        var u : UInt8

        u = MqttControlPacketType.PUBLISH.rawValue << 4 |
            message.qos.rawValue << 1 |
            (message.retain ? 0x01 : 0x00)

        publish.append(u)

        var properties = Data()

        if message.payloadFormatIndicator != nil {
            u = MqttPropertyIdentifier.PayloadFormatIndicator.rawValue
            properties.append(u)
            u = message.payloadFormatIndicator!
            properties.append(u)
        }

        if message.publicationExpiryInterval != nil {
            u = MqttPropertyIdentifier.PublicationExpiryInterval.rawValue
            properties.append(u)
            properties.append(MqttControlPacket.mqttFourByte(variable: message.publicationExpiryInterval!))
        }

        if message.topicAlias != nil {
            u = MqttPropertyIdentifier.TopicAlias.rawValue
            properties.append(u)
            properties.append(MqttControlPacket.mqttTwoByte(variable: message.topicAlias!))
        }

        if message.subscriptionIdentifiers != nil {
            for subscriptionIdentifier in message.subscriptionIdentifiers! {
                u = MqttPropertyIdentifier.SubscriptionIdentifier.rawValue
                properties.append(u)
                properties.append(MqttControlPacket.mqttVariableByte(variable: subscriptionIdentifier))
            }
        }

        if message.responseTopic != nil {
            u = MqttPropertyIdentifier.ResponseTopic.rawValue
            properties.append(u)
            properties.append(MqttControlPacket.mqttUtf8(variable: message.responseTopic!))
        }

        if message.correlationData != nil {
            u = MqttPropertyIdentifier.CorrelationData.rawValue
            properties.append(u)
            properties.append(MqttControlPacket.mqttVariableByte(variable: message.correlationData!.count))
            properties.append(message.correlationData!)
        }

        if message.contentType != nil {
            u = MqttPropertyIdentifier.ContentType.rawValue
            properties.append(u)
            properties.append(MqttControlPacket.mqttUtf8(variable: message.contentType!))
        }

        var userProperties = Data()

        if message.userProperties != nil {
            for userProperty in message.userProperties! {
                for (propertyName, propertyValue) in userProperty {
                    u = MqttPropertyIdentifier.UserProperty.rawValue
                    userProperties.append(u)
                    userProperties.append(MqttControlPacket.mqttUtf8(variable:propertyName))
                    userProperties.append(MqttControlPacket.mqttUtf8(variable: propertyValue))
                }
            }

            let additionalUserProperties = publish.count +
                MqttControlPacket.mqttVariableByte(variable: properties.count).count +
                properties.count +
                userProperties.count

            if self.maximumPacketSize != nil  &&
                additionalUserProperties <= self.maximumPacketSize! {
                properties.append(userProperties)
            }
        }

        let td = MqttControlPacket.mqttUtf8(variable: message.topic)
        if message.qos != MqttQoS.AtMostOnce {
            publish.append(MqttControlPacket.mqttVariableByte(variable: td.count + 2 + MqttControlPacket.mqttVariableByte(variable: properties.count).count + properties.count + message.data.count))
        } else {
            publish.append(MqttControlPacket.mqttVariableByte(variable: td.count + MqttControlPacket.mqttVariableByte(variable: properties.count).count + properties.count + message.data.count))
        }
        publish.append(td)
        if message.qos != MqttQoS.AtMostOnce {
            publish.append(MqttControlPacket.mqttTwoByte(variable: message.packetIdentifier!))
        }

        publish.append(MqttControlPacket.mqttVariableByte(variable: properties.count))
        publish.append(properties)

        publish.append(message.data)

        if self.maximumPacketSize != nil && self.maximumPacketSize! < publish.count {
            self.debug(s: "write \(clientId) too long \(publish.count)/\(self.maximumPacketSize!)", p: nil)
            MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-28")
        } else {
            if self.write(data: publish) {
                MqttSessions.sharedInstance().sentMessages = MqttSessions.sharedInstance().sentMessages + 1
            }
        }
    }

    func connect(cp: MqttControlPacket!) -> (Bool, MqttReturnCode) {
        self.lastPacket = Date()
        let mqttProperties = cp!.mqttProperties()
        self.topicAliasesIn = [Int: String]()
        self.topicAliasesOut = [Int: String]()
        if mqttProperties != nil &&
            mqttProperties!.topicAliasMaximum != nil &&
            mqttProperties!.topicAliasMaximum! > 0 {
            self.topicAliasMaximumOut =  mqttProperties!.topicAliasMaximum!
        } else {
            self.topicAliasMaximumOut = 0
        }
        self.topicAliasMaximumIn = MqttServer.sharedInstance().topicAliasMaximum

        if mqttProperties != nil {
            if (mqttProperties!.willDelayInterval != nil) {
                self.willDelayInterval = mqttProperties!.willDelayInterval!
            }
            if (mqttProperties!.sessionExpiryInterval != nil) {
                self.sessionExpiryInterval = mqttProperties!.sessionExpiryInterval!
            } else {
                self.sessionExpiryInterval = 0
            }
            self.maximumSessionExpiryInterval = MqttServer.sharedInstance().maximumSessionExpiryInterval

            /* Maximum Packet Size */
            if (mqttProperties!.maximumPacketSize != nil) {
                if mqttProperties!.maximumPacketSize! == 0 ||
                    mqttProperties!.maximumPacketSize! >  2684354565 {
                    MqttCompliance.sharedInstance().log(target: "Maximum Packet Size illegal Value")
                    return (false, MqttReturnCode.ProtocolError)
                }
                self.maximumPacketSize = mqttProperties!.maximumPacketSize
            }
        }

        MqttCompliance.sharedInstance().log(target: "MQTT-3.2.0-1")
        MqttCompliance.sharedInstance().log(target: "MQTT-3.2.0-2")

        self.connectKeepAlive = cp!.mqttConnectKeepAlive()!
        self.serverKeepAlive = cp!.mqttConnectKeepAlive()!

        var connack = Data()
        var u : UInt8
        u = MqttControlPacketType.CONNACK.rawValue << 4
        connack.append(u)

        MqttCompliance.sharedInstance().log(target: "MQTT-3.2.2-1")

        var variableData = Data()

        u = 0
        if self.present != nil && self.present! {
            u = u | 1
        }
        variableData.append(u)
        u = 0
        if returnCode != nil {
            u = returnCode!.rawValue
        }
        variableData.append(u)

        var remaining = Data()
        u = MqttPropertyIdentifier.ReceiveMaximum.rawValue
        remaining.append(u)
        remaining.append(MqttControlPacket.mqttTwoByte(variable: MqttServer.sharedInstance().receiveMaximum))

        u = MqttPropertyIdentifier.MaximumQoS.rawValue
        remaining.append(u)
        u = MqttServer.sharedInstance().maximumQoS.rawValue
        remaining.append(u)

        u = MqttPropertyIdentifier.RetainAvailable.rawValue
        remaining.append(u)
        u = MqttServer.sharedInstance().retainAvailable ? 1 : 0
        remaining.append(u)

        u = MqttPropertyIdentifier.MaximumPacketSize.rawValue
        remaining.append(u)
        remaining.append(MqttControlPacket.mqttFourByte(variable: MqttServer.sharedInstance().maximumPacketSize))

        u = MqttPropertyIdentifier.AssignedClientIdentifier.rawValue
        remaining.append(u)
        remaining.append(MqttControlPacket.mqttUtf8(variable: self.clientId))

        u = MqttPropertyIdentifier.TopicAliasMaximum.rawValue
        remaining.append(u)
        remaining.append(MqttControlPacket.mqttTwoByte(variable: MqttServer.sharedInstance().topicAliasMaximum))

        u = MqttPropertyIdentifier.ResponseInformation.rawValue
        remaining.append(u)
        remaining.append(MqttControlPacket.mqttUtf8(variable: "Todo"))

        u = MqttPropertyIdentifier.WildcardSubscriptionAvailable.rawValue
        remaining.append(u)
        u = MqttServer.sharedInstance().wildcardSubscritionAvailable ? 1 : 0
        remaining.append(u)

        u = MqttPropertyIdentifier.SubscriptionIdentifiersAvailable.rawValue
        remaining.append(u)
        u = MqttServer.sharedInstance().subscriptionIdentifiersAvailable ? 1 : 0
        remaining.append(u)

        u = MqttPropertyIdentifier.SharedSubscriptionAvailable.rawValue
        remaining.append(u)
        u = MqttServer.sharedInstance().sharedSubscriptionAvailable ? 1 : 0
        remaining.append(u)

        if (MqttServer.sharedInstance().serverKeepAlive != nil) {
            self.serverKeepAlive = MqttServer.sharedInstance().serverKeepAlive!
            u = MqttPropertyIdentifier.ServerKeepAlive.rawValue
            remaining.append(u)
            remaining.append(MqttControlPacket.mqttTwoByte(variable: self.serverKeepAlive))
        }

        if mqttProperties != nil &&
            mqttProperties!.topicAliasMaximum != nil &&
            mqttProperties!.topicAliasMaximum! > 0 {
            self.topicAliasMaximumOut =  mqttProperties!.topicAliasMaximum!
        } else {
            self.topicAliasMaximumOut = 0
        }

        if (self.sessionExpiryInterval! != self.maximumSessionExpiryInterval) {
            u = MqttPropertyIdentifier.SessionExpiryInterval.rawValue
            remaining.append(u)
            remaining.append(MqttControlPacket.mqttFourByte(variable: self.maximumSessionExpiryInterval))
        }

        if (MqttServer.sharedInstance().serverMoved != nil) {
            u = MqttPropertyIdentifier.ServerReference.rawValue
            remaining.append(u)
            remaining.append(MqttControlPacket.mqttUtf8(variable: MqttServer.sharedInstance().serverMoved!))
        } else if (MqttServer.sharedInstance().serverToUse != nil) {
            u = MqttPropertyIdentifier.ServerReference.rawValue
            remaining.append(u)
            remaining.append(MqttControlPacket.mqttUtf8(variable: MqttServer.sharedInstance().serverToUse!))
        }

        u = MqttPropertyIdentifier.ResponseInformation.rawValue
        remaining.append(u)
        remaining.append(MqttControlPacket.mqttUtf8(variable: "Todo"))

        let reasonString = "Reason"
        let additionalReasonString = connack.count +
            variableData.count +
            MqttControlPacket.mqttVariableByte(variable: remaining.count).count +
            remaining.count +
            1 +
            MqttControlPacket.mqttUtf8(variable: reasonString).count

        if self.maximumPacketSize != nil  &&
            additionalReasonString <= self.maximumPacketSize! {
            u = MqttPropertyIdentifier.ReasonString.rawValue
            remaining.append(u)
            remaining.append(MqttControlPacket.mqttUtf8(variable: reasonString))
        }

        var userProperties = Data()

        for (propertyName, propertyValue) in MqttServer.sharedInstance().userProperties {
            u = MqttPropertyIdentifier.UserProperty.rawValue
            userProperties.append(u)
            userProperties.append(MqttControlPacket.mqttUtf8(variable:propertyName))
            userProperties.append(MqttControlPacket.mqttUtf8(variable: propertyValue))
        }

        let additionalUserProperties = connack.count +
            variableData.count +
            MqttControlPacket.mqttVariableByte(variable: remaining.count).count +
            remaining.count +
            userProperties.count

        if self.maximumPacketSize != nil  &&
            additionalUserProperties <= self.maximumPacketSize! {
            u = MqttPropertyIdentifier.ReasonString.rawValue
            remaining.append(userProperties)
        }

        /* Auth Method */
        /* Auth Data */

        variableData.append(MqttControlPacket.mqttVariableByte(variable: remaining.count))
        variableData.append(remaining)

        connack.append(MqttControlPacket.mqttVariableByte(variable: variableData.count))
        connack.append(variableData)

        self.debug(s:"CONNACK sent", p:nil)
        return (self.write(data: connack), MqttReturnCode.Success)
    }

    func publish(cp: MqttControlPacket!) -> (Bool, MqttReturnCode) {
        print ("PUBLISH received")
        self.lastPacket = Date()

        var topic = cp!.mqttTopic()

        if topic == nil {
            MqttCompliance.sharedInstance().log(target: "MQTT-3.3.2-1")
            return (false, MqttReturnCode.TopicNameInvalid)
        }

        if topic!.contains("+") {
            MqttCompliance.sharedInstance().log(target: "MQTT-3.3.2-2")
            return (false, MqttReturnCode.TopicNameInvalid)
        }

        if topic!.contains("#") {
            MqttCompliance.sharedInstance().log(target: "MQTT-3.3.2-2")
            return (false, MqttReturnCode.TopicNameInvalid)
        }

        let retain = cp!.mqttControlPacketRetain()
        let qos = cp!.mqttControlPacketQoS()
        let mqttProperties = cp!.mqttProperties()

        if topic!.characters.count == 0  &&
            (mqttProperties == nil || mqttProperties!.topicAlias == nil || mqttProperties!.topicAlias! <= 0) {
            MqttCompliance.sharedInstance().log(target: "MQTT-4.7.3-1")
            return (false, MqttReturnCode.TopicNameInvalid)
        }

        if topic!.characters.count == 0  &&
            (mqttProperties != nil && mqttProperties!.topicAlias != nil && mqttProperties!.topicAlias! > 0 &&
            self.topicAliasesIn[mqttProperties!.topicAlias!] != nil) {
            MqttCompliance.sharedInstance().log(target: "Replaced Topic from Topic Alias")
            topic = self.topicAliasesIn[mqttProperties!.topicAlias!]
            mqttProperties!.topicAlias = nil
        }

        if mqttProperties != nil && mqttProperties!.topicAlias != nil && mqttProperties!.topicAlias! > 0 {
            let oldTopicAliasIn = self.topicAliasesIn[mqttProperties!.topicAlias!]
            if oldTopicAliasIn == nil {
                if self.topicAliasesIn.count < self.topicAliasMaximumIn && mqttProperties!.topicAlias! <= self.topicAliasMaximumIn {
                    MqttCompliance.sharedInstance().log(target: "Set new Topic Alias")
                    self.topicAliasesIn[mqttProperties!.topicAlias!] = topic!
                    mqttProperties!.topicAlias = nil
                } else {
                    MqttCompliance.sharedInstance().log(target: "[MQTT-3.3.2-9]")
                    return (false, MqttReturnCode.TopicAliasInvalid)
                }
            } else {
                MqttCompliance.sharedInstance().log(target: "Overwrite Topic Alias")
                self.topicAliasesIn[mqttProperties!.topicAlias!] = topic!
                mqttProperties!.topicAlias = nil
            }
        }

        if qos == nil {
            MqttCompliance.sharedInstance().log(target: "MQTT-3.3.1-4")
            return (false, MqttReturnCode.ProtocolError)
        }

        let payload = cp!.mqttPayload()
        var message: MqttMessage?
        if payload != nil {
            message = MqttMessage(topic:topic!,
                                  data:payload!,
                                  qos:qos!,
                                  retain:retain!)
            message!.payloadFormatIndicator = mqttProperties != nil ? mqttProperties!.payloadFormatIndicator : 0
            message!.publicationExpiryInterval = mqttProperties != nil ? mqttProperties!.publicationExpiryInterval : nil
            message!.responseTopic = mqttProperties != nil ? mqttProperties!.responseTopic : nil
            message!.correlationData = mqttProperties != nil ? mqttProperties!.correlationData : nil
            message!.contentType = mqttProperties != nil ? mqttProperties!.contentType : nil
            message!.topicAlias = mqttProperties != nil ? mqttProperties!.topicAlias : nil
            message!.userProperties = mqttProperties != nil ? mqttProperties!.userProperties : nil
            message!.subscriptionIdentifiers = mqttProperties != nil ? mqttProperties!.subscriptionIdentifiers : nil
        }

        if (qos == MqttQoS.AtMostOnce) {
            MqttSessions.processReceivedMessage(message: message!, from:self.clientId)

        } else if (qos == MqttQoS.AtLeastOnce) {
            MqttSessions.processReceivedMessage(message: message!, from:self.clientId)

            var puback = Data()
            var u : UInt8
            u = MqttControlPacketType.PUBACK.rawValue << 4
            puback.append(u)
            let pid = cp!.mqttPacketIdentifier()
            if pid != nil {
                u = 4
                puback.append(u)
                puback.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                u = 0
                puback.append(u)
                puback.append(MqttControlPacket.mqttVariableByte(variable: 0))

                /* TODO Reason String */
                /* TODO User Properties */

                self.debug(s:"PUBACK sent", p:pid)
                return (self.write(data: puback), MqttReturnCode.Success)
            }

        } else if (qos == MqttQoS.ExactlyOnce) {
            let pid = cp!.mqttPacketIdentifier()
            if message != nil {
                message!.packetIdentifier = pid
                self.inflightIn.store(message:message!)
            }
            var pubrec = Data()
            var u : UInt8
            u = (MqttControlPacketType.PUBREC.rawValue << 4)
            pubrec.append(u)
            if pid != nil {
                u = 4
                pubrec.append(u)
                pubrec.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                u = 0
                pubrec.append(u)
                pubrec.append(MqttControlPacket.mqttVariableByte(variable: 0))

                /* TODO Reason String */
                /* TODO User Properties */

                self.debug(s:"PUBREC sent", p:pid)
                return (self.write(data: pubrec), MqttReturnCode.Success)
            }
        }
        return (true, MqttReturnCode.Success)
    }

    func puback(cp: MqttControlPacket!) -> Bool {
        self.lastPacket = Date()
        let pid = cp!.mqttPacketIdentifier()
        self.debug(s:"PUBACK received", p:pid)

        let message = self.inflightOut.find(pid:pid)
        if message != nil {
            self.inflightOut.remove(pid:pid)
            if self.socket != nil {
                let message = self.queued.get()
                if message != nil {
                    self.send(message:message)
                    self.inflightOut.store(message:message)
                }
            }
        } else {
            MqttSessions.processReceivedMessage(
                message: MqttMessage(topic: "$SYS/broker/log/E/puback",
                                     data: "\(Int(Date.init().timeIntervalSince1970)): \(self.clientId) pid not found \(pid != nil ? pid! : -1)".data(using:String.Encoding.utf8),
                                     qos: .AtMostOnce,
                                     retain: false),
                from:"")
        }
        return true
    }

    func pubrec(cp: MqttControlPacket!) -> Bool {
        self.lastPacket = Date()
        let pid = cp!.mqttPacketIdentifier()
        self.debug(s:"PUBREC received", p:pid)

        let message = self.inflightOut.find(pid:pid)
        if message != nil {
            message!.pubrel = true
            self.inflightOut.update(message:message)

            var pubrel = Data()
            var u : UInt8
            u = (MqttControlPacketType.PUBREL.rawValue << 4) | 0x02
            pubrel.append(u)

            u = 4
            pubrel.append(u)
            pubrel.append(MqttControlPacket.mqttTwoByte(variable: pid!))
            u = 0
            pubrel.append(u)
            pubrel.append(MqttControlPacket.mqttVariableByte(variable: 0))

            /* TODO Reason String */
            /* TODO User Properties */

            self.debug(s:"PUBREL sent", p:pid)
            return self.write(data: pubrel)
        } else {
            MqttSessions.processReceivedMessage(
                message: MqttMessage(topic: "$SYS/broker/log/E/pubrel",
                                     data: "\(Int(Date.init().timeIntervalSince1970)): \(self.clientId) pid not found \(pid != nil ? pid! : -1)".data(using:String.Encoding.utf8),
                                     qos: .AtMostOnce,
                                     retain: false),
                from:"")
        }
        return true
    }

    func pubrel(cp: MqttControlPacket!) -> Bool {
        self.lastPacket = Date()
        let pid = cp!.mqttPacketIdentifier()
        self.debug(s:"PUBREL received", p:pid)

        let message = self.inflightIn.find(pid:pid)
        if message != nil {
            MqttSessions.processReceivedMessage(message: message!, from: self.clientId)
            self.inflightIn.remove(pid:pid)

            var pubcomp = Data()
            var u : UInt8
            u = (MqttControlPacketType.PUBCOMP.rawValue << 4)
            pubcomp.append(u)

            u = 4
            pubcomp.append(u)
            pubcomp.append(MqttControlPacket.mqttTwoByte(variable: pid!))
            u = 0
            pubcomp.append(u)
            pubcomp.append(MqttControlPacket.mqttVariableByte(variable: 0))

            /* TODO Reason String */
            /* TODO User Properties */

            self.debug(s:"PUBCOMP sent", p:pid)
            return self.write(data: pubcomp)
        } else {
            MqttSessions.processReceivedMessage(
                message: MqttMessage(topic: "$SYS/broker/log/E/pubrel",
                                     data: "\(Int(Date.init().timeIntervalSince1970)): \(self.clientId) pid not found \(pid != nil ? pid! : -1)".data(using:String.Encoding.utf8),
                                     qos: .AtMostOnce,
                                     retain: false),
                from:"")
        }
        return true
    }

    func pubcomp(cp: MqttControlPacket!) -> Bool {
        self.lastPacket = Date()
        let pid = cp!.mqttPacketIdentifier()
        self.debug(s:"PUBCOMP received", p:pid)

        let message = self.inflightOut.find(pid:pid)
        if message != nil {
            self.inflightOut.remove(pid:pid)
            if self.socket != nil {
                let message = self.queued.get()
                if message != nil {
                    self.send(message:message)
                    self.inflightOut.store(message:message)
                }
            }
        } else {
            MqttSessions.processReceivedMessage(
                message: MqttMessage(topic: "$SYS/broker/log/E/pubcomp",
                                     data: "\(Int(Date.init().timeIntervalSince1970)): \(self.clientId) pid not found \(pid != nil ? pid! : -1)".data(using:String.Encoding.utf8),
                                     qos: .AtMostOnce,
                                     retain: false),
                from:"")
        }
        return true
    }

    func subscribe(cp: MqttControlPacket!) -> (Bool, MqttReturnCode) {
        print ("SUBSCRIBE received")
        self.lastPacket = Date()

        let sf = cp!.mqttSubscriptions()

        if sf.count == 0 {
            MqttCompliance.sharedInstance().log(target: "MQTT-3.8.3-2")
            return (false, MqttReturnCode.ProtocolError)
        }

        
        if sf.count > 1 {
            MqttCompliance.sharedInstance().log(target: "MQTT-3.8.4-5")
        }

        for (subscription) in sf {
            if subscription.qos.rawValue == 3 {
                MqttCompliance.sharedInstance().log(target: "MQTT-3.8.3-5")
                return (false, MqttReturnCode.ProtocolError)
            }

            if !MqttServer.sharedInstance().wildcardSubscritionAvailable &&
                (subscription.topicFilter.contains("#") ||
                    subscription.topicFilter.contains("+")) {
                MqttCompliance.sharedInstance().log(target: "SUBSCRIBE contains wildcards which are not supported")
                return (false, MqttReturnCode.WildcardSubscriptionNotSupported)
            }

            if subscription.topicFilter.count == 0 {
                MqttCompliance.sharedInstance().log(target: "MQTT-4.7.3-1")
                return (false, MqttReturnCode.ProtocolError)
            }

            if (subscription.subscribeOptions & 0xc0) != 0x00 {
                MqttCompliance.sharedInstance().log(target: "MQTT-3.8.3-5")
                return (false, MqttReturnCode.ProtocolError)
            }

            if subscription.qos.rawValue > MqttServer.sharedInstance().maximumQoS.rawValue {
                MqttCompliance.sharedInstance().log(target: "SUBSCRIBE > maximumQoS")
                subscription.qos = MqttServer.sharedInstance().maximumQoS
            }

            self.store(subscription: subscription)
            MqttSessions.processReceivedMessage(
                message: MqttMessage(topic: "$SYS/broker/log/M/subscribe",
                                     data: "\(Int(Date.init().timeIntervalSince1970)): \(self.clientId) \(subscription.qos) n=\(subscription.noLocal) r=\(subscription.retainAsPublish) \(subscription.retainHandling) i=\(subscription.subscriptionIdentifier != nil ? subscription.subscriptionIdentifier! : nil) \(subscription.topicFilter!)".data(using:String.Encoding.utf8),
                                     qos: .AtMostOnce,
                                     retain: true),
                from:"")
        }

        var suback = Data()
        var u : UInt8
        u = (MqttControlPacketType.SUBACK.rawValue << 4)
        suback.append(u)

        let remainingLength = 2 + 1 + sf.count
        let remainingLengthData = MqttControlPacket.mqttVariableByte(variable: remainingLength)
        suback.append(remainingLengthData)

        let pid = cp!.mqttPacketIdentifier()
        if pid != nil {
            suback.append(MqttControlPacket.mqttTwoByte(variable: pid!))
        }
        u = 0
        suback.append(u)

        if sf.count > 1 {
            MqttCompliance.sharedInstance().log(target: "MQTT-3.8.4-6")
        }

        for subscription in sf {
            MqttCompliance.sharedInstance().log(target: "MQTT-3.8.4-7")
            suback.append(subscription.qos.rawValue)
        }

        /* TODO Reason String */
        /* TODO User Properties */

        self.debug(s:"SUBACK sent", p:pid)
        return (self.write(data: suback), MqttReturnCode.Success)
    }

    func unsubscribe(cp: MqttControlPacket!) -> Bool {
        self.debug(s:"UNSUBSCRIBE received", p:nil)
        self.lastPacket = Date()

        let uf = cp!.mqttUnsubscribeTopicFilters()

        var unsuback = Data()
        var u : UInt8
        u = (MqttControlPacketType.UNSUBACK.rawValue << 4)
        unsuback.append(u)

        let remainingLength = 2 + 1 + uf.count
        let remainingLengthData = MqttControlPacket.mqttVariableByte(variable: remainingLength)
        unsuback.append(remainingLengthData)

        let pid = cp!.mqttPacketIdentifier()
        if pid != nil {
            unsuback.append(MqttControlPacket.mqttTwoByte(variable: pid!))
        }

        u = 0
        unsuback.append(u)

        for (topicFilter) in uf {
            let existingTopicFilter = self.get(topicFilter:topicFilter)
            if existingTopicFilter == nil {
                u = MqttReturnCode.NoSubscriptionExisted.rawValue
            } else {
                self.remove(topicFilter:topicFilter)
                u = 0
            }
            MqttSessions.processReceivedMessage(
                message: MqttMessage(topic: "$SYS/broker/log/M/unsubscribe",
                                     data: "\(Int(Date.init().timeIntervalSince1970)): \(self.clientId) \(topicFilter)".data(using:String.Encoding.utf8),
                                     qos: .AtMostOnce,
                                     retain: true),
                from:"")

            unsuback.append(u)
        }

        /* TODO Reason String */
        /* TODO User Properties */

        self.debug(s:"UNSUBACK sent", p:pid)
        return self.write(data: unsuback)
    }

    func pingreq(cp: MqttControlPacket!)  -> Bool {
        self.debug(s:"PINGREQ received", p:nil)
        self.lastPacket = Date()

        var pingresp = Data()
        var u : UInt8
        u = MqttControlPacketType.PINGRESP.rawValue << 4
        pingresp.append(u)
        u = 0
        pingresp.append(u)

        self.debug(s:"PINGRESP received", p:nil)
        return self.write(data: pingresp)
    }

    func disconnect(cp: MqttControlPacket!) -> Bool {
        self.debug(s:"DISCONNECT received", p:nil)
        self.lastPacket = Date()

        let rc = cp!.mqttReturnCode()
        if rc != nil {
            if rc == MqttReturnCode.DisconnectWithWillMessage {
                self.askForWill()
            }
        }

        let mqttProperties = cp!.mqttProperties()
        if mqttProperties != nil {
            if mqttProperties!.sessionExpiryInterval != nil {
                self.sessionExpiryInterval = mqttProperties!.sessionExpiryInterval
            }
        }

        return false
    }

    func auth(cp: MqttControlPacket!) -> Bool {
        self.debug(s:"AUTH received", p:nil)
        self.lastPacket = Date()

        return false
    }

    func write(data: Data!) -> Bool {
        let writeBuffer = [UInt8](data)
        print("writeBuffer \(writeBuffer)")

        do {
            if self.socket != nil {
                try self.socket!.write(from: data)
            }
            return true
        } catch let error {
            guard let socketError = error as? Socket.Error else {
                print("Unexpected error by connection at \(self.socket!.remoteHostname):\(self.socket!.remotePort)...")
                return false
            }
            if self.socket != nil {
                print("Error reported by connection at \(self.socket!.remoteHostname):\(self.socket!.remotePort):\n \(socketError.description)")
                self.socket!.close()
            } else {
                print("Error reported by connection at \(socketError.description)")
            }
            self.socket = nil
            return false
        }
    }

}
