import Foundation
import Socket

class MqttSession {
    var clientId: String
    var socket: Socket?
    var connectKeepAlive: Int
    var serverKeepAlive: Int
    var willDelayInterval: Int
    var sessionExpiryInterval: Int?
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
        self.shouldKeepRunning = true
        self.subscriptions = [String: MqttSubscription]()
        self.queued = MqttQueued()
        self.inflightIn = MqttInflight()
        self.inflightOut = MqttInflight()
        self.connectKeepAlive = 60
        self.serverKeepAlive = 60
        self.willDelayInterval = 0
        self.sessionExpiryInterval = nil
        
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
        MqttSessions.processReceivedMessage(message: message)
        self.pendingWillAt = nil
    }

    func store(subscription: MqttSubscription!) {

        let existingSubscription = self.get(topicFilter: subscription.topicFilter)
        
        if subscription.retainHandling == MqttRetainHandling.SendRetained ||
            subscription.retainHandling == MqttRetainHandling.SendRetainedIfNotYetSubscribed && existingSubscription == nil {
            for (topic, message) in MqttRetained.sharedInstance().retained {
                if MqttSessions.matches(topic: topic, topicFilter: subscription.topicFilter) {
                    self.publish(message:message)
                }
            }
        }
        self.subscriptions[subscription.topicFilter] = subscription
    }

    func remove(topicFilter: String) {
        self.subscriptions[topicFilter] = nil
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
        self.debug(s: "Sending to \(clientId) q\(message.qos) pid\(message.packetIdentifier) \(message.topic) \(message.data)", p:nil)
        var publish = Data()
        var u : UInt8

        u = MqttControlPacketType.PUBLISH.rawValue << 4 |
            message.qos.rawValue << 1 |
            (message.retain ? 0x01 : 0x00)

        publish.append(u)
        let td = MqttControlPacket.mqttUtf8(variable: message.topic)
        if message.qos != MqttQoS.AtMostOnce {
            publish.append(MqttControlPacket.mqttVariableByte(variable: td.count + 2 + 1 + message.data.count))
        } else {
            publish.append(MqttControlPacket.mqttVariableByte(variable: td.count + 1 + message.data.count))
        }
        publish.append(td)
        if message.qos != MqttQoS.AtMostOnce {
            publish.append(MqttControlPacket.mqttTwoByte(variable: message.packetIdentifier!))
        }
        u = 0
        publish.append(u)
        publish.append(message.data)

        if self.maximumPacketSize != nil && self.maximumPacketSize! < publish.count {
            self.debug(s: "write \(clientId) too long \(publish.count)/\(self.maximumPacketSize!)", p: nil)
            MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-28")
        } else {
            self.debug(s: "write \(clientId) \(self.lastPacketIdentifier) \(message.topic) \(message.data)", p: nil)
            do {
                try self.socket!.write(from: publish)
            } catch {
                //
            }
            MqttSessions.sharedInstance().sentMessages = MqttSessions.sharedInstance().sentMessages + 1
        }
    }
}
