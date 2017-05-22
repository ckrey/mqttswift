import Foundation
import Socket

class MqttSession {
	var clientId: String
	var socket: Socket?
    var connectKeepAlive: Int
    var serverKeepAlive: Int
    var willDelayInterval: Int
    var sessionExpiryInterval: Int?
    var willFlag: Bool
    var willTopic: String?
    var willMessage: Data?
    var willQoS: MqttQoS?
    var willRetain: Bool?

    var shouldKeepRunning: Bool
    var returnCode: MqttReturnCode?
    var lastPacket: Date
    var subscriptions: [String: MqttQoS]
    var present: Bool?
    var deleted: Bool

    private var stateTimer: DispatchSourceTimer
    private var pendingWillAt: Date?
    private var pendingDeleteAt: Date?

	init(clientId: String) {
		self.clientId = clientId
        self.shouldKeepRunning = true
		self.subscriptions = [String: MqttQoS]()
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

        let q = DispatchQueue.global()

        self.stateTimer = DispatchSource.makeTimerSource(queue: q)
        self.stateTimer.scheduleRepeating(deadline: .now(), interval: .seconds(1), leeway: .seconds(1))
        self.stateTimer.setEventHandler { [weak self] in
            self!.states()
        }
        self.stateTimer.resume()

	}

    func states() {
        let now = Date()

        print ("Checking_ \(self.clientId)")

        if !deleted {
            if socket != nil {
                let noPacketSince = Int(now.timeIntervalSince1970 - self.lastPacket.timeIntervalSince1970)
                print ("Checking_ \(self.clientId) No Packet since \(noPacketSince) (\(self.connectKeepAlive)/\(self.serverKeepAlive))")

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
                            print ("Scheduled \(self.clientId) deletion in \(self.sessionExpiryInterval)")
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
        print ("Sending__ \(self.clientId) Will")
        MqttSessions.processReceivedMessage(topic: self.willTopic!,
                                            payload: self.willMessage!,
                                            retain: self.willRetain!,
                                            qos: self.willQoS!)
        self.pendingWillAt = nil
    }

	func store(topicFilter: String, qos: MqttQoS) {
		self.subscriptions[topicFilter] = qos
	}

	func get(topicFilter: String) -> MqttQoS? {
		return self.subscriptions[topicFilter];
	}

	func summary() {
		print ("MqttSession Subscription Summary")
		for (topicFilter, qos) in self.subscriptions {
			print("\(topicFilter): \(qos)")
		}
	}
}
