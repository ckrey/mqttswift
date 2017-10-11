import Foundation

class MqttRetained {
    private static var theInstance: MqttRetained?

    public class func sharedInstance() -> MqttRetained {
        if theInstance == nil {
            theInstance = MqttRetained()
        }
        return theInstance!
    }

    var retained : [String: MqttMessage]
    private var stateTimer: DispatchSourceTimer

	private init() {
        self.retained = [String: MqttMessage]()

        let q = DispatchQueue.global()
        self.stateTimer = DispatchSource.makeTimerSource(queue: q)
        self.stateTimer.scheduleRepeating(deadline: .now(), interval: .seconds(1), leeway: .seconds(1))
        self.stateTimer.setEventHandler { [weak self] in
            self!.expired()
        }
        self.stateTimer.resume()
	}

    func expired() {
        let now = Date()
        for (topic, message) in retained {
            if message.publicationExpiryTime != nil {
                if message.publicationExpiryTime! < now {
                    MqttCompliance.sharedInstance().log(target: "retained message expired")
                    retained[topic] = nil
                }
            }
        }
    }

    func store(message: MqttMessage!) {
		if message.data.count == 0 {
			self.retained[message.topic] = nil
		} else {
			self.retained[message.topic] = message
        }
	}
}
