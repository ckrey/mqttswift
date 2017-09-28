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

	private init() {
        self.retained = [String: MqttMessage]()
	}

    func store(message: MqttMessage!) {
		if message.data.count == 0 {
			self.retained[message.topic] = nil
		} else {
			self.retained[message.topic] = message
        }
	}
}
