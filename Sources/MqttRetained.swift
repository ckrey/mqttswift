import Foundation

class MqttRetained {
    private static var theInstance: MqttRetained?

    public class func sharedInstance() -> MqttRetained {
        if theInstance == nil {
            theInstance = MqttRetained()
        }
        return theInstance!
    }

	var retained : [String: Data]

	private init() {
		self.retained = [String: Data]()
	}

	func store(topic: String, data: Data) {
		if data.count == 0 {
			self.retained[topic] = nil
		} else {
			self.retained[topic] = data
		}
	}

	func get(topic: String) -> Data? {
		return self.retained[topic];
	}

    func summary() {
        print ("MqttRetained Summary")
        for (topic, data) in self.retained {
            print("\(topic): \(data)")
        }
    }
}
