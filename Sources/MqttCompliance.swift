import Foundation

class MqttCompliance {
    static let complianceReference: [String: String] = [
        "MQTT-7.1.2-1": "A conformant Client MUST support the use of one or more underlying transport protocols that provide an ordered, lossless, stream of bytes from the Client to Server and Server to Client."
    ]
    static let clientFeatures: [String] = [
        "MQTT-7.1.2-1"
    ]
    static let clientEvents: [String] = [
    ]
    static let serverFeatures: [String] = [
    ]
    static let serverEvents: [String] = [
        "MQTT-3.1.0-1", "MQTT-3.1.0-2"
    ]
    
    var targets : [String: Int]
    let verbose: Bool

    init(verbose: Bool) {
        self.verbose = verbose
        self.targets = [String: Int]()
    }

    func log(target: String) {
        if self.verbose {
            print("MQTTCompliance log \(target)")
        }
        var count = self.targets[target]
        if (count == nil) {
            count = 0
        }
        self.targets[target] = count! + 1
    }

    func summary() {
        print ("MQTTCompliance Summary")
        for (target, count) in self.targets {
            print("\(target): \(count)")
        }
    }
}
