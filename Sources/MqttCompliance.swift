import Foundation

class MqttCompliance {
    private static var theInstance: MqttCompliance?

    public class func sharedInstance() -> MqttCompliance {
        if theInstance == nil {
            theInstance = MqttCompliance()
        }
        return theInstance!
    }

    static let clientFeatures : Set = [
        "MQTT-7.1.2-1"
    ]
    static let clientEvents: Set = [
        "MQTT-3.3.1-4"
    ]
    static let serverFeatures: Set = [
        "MQTT-3.2.0-2"
    ]
    static let serverEvents: Set = [
        "MQTT-3.1.0-1", "MQTT-3.1.0-2"
    ]

    var targets : [String: Int]

    init() {
        self.targets = [String: Int]()
    }

    func log(target: String) {
        var count = self.targets[target]
        if (count == nil) {
            count = 0
        }
        self.targets[target] = count! + 1
    }

    func stats() {
        var checkedClientFeatures: Set<String> = []
        var checkedClientEvents: Set<String> = []
        var checkedServerFeatures: Set<String> = []
        var checkedServerEvents: Set<String> = []

        for (target, count) in self.targets {
            MqttSessions.statMessage(topic: "$SYS/compliance/target/\(target)", payload: "\(count)")
            if MqttCompliance.clientFeatures.contains(target) {
                checkedClientFeatures.insert(target)
            }
            if MqttCompliance.clientEvents.contains(target) {
                checkedClientEvents.insert(target)
            }
            if MqttCompliance.serverFeatures.contains(target) {
                checkedServerFeatures.insert(target)
            }
            if MqttCompliance.serverEvents.contains(target) {
                checkedServerEvents.insert(target)
            }
        }

        let uncheckedClientFeatures = MqttCompliance.clientFeatures.subtracting(checkedClientFeatures)

        MqttSessions.statMessage(topic: "$SYS/compliance/clientFeatures/checked", payload: "\(checkedClientFeatures)")
        MqttSessions.statMessage(topic: "$SYS/compliance/clientFeatures/unchecked", payload: "\(uncheckedClientFeatures)")
        MqttSessions.statMessage(topic: "$SYS/compliance/clientFeatures/coverage", payload: "\(checkedClientFeatures.count / MqttCompliance.clientFeatures.count)")

        let uncheckedClientEvents = MqttCompliance.clientEvents.subtracting(checkedClientEvents)
        MqttSessions.statMessage(topic: "$SYS/compliance/clientEvents/checked", payload: "\(checkedClientEvents)")
        MqttSessions.statMessage(topic: "$SYS/compliance/clientEvents/unchecked", payload: "\(uncheckedClientEvents)")
        MqttSessions.statMessage(topic: "$SYS/compliance/clientEvents/coverage", payload: "\(checkedClientEvents.count / MqttCompliance.clientEvents.count)")
        
        let uncheckedServerFeatures = MqttCompliance.serverFeatures.subtracting(checkedServerFeatures)
        MqttSessions.statMessage(topic: "$SYS/compliance/serverFeatures/checked", payload: "\(checkedServerFeatures)")
        MqttSessions.statMessage(topic: "$SYS/compliance/serverFeatures/unchecked", payload: "\(uncheckedServerFeatures)")
        MqttSessions.statMessage(topic: "$SYS/compliance/serverFeatures/coverage", payload: "\(checkedServerFeatures.count / MqttCompliance.serverFeatures.count)")
        
        let uncheckedServerEvents = MqttCompliance.clientFeatures.subtracting(checkedServerEvents)
        MqttSessions.statMessage(topic: "$SYS/compliance/serverEvents/checked", payload: "\(checkedServerEvents)")
        MqttSessions.statMessage(topic: "$SYS/compliance/serverEvents/unchecked", payload: "\(uncheckedServerEvents)")
        MqttSessions.statMessage(topic: "$SYS/compliance/serverEvents/coverage", payload: "\(checkedServerEvents.count / MqttCompliance.serverEvents.count)")
        
    }
}
