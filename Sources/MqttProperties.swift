import Foundation

class MqttProperties {
    var payloadFormatIndicator: UInt8 = 0x00
    var publicationExpiryInterval: Int? = nil
    var contentType: String? = nil
    var responseTopic: String? = nil
    var correlationData: Data? = nil
    var subscriptionIdentifier: Int? = nil
    var sessionExpiryInterval: Int? = nil
    var assignedClientIdentifier: String? = nil
    var serverKeepAlive: Int? = nil
    var authMethod: String? = nil
    var authData: Data? = nil
    var requestProblemInformation: UInt8? = nil
    var willDelayInterval: Int? = nil
    var requestResponseInformation: UInt8? = nil
    var responseInformation: String? = nil
    var serverReference: String? = nil
    var reasonString: String? = nil
    var receiveMaximum: Int? = nil
    var topicAliasMaximum: Int? = nil
    var topicAlias: Int? = nil
    var maximumQoS: MqttQoS? = nil
    var retainAvailable: UInt8? = nil
    var userProperty: [String: String]? = nil
    var maximumPacketSize: Int? = nil
    var wildcardSubscriptionAvailable: UInt8? = nil
    var subscriptionIdentifiersAvailable: UInt8? = nil
    var sharedSubscriptionAvailable: UInt8? = nil
}
