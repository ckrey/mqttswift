//
//  MqttMessage.swift
//  mqttswift
//
//  Created by Christoph Krey on 18.09.17.
//
//

import Foundation

class MqttMessage {
    var topic: String!
    var data: Data = Data()
    var qos: MqttQoS = MqttQoS.AtMostOnce
    var retain: Bool = false
    var payloadFormatIndicator: UInt8? = nil
    var publicationExpiryInterval : Int? = nil
    var responseTopic: String? = nil
    var correlationData: Data? = nil
    var userProperties: [[String: String]]? = nil
    var contentType: String? = nil
    var topicAlias:Int? = nil
    var subscriptionIdentifiers: [Int]? = nil
    var packetIdentifier: Int? = nil
    var pubrel: Bool = false

    init () {
    }

    init(topic: String!,
         data: Data!,
         qos: MqttQoS!,
         retain: Bool!) {
        self.topic = topic
        self.data = data
        self.qos = qos
        self.retain = retain
    }
}
