//
//  MqttSubscription.swift
//  mqttswift
//
//  Created by Christoph Krey on 20.09.17.
//

import Foundation

class MqttSubscription {
    var topicFilter: String!
    var subscribeOptions: UInt8 = 0
    var qos: MqttQoS = MqttQoS.AtMostOnce
    var noLocal: Bool = false
    var retainHandling: MqttRetainHandling = MqttRetainHandling.SendRetained
    var retainAsPublish: Bool = false
    var subscriptionIdentifier: Int? = nil

    init () {
    }

    init(topicFilter: String!,
         qos: MqttQoS!,
         noLocal: Bool!,
         retainHandling: MqttRetainHandling!,
         retainAsPublish: Bool!,
         subscriptionIdentifier: Int?
        ) {
        self.topicFilter = topicFilter
        self.qos = qos
        self.noLocal = noLocal
        self.retainHandling = retainHandling
        self.retainAsPublish = retainAsPublish
        self.subscriptionIdentifier = subscriptionIdentifier
    }

    func isShared() -> Bool {
        var topicFilterComponents = self.topicFilter.components(separatedBy: "/");
        return topicFilterComponents.count >= 3 && topicFilterComponents[0] == "$share"
    }

    func shareName() -> String? {
        var topicFilterComponents = self.topicFilter.components(separatedBy: "/");

        if topicFilterComponents.count >= 3 && topicFilterComponents[0] == "$share" {
            return topicFilterComponents[1]
        }
        return nil
    }

    func netTopicFilter() -> String {
        var topicFilterComponents = self.topicFilter.components(separatedBy: "/");

        if topicFilterComponents.count >= 3 && topicFilterComponents[0] == "$share" {
            topicFilterComponents.removeFirst()
            topicFilterComponents.removeFirst()
        }
        return topicFilterComponents.joined(separator:"/")
    }
}
