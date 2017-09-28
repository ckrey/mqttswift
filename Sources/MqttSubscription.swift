//
//  MqttSubscription.swift
//  mqttswift
//
//  Created by Christoph Krey on 20.09.17.
//

import Foundation

class MqttSubscription {
    var topicFilter: String!
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
}
