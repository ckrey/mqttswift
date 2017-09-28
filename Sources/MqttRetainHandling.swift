//
//  MqttRetainHandling.swift
//  mqttswift
//
//  Created by Christoph Krey on 05.09.17.
//
//

import Foundation

enum MqttRetainHandling: UInt8 {
    case SendRetained = 0
    case SendRetainedIfNotYetSubscribed = 1
    case DontSentRetained = 2
    case Reserved = 3
}
