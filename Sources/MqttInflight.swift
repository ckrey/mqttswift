//
//  MqttInflight.swift
//  mqttswift
//
//  Created by Christoph Krey on 17.09.17.
//
//

import Foundation

class MqttInflight {
    private var inflight : [MqttMessage]

    init() {
        self.inflight = [MqttMessage]()
    }

    func store(message: MqttMessage!) {
        self.inflight.append(message)
    }

    func find(pid: Int!) -> (MqttMessage?) {
        for message in self.inflight {
            if message.packetIdentifier == pid {
                return message
            }
        }
        return nil
    }

    func update(message: MqttMessage!) {
        for (index, aMessage) in self.inflight.enumerated() {
            if aMessage.packetIdentifier == message.packetIdentifier {
                self.inflight[index] = message
                return
            }
        }
    }

    func remove(pid: Int!) {
        for (index, message) in self.inflight.enumerated() {
            if message.packetIdentifier == pid {
                self.inflight.remove(at:index)
                return
            }
        }
    }

}
