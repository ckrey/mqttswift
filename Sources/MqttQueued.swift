//
//  MqttQueued.swift
//  mqttswift
//
//  Created by Christoph Krey on 18.09.17.
//
//

import Foundation

class MqttQueued {
    private var queued : [MqttMessage]

    init() {
        self.queued = [MqttMessage]()
    }

    func append(message: MqttMessage!) {
        self.queued.append(message)
    }

    func count() -> Int! {
        return self.queued.count
    }

    func get() -> MqttMessage? {
        if self.queued.count > 0 {
            return self.queued.remove(at:0)
        } else {
            return nil
        }
    }
}
