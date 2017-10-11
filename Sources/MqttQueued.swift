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
    private var stateTimer: DispatchSourceTimer

    init() {
        self.queued = [MqttMessage]()

        let q = DispatchQueue.global()
        self.stateTimer = DispatchSource.makeTimerSource(queue: q)
        self.stateTimer.scheduleRepeating(deadline: .now(), interval: .seconds(1), leeway: .seconds(1))
        self.stateTimer.setEventHandler { [weak self] in
            self!.expired()
        }
        self.stateTimer.resume()
    }

    func expired() {
        let now = Date()
        for (index, message) in queued.enumerated() {
            if message.publicationExpiryTime != nil {
                if message.publicationExpiryTime! < now {
                    MqttCompliance.sharedInstance().log(target: "queued messager expired")
                    queued.remove(at:index)
                }
            }
        }
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
