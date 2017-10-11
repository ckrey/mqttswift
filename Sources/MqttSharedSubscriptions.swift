//
//  MqttSharedSubscriptions.swift
//  mqttswift
//
//  Created by Christoph Krey on 08.10.17.
//

import Foundation

class MqttSharedSubscriptions {
    private static var theInstance: MqttSharedSubscriptions?

    var shares: [String: [String]]

    public class func sharedInstance() -> MqttSharedSubscriptions {
        if theInstance == nil {
            theInstance = MqttSharedSubscriptions()
        }
        return theInstance!
    }

    private init() {
        self.shares = [String: [String]]()
    }

    func remove(subscription: MqttSubscription) {
        let shareName = subscription.shareName()
        if shareName != nil {
            var share = self.shares[shareName!]
            if share != nil {
                for (index, topicFilter) in share!.enumerated() {
                    if topicFilter == subscription.topicFilter {
                        share!.remove(at:index)
                        self.shares[shareName!] = share
                        return
                    }
                }
            }
        }
    }

    func store(subscription: MqttSubscription) {
        let shareName = subscription.shareName()
        if shareName != nil {
            var share = self.shares[shareName!]
            if share == nil {
                share = [String]()
            }
            for topicFilter in share! {
                if topicFilter == subscription.topicFilter {
                    return
                }
            }
            share!.append(subscription.topicFilter)
            self.shares[shareName!] = share
        }
    }

    func position(subscription: MqttSubscription) -> Int {
        let shareName = subscription.shareName()
        if shareName != nil {
            let share = self.shares[shareName!]
            if share != nil {
                for (index, topicFilter) in share!.enumerated() {
                    if topicFilter == subscription.topicFilter {
                        return index + 1
                    }
                }
            }
        }
        return 1
    }

}

