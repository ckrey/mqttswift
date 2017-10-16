import Foundation

class MqttControlPacket {
    var array: [UInt8]
    let max: Int?
    var processed : Int
    
    init(max: Int?) {
        self.max = max
        self.processed = 0
        self.array = [UInt8]()
    }
    
    func process(byte: UInt8) -> (Bool, MqttReturnCode?) {
        self.processed = self.processed + 1
        if self.max == nil || self.processed <= self.max! {
            self.array.append(byte)
        }
        
        if self.complete() {
            //print ("MqttControlPacket complete", terminator: ": ")
            var end = 31
            if self.array.count - 1 < end {
                end = self.array.count - 1
            }
            for i in 0...end {
                print (String(format:"%02x", self.array[i]), terminator: " ")
            }
            if (self.array.count - 1 > end) {
                print ("...")
            } else {
                print ()
            }
            
            //            let cpt = self.mqttControlPacketType()
            //            let dup = self.mqttControlPacketDup()
            //            let qos = self.mqttControlPacketQoS()
            //            let retain = self.mqttControlPacketRetain()
            //            let pid = self.mqttPacketIdentifier()
            //            print(String(format:"MqttControlPacket: %02x d%d q%d r%d p%d",
            //                         cpt!.rawValue,
            //                         dup! ? 1 : 0,
            //                         qos!.rawValue,
            //                         retain! ? 1 : 0,
            //                         pid != nil ? pid! : -1))
            return (true, MqttReturnCode.Success)
        } else {
            let cpt = self.mqttControlPacketType()
            let rl = self.mqttRemainingLength()
            let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
            
            if cpt != nil &&
                rl != nil &&
                rll != nil &&
                self.processed == 1 + rll! + rl! {
                return (true, MqttReturnCode.PacketTooLarge)
            } else {
                return (false, MqttReturnCode.Success)
            }
        }
    }
    
    func complete() -> Bool {
        let cpt = self.mqttControlPacketType()
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        
        if cpt != nil &&
            rl != nil &&
            rll != nil &&
            self.array.count == 1 + rll! + rl! {
            return true
        } else {
            return false
        }
    }
    
    func mqttRemainingLength() -> Int? {
        return self.mqttVariableByteInteger(start: 1)
    }
    
    func mqttVariableByteInteger(start: Int) -> Int? {
        var multiplier : Int = 1
        var pos = start
        var value : Int = 0
        var encodedByte: UInt8
        repeat {
            if pos >= self.array.count {
                return nil
            }
            encodedByte = self.array[pos]
            pos = pos + 1
            value = value + Int(encodedByte & 127) * multiplier
            if (multiplier > 128*128*128) {
                return nil
            }
            multiplier *= 128
        } while ((encodedByte & 128) != 0)
        return value
    }
    
    func mqttFourByteInteger(start: Int) -> Int? {
        var value : Int?
        if start + 3 < self.array.count {
            value = Int(self.array[start]) * 256 * 256 * 256 +
                Int(self.array[start + 1]) * 256 * 256 +
                Int(self.array[start + 2]) * 256 +
                Int(self.array[start + 3])
        }
        return value
    }
    
    func mqttTwoByteInteger(start: Int) -> Int? {
        var value : Int?
        if start + 1 < self.array.count {
            value = Int(self.array[start]) * 256 +
                Int(self.array[start + 1])
        }
        return value
    }
    
    func mqttTopicLength() -> Int? {
        let cpt = self.mqttControlPacketType()
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        
        if self.complete() &&
            cpt == MqttControlPacketType.PUBLISH &&
            rl != nil && rl! >= 2 &&
            rll != nil {
            return self.mqttTwoByteInteger(start: 1 + rll!)
        } else {
            return nil
        }
    }
    
    func mqttTopic() -> String? {
        let cpt = self.mqttControlPacketType()
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        
        if self.complete() &&
            cpt == MqttControlPacketType.PUBLISH &&
            rl != nil && rl! >= 2 &&
            rll != nil {
            return self.mqttUtf8String(start: 1 + rll!)
        } else {
            return nil
        }
    }
    
    func mqttUtf8String(start: Int) -> String? {
        let td = self.mqttBinaryData(start: start)
        if td == nil {
            return nil
        } else {
            let t = String(data: td!, encoding: String.Encoding.utf8)
            return t
        }
    }
    
    func mqttBinaryData(start: Int) -> Data? {
        let l = self.mqttTwoByteInteger(start: start)
        if l == nil {
            return nil
        }
        if start + 2 + l! > self.array.count {
            return nil
        }
        var td = Data()
        let sStart = start + 2
        if l! > 0 {
            for i in sStart ... sStart + l! - 1 {
                td.append(&self.array[i], count: 1)
            }
        }
        return td
    }
    
    func mqttPropertyLength() -> (Int?, Int?) {
        let cpt = self.mqttControlPacketType()
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        
        if self.complete() {
            if cpt! == MqttControlPacketType.CONNECT {
                return (self.mqttVariableByteInteger(start: 1 + rll! + 10), 1 + rll! + 10)
            } else if cpt! == MqttControlPacketType.PUBLISH {
                let tl = self.mqttTopicLength()
                let pid = self.mqttPacketIdentifier()
                if tl != nil {
                    if pid != nil {
                        return (self.mqttVariableByteInteger(start: 1 + rll! + 2 + tl! + 2), 1 + rll! + 2 + tl! + 2)
                    } else {
                        return (self.mqttVariableByteInteger(start: 1 + rll! + 2 + tl!), 1 + rll! + 2 + tl!)
                    }
                } else {
                    return (nil, nil)
                }
            } else if cpt! == MqttControlPacketType.SUBSCRIBE {
                return (self.mqttVariableByteInteger(start: 1 + rll! + 2), 1 + rll! + 2)
            } else if cpt! == MqttControlPacketType.UNSUBSCRIBE {
                return (self.mqttVariableByteInteger(start: 1 + rll! + 2), 1 + rll! + 2)
            } else if cpt! == MqttControlPacketType.DISCONNECT {
                return (self.mqttVariableByteInteger(start: 1 + rll! + 1), 1 + rll! + 1)
            } else {
                return (nil, nil)
            }
        } else {
            return (nil, nil)
        }
    }
    
    func mqttReturnCode() -> MqttReturnCode? {
        let cpt = self.mqttControlPacketType()
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        
        if self.complete() {
            if cpt! == MqttControlPacketType.DISCONNECT {
                var returnCode : MqttReturnCode = .Success
                if self.array.count > 1 + rll! {
                    returnCode = MqttReturnCode(rawValue:self.array[1 + rll!])!
                }
                return returnCode
            } else {
                return nil;
            }
        }
        return nil
    }
    
    func mqttPayload() -> Data? {
        let cpt = self.mqttControlPacketType()
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        
        if self.complete() {
            if cpt! == MqttControlPacketType.CONNECT {
                let (pl, _) = self.mqttPropertyLength()
                let pll = MqttControlPacket.mqttVariableByteIntegerLength(variable: pl)
                if pll != nil {
                    var p = Data()
                    let end = rl! + 1 + rll! - 1
                    var start: Int
                    start = 1 + rll! + 10 + pll!
                    //print ("Payload bounds \(start) \(end)")
                    if start <= end {
                        for i in start ... end {
                            p.append(&self.array[i], count: 1)
                        }
                    }
                    return p
                } else {
                    return nil
                }
            } else if cpt! == MqttControlPacketType.PUBLISH {
                let tl = self.mqttTopicLength()
                let pid = self.mqttPacketIdentifier()
                let (pl, _) = self.mqttPropertyLength()
                let pll = MqttControlPacket.mqttVariableByteIntegerLength(variable: pl)
                if tl != nil &&
                    pl != nil &&
                    pll != nil {
                    var p = Data()
                    let end = rl! + 1 + rll! - 1
                    var start = 1 + rll! + 2 + tl! + pll! + pl!
                    if pid != nil {
                        start += 2
                    }
                    //print ("Payload bounds \(start) \(end)")
                    if start <= end {
                        for i in start ... end {
                            p.append(&self.array[i], count: 1)
                        }
                    }
                    return p
                } else {
                    return nil
                }
            } else if cpt! == MqttControlPacketType.SUBSCRIBE {
                let (pl, _) = self.mqttPropertyLength()
                let pll = MqttControlPacket.mqttVariableByteIntegerLength(variable: pl)
                if pll != nil {
                    var p = Data()
                    let end = rl! + 1 + rll! - 1
                    var start: Int
                    start = 1 + rll! + 2 + pll! + pl!
                    //print ("Payload bounds \(start) \(end)")
                    if start <= end {
                        for i in start ... end {
                            p.append(&self.array[i], count: 1)
                        }
                    }
                    return p
                } else {
                    return nil
                }
            } else if cpt! == MqttControlPacketType.UNSUBSCRIBE {
                let (pl, _) = self.mqttPropertyLength()
                let pll = MqttControlPacket.mqttVariableByteIntegerLength(variable: pl)
                if pll != nil {
                    var p = Data()
                    let end = rl! + 1 + rll! - 1
                    var start: Int
                    start = 1 + rll! + 2 + pll! + pl!
                    //print ("Payload bounds \(start) \(end)")
                    if start <= end {
                        for i in start ... end {
                            p.append(&self.array[i], count: 1)
                        }
                    }
                    return p
                } else {
                    return nil
                }
            } else {
                return nil
            }
        } else {
            return nil
        }
    }
    
    func mqttClientId() -> String? {
        let cpt = self.mqttControlPacketType()
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        
        if self.complete() {
            if cpt! == MqttControlPacketType.CONNECT {
                let (pl, _) = self.mqttPropertyLength()
                let pll = MqttControlPacket.mqttVariableByteIntegerLength(variable: pl)
                return mqttUtf8String(start: 1 + rll! + 10 + pll!)
            } else {
                return nil
            }
        } else {
            return nil
        }
    }
    
    func mqttConnectStringField(position: Int) -> String? {
        let d = self.mqttConnectDataField(position: position)
        if d != nil {
            let s = String(data: d!, encoding: String.Encoding.utf8)
            return s
        } else {
            return nil
        }
    }
    
    func mqttConnectDataField(position: Int) -> Data? {
        let cpt = self.mqttControlPacketType()
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        
        if self.complete() {
            if cpt! == MqttControlPacketType.CONNECT {
                let (pl, _) = self.mqttPropertyLength()
                let pll = MqttControlPacket.mqttVariableByteIntegerLength(variable: pl)
                var offset = 0
                var skip = position
                while skip > 0 {
                    skip = skip - 1
                    let l = self.mqttTwoByteInteger(start: 1 + rll! + 10 + pll! + pl! + offset)
                    if l == nil {
                        return nil
                    } else {
                        offset = offset + 2 + l!
                    }
                }
                return mqttBinaryData(start: 1 + rll! + 10 + pll! + pl! + offset)
            } else {
                return nil
            }
        } else {
            return nil
        }
    }
    
    func mqttSubscriptions() -> [MqttSubscription] {
        var stf = [MqttSubscription]()
        
        var payload = self.mqttPayload()
        if payload != nil {
            var position = 0
            while position + 1 < payload!.count {
                let stringLength = Int(payload![position]) * 256 + Int(payload![position + 1])
                if position + 1 + stringLength + 1 <= payload!.count {
                    var stringData = Data()
                    let stringStart = position + 2
                    if stringLength > 0 {
                        for i in stringStart ... stringStart + stringLength - 1 {
                            stringData.append(&(payload![i]), count: 1)
                        }
                    }
                    let subscription = MqttSubscription()
                    subscription.topicFilter = String(data: stringData, encoding: String.Encoding.utf8)
                    subscription.subscribeOptions = payload![position + 2 + stringLength]
                    subscription.qos = MqttQoS(rawValue: subscription.subscribeOptions & 0x03)!
                    subscription.noLocal = (subscription.subscribeOptions & 0x04 != 0)
                    subscription.retainAsPublish = (subscription.subscribeOptions & 0x08 != 0)
                    subscription.retainHandling = MqttRetainHandling(rawValue: (subscription.subscribeOptions & 0x30) >> 4)!
                    
                    if self.mqttProperties() != nil && self.mqttProperties()!.subscriptionIdentifiers != nil {
                        subscription.subscriptionIdentifier = self.mqttProperties()!.subscriptionIdentifiers![0]
                    }
                    stf.append(subscription)
                    position = position + 2 + stringLength + 1
                } else {
                    break;
                }
            }
        }
        return stf
    }
    
    func mqttUnsubscribeTopicFilters() -> [String] {
        var utf = [String]()
        var payload = self.mqttPayload()
        if payload != nil {
            var position = 0
            while position + 1 < payload!.count {
                let stringLength = Int(payload![position]) * 256 + Int(payload![position + 1])
                if position + 1 + stringLength <= payload!.count {
                    var stringData = Data()
                    let stringStart = position + 2
                    if stringLength > 0 {
                        for i in stringStart ... stringStart + stringLength - 1 {
                            stringData.append(&(payload![i]), count: 1)
                        }
                    }
                    let string = String(data: stringData, encoding: String.Encoding.utf8)
                    if string != nil {
                        utf.append(string!)
                    }
                    position = position + 2 + stringLength
                } else {
                    break;
                }
            }
        }
        return utf
    }
    
    func mqttProperties() -> MqttProperties? {
        let (pl, ppos) = self.mqttPropertyLength()
        let pll = MqttControlPacket.mqttVariableByteIntegerLength(variable: pl)
        let mqttProperties = MqttProperties()
        
        if pl != nil && pll != nil {
            var remaining = pl! - pll!
            while remaining > 0 {
                let propertyType = self.array[ppos! + pl! - remaining]
                switch propertyType {
                case MqttPropertyIdentifier.PayloadFormatIndicator.rawValue:
                    mqttProperties.payloadFormatIndicator = self.array[ppos! + pl! - remaining + 1]
                    remaining = remaining - 1
                    break
                    
                case MqttPropertyIdentifier.PublicationExpiryInterval.rawValue:
                    mqttProperties.publicationExpiryInterval = self.mqttFourByteInteger(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 4
                    break
                    
                case MqttPropertyIdentifier.ContentType.rawValue:
                    let utf8Length = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    mqttProperties.contentType = self.mqttUtf8String(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2 - utf8Length!
                    break
                    
                case MqttPropertyIdentifier.ResponseTopic.rawValue:
                    let utf8Length = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    mqttProperties.responseTopic = self.mqttUtf8String(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2 - utf8Length!
                    break
                    
                case MqttPropertyIdentifier.CorrelationData.rawValue:
                    let binaryLength = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    mqttProperties.correlationData = self.mqttBinaryData(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2 - binaryLength!
                    break
                    
                case MqttPropertyIdentifier.SubscriptionIdentifier.rawValue:
                    let subscriptionIdentifier = self.mqttVariableByteInteger(start: ppos! + pl! - remaining + 1)
                    if mqttProperties.subscriptionIdentifiers == nil {
                        mqttProperties.subscriptionIdentifiers = [Int]()
                    }
                    mqttProperties.subscriptionIdentifiers!.append(subscriptionIdentifier!)
                    
                    remaining = remaining - MqttControlPacket.mqttVariableByteIntegerLength(variable: subscriptionIdentifier)!
                    break
                    
                case MqttPropertyIdentifier.SessionExpiryInterval.rawValue:
                    mqttProperties.sessionExpiryInterval = self.mqttFourByteInteger(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 4
                    break
                    
                case MqttPropertyIdentifier.AssignedClientIdentifier.rawValue:
                    let utf8Length = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    mqttProperties.assignedClientIdentifier = self.mqttUtf8String(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2 - utf8Length!
                    break
                    
                case MqttPropertyIdentifier.ServerKeepAlive.rawValue:
                    mqttProperties.serverKeepAlive = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2
                    break
                    
                case MqttPropertyIdentifier.AuthMethod.rawValue:
                    let utf8Length = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    mqttProperties.authMethod = self.mqttUtf8String(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2 - utf8Length!
                    break
                    
                case MqttPropertyIdentifier.AuthData.rawValue:
                    let binaryLength = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    mqttProperties.authData = self.mqttBinaryData(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2 - binaryLength!
                    break;
                    
                case MqttPropertyIdentifier.RequestProblemInformation.rawValue:
                    mqttProperties.requestProblemInformation = self.array[ppos! + pl! - remaining + 1]
                    remaining = remaining - 1
                    break
                    
                case MqttPropertyIdentifier.WillDelayInterval.rawValue:
                    mqttProperties.willDelayInterval = self.mqttFourByteInteger(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 4
                    break
                    
                case MqttPropertyIdentifier.RequestResponseInformation.rawValue:
                    mqttProperties.requestResponseInformation = self.array[ppos! + pl! - remaining + 1]
                    remaining = remaining - 1
                    break
                    
                case MqttPropertyIdentifier.ResponseInformation.rawValue:
                    let utf8Length = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    mqttProperties.responseInformation = self.mqttUtf8String(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2 - utf8Length!
                    break
                    
                case MqttPropertyIdentifier.ServerReference.rawValue:
                    let utf8Length = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    mqttProperties.serverReference = self.mqttUtf8String(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2 - utf8Length!
                    break
                    
                case MqttPropertyIdentifier.ReasonString.rawValue:
                    let utf8Length = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    mqttProperties.reasonString = self.mqttUtf8String(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2 - utf8Length!
                    break
                    
                case MqttPropertyIdentifier.ReceiveMaximum.rawValue:
                    mqttProperties.receiveMaximum = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2
                    break
                    
                case MqttPropertyIdentifier.TopicAliasMaximum.rawValue:
                    mqttProperties.topicAliasMaximum = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2
                    break
                    
                case MqttPropertyIdentifier.TopicAlias.rawValue:
                    mqttProperties.topicAlias = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 2
                    break
                    
                case MqttPropertyIdentifier.MaximumQoS.rawValue:
                    mqttProperties.maximumQoS = MqttQoS(rawValue:self.array[ppos! + pl! - remaining + 1])
                    remaining = remaining - 1
                    break
                    
                case MqttPropertyIdentifier.RetainAvailable.rawValue:
                    mqttProperties.retainAvailable = self.array[ppos! + pl! - remaining + 1]
                    remaining = remaining - 1
                    break
                    
                case MqttPropertyIdentifier.UserProperty.rawValue:
                    let keyUtf8Length = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1)
                    let key = self.mqttUtf8String(start: ppos! + pl! - remaining + 1)
                    let valueUtf8Length = self.mqttTwoByteInteger(start: ppos! + pl! - remaining + 1 + 2 + keyUtf8Length!)
                    let value = self.mqttUtf8String(start: ppos! + pl! - remaining + 1 + 2 + keyUtf8Length!)
                    if mqttProperties.userProperties == nil {
                        mqttProperties.userProperties = [[String: String]]()
                    }
                    mqttProperties.userProperties!.append([key!: value!])
                    remaining = remaining - 2 - keyUtf8Length! - 2 - valueUtf8Length!
                    break
                    
                case MqttPropertyIdentifier.MaximumPacketSize.rawValue:
                    mqttProperties.maximumPacketSize = self.mqttFourByteInteger(start: ppos! + pl! - remaining + 1)
                    remaining = remaining - 4
                    break
                    
                case MqttPropertyIdentifier.WildcardSubscriptionAvailable.rawValue:
                    mqttProperties.wildcardSubscriptionAvailable = self.array[ppos! + pl! - remaining + 1]
                    remaining = remaining - 1
                    break
                    
                case MqttPropertyIdentifier.SubscriptionIdentifiersAvailable.rawValue:
                    mqttProperties.subscriptionIdentifiersAvailable = self.array[ppos! + pl! - remaining + 1]
                    remaining = remaining - 1
                    break
                    
                case MqttPropertyIdentifier.SharedSubscriptionAvailable.rawValue:
                    mqttProperties.sharedSubscriptionAvailable = self.array[ppos! + pl! - remaining + 1]
                    remaining = remaining - 1
                    break
                    
                default:
                    return nil
                }
                remaining = remaining - 1
            }
        }
        return mqttProperties
    }
    
    func mqttPacketIdentifier() -> Int? {
        let tl = self.mqttTopicLength()
        let qos = self.mqttControlPacketQoS()
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        let cpt = self.mqttControlPacketType()
        
        if self.complete() {
            if cpt != nil {
                if cpt == MqttControlPacketType.PUBLISH {
                    if tl != nil &&
                        (qos == MqttQoS.AtLeastOnce || qos == MqttQoS.ExactlyOnce) &&
                        rl != nil && rl! >= 2 + tl! + 2 &&
                        rll != nil {
                        return self.mqttTwoByteInteger(start: 1 + rll! + 2 + tl!)
                    } else {
                        return nil
                    }
                } else if (cpt == MqttControlPacketType.PUBACK ||
                    cpt == MqttControlPacketType.PUBREC ||
                    cpt == MqttControlPacketType.PUBREL ||
                    cpt == MqttControlPacketType.PUBCOMP ||
                    cpt == MqttControlPacketType.SUBSCRIBE ||
                    cpt == MqttControlPacketType.UNSUBSCRIBE) &&
                    rl != nil &&
                    rl! >= 2 {
                    return self.mqttTwoByteInteger(start: 1 + rll!)
                } else {
                    return nil
                }
            } else {
                return nil
            }
        } else {
            return nil
        }
    }
    
    static func mqttVariableByteIntegerLength(variable: Int?) -> Int? {
        var l : Int?
        if variable != nil {
            if variable! <= 127 {
                l = 1
            } else if variable! <= 16383 {
                l = 2
            } else if variable! <= 2097151{
                l = 3
            } else if variable! <= 268435455 {
                l = 4
            }
        }
        return l
    }
    
    static func mqttVariableByte(variable: Int) -> Data {
        var d = Data()
        var v = variable
        var u: UInt8
        repeat {
            u = UInt8(v % 128)
            v = v / 128
            if v > 0 {
                u = u | 128
            }
            d.append(u)
        } while v > 0
        return d
    }
    
    static func mqttUtf8(variable: String) -> Data {
        var d = Data()
        let sd = variable.data(using: String.Encoding.utf8)
        d.append(MqttControlPacket.mqttTwoByte(variable: sd!.count))
        d.append(sd!)
        return d
    }
    
    static func mqttFourByte(variable: Int) -> Data {
        var d = Data()
        var u: UInt8
        u = UInt8((variable / 256 * 256 * 256) % 256)
        d.append(u)
        u = UInt8((variable / 256 * 256) % 256)
        d.append(u)
        u = UInt8((variable / 256) % 256)
        d.append(u)
        u = UInt8(variable % 256)
        d.append(u)
        return d
    }
    
    static func mqttTwoByte(variable: Int) -> Data {
        var d = Data()
        var u: UInt8
        u = UInt8((variable / 256) % 256)
        d.append(u)
        u = UInt8(variable % 256)
        d.append(u)
        return d
    }
    
    func mqttProtocolName() -> String? {
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        if rl != nil &&
            rll != nil &&
            self.mqttControlPacketType() == MqttControlPacketType.CONNECT {
            return self.mqttUtf8String(start: 1 + rll!)
        } else {
            return nil
        }
    }
    
    func mqttProtocolVersion() -> UInt8? {
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        if rl != nil &&
            rll != nil &&
            self.mqttControlPacketType() == MqttControlPacketType.CONNECT {
            let l = self.mqttTwoByteInteger(start: 1 + rll!)
            if l == nil {
                return nil
            }
            return self.array[1 + rll! + 2 + l!]
        } else {
            return nil
        }
    }
    
    func mqttConnectKeepAlive() -> Int? {
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        if rl != nil &&
            rll != nil &&
            self.mqttControlPacketType() == MqttControlPacketType.CONNECT {
            let l = self.mqttTwoByteInteger(start: 1 + rll!)
            if l == nil {
                return nil
            }
            return self.mqttTwoByteInteger(start: 1 + rll! + 2 + l! + 2)
        } else {
            return nil
        }
    }
    
    func mqttConnectFlags() -> UInt8? {
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        if rl != nil &&
            rll != nil &&
            self.mqttControlPacketType() == MqttControlPacketType.CONNECT {
            let l = self.mqttTwoByteInteger(start: 1 + rll!)
            if l == nil {
                return nil
            }
            return self.array[1 + rll! + 2 + l! + 1]
        } else {
            return nil
        }
    }
    
    func mqttKeepAlive() -> Int? {
        let rl = self.mqttRemainingLength()
        let rll = MqttControlPacket.mqttVariableByteIntegerLength(variable: rl)
        if rl != nil &&
            rll != nil &&
            self.mqttControlPacketType() == MqttControlPacketType.CONNECT {
            let l = self.mqttTwoByteInteger(start: 1 + rll!)
            if l == nil {
                return nil
            }
            return self.mqttTwoByteInteger(start: 1 + rll! + 2 + l! + 1)
        } else {
            return nil
        }
    }
    
    func mqttControlPacketType() -> MqttControlPacketType? {
        if self.array.count > 0 {
            let cpt = self.array[0] >> 4
            return MqttControlPacketType(rawValue: cpt)
        } else {
            return nil
        }
    }
    
    func mqttControlPacketDup() -> Bool? {
        if self.array.count > 0 {
            let dup = (self.array[0] >> 3) & 1
            return (dup == 1)
        } else {
            return nil
        }
    }
    
    func mqttControlPacketQoS() -> MqttQoS? {
        if self.array.count > 0 {
            let qos = ((self.array[0] >> 1) & 3)
            if (qos < 3 ) {
                return MqttQoS(rawValue: qos)
            } else {
                return nil
            }
        } else {
            return nil
        }
    }
    
    func mqttControlPacketRetain() -> Bool? {
        if self.array.count > 0 {
            return ((self.array[0] & 1) == 1)
        } else {
            return nil
        }
    }
}

