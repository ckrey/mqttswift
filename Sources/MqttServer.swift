import Foundation
import Socket
import Dispatch

class MqttServer {

    static let bufferSize = 4096

    let port: Int
    let serverKeepAlive: Int?
    let verbose: Bool
    let retainAvailable: Bool
    let maximumQoS: MqttQoS
    let maximumClientIdLength: Int
    let restrictedClientId: Bool
    let receiveMaximum: Int
    let maximumPacketSize: Int
    let topicAliasMaximum: Int
    let wildcardSubscritionAvailable: Bool
    let subscriptionIdentifiersAvailable: Bool
    let sharedSubscriptionAvailable: Bool
    let serverToUse: String?
    let serverMoved: String?
    let authMethods: [String]
    let allowAnonymous: Bool
    let users: [String: String]
    let userProperties: [String: String]
    let accessControl: [String: [String: Bool]]

    let start = Date()

    var listenSocket: Socket? = nil
    var continueRunning = true
    var connectedSockets = [Int32: Socket]()
    let socketLockQueue = DispatchQueue(label: "MqttSocketLockQueue")

    init(verbose: Bool = false,
         port: Int = 1883,
         serverKeepAlive: Int? = nil,
         retainAvailable: Bool = false,
         maximumQoS: MqttQoS = MqttQoS.AtMostOnce,
         maximumClientIdLength: Int = 23,
         restrictedClientId: Bool = true,
         receiveMaximum: Int = 1,
         maximumPacketSize: Int = 2684354565,
         topicAliasMaximum: Int = 0,
         wildcardSubscritionAvailable: Bool = false,
         subscriptionIdentifiersAvailable: Bool = false,
         sharedSubscriptionAvailable: Bool = false,
         serverToUse: String? = nil,
         serverMoved: String? = nil,
         authMethods: [String] = [],
         allowAnonymous: Bool = false,
         users: [String: String] = [:],
         userProperties: [String: String] = [:],
         accessControl: [String: [String: Bool]] = [:]) {
        self.verbose = verbose
        self.port = port
        self.serverKeepAlive = serverKeepAlive
        self.retainAvailable = retainAvailable
        self.maximumQoS = maximumQoS
        self.maximumClientIdLength = maximumClientIdLength
        self.restrictedClientId = restrictedClientId
        self.receiveMaximum = receiveMaximum
        self.maximumPacketSize = maximumPacketSize
        self.topicAliasMaximum = topicAliasMaximum
        self.wildcardSubscritionAvailable = wildcardSubscritionAvailable
        self.subscriptionIdentifiersAvailable = subscriptionIdentifiersAvailable
        self.sharedSubscriptionAvailable = sharedSubscriptionAvailable
        self.serverToUse = serverToUse
        self.serverMoved = serverMoved
        self.authMethods = authMethods
        self.allowAnonymous = allowAnonymous
        self.users = users
        self.userProperties = userProperties
        self.accessControl = accessControl
    }

    deinit {
        print ("deinit");
        for socket in connectedSockets.values {
            socket.close()
        }
        self.listenSocket?.close()
    }

    func run() {
        let queue = DispatchQueue.global(qos: .userInteractive)
        queue.async { [unowned self] in
            do {
                try self.listenSocket = Socket.create(family: .inet6)
                guard let socket = self.listenSocket else {
                    print("Unable to unwrap socket...")
                    return
                }

                try socket.listen(on: self.port)
                print("Listening on port: \(socket.listeningPort)")
                repeat {
                    let newSocket = try socket.acceptClientConnection()
                    print("Accepted connection from: \(newSocket.remoteHostname) on port \(newSocket.remotePort)")
                    print("Socket Signature: \(newSocket.signature?.description)")
                    self.addNewConnection(socket: newSocket)
                } while self.continueRunning
            }
            catch let error {
                guard let socketError = error as? Socket.Error else {
                    print("Unexpected error...")
                    return
                }

                if self.continueRunning {
                    print("Error reported:\n \(socketError.description)")
                }
            }
        }

        MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/version",
                                                                 data: "m5s 0.0.1".data(using:String.Encoding.utf8),
                                                                 qos: .AtMostOnce,
                                                                 retain: true))
        MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/timestamp",
                                                                 data: Date().description.data(using:String.Encoding.utf8),
                                                                 qos: .AtMostOnce,
                                                                 retain: true))
        let q = DispatchQueue.global()

        var sysTimer: DispatchSourceTimer?
        sysTimer = DispatchSource.makeTimerSource(queue: q)
        sysTimer?.scheduleRepeating(deadline: .now(), interval: .seconds(60), leeway: .seconds(1))
        sysTimer?.setEventHandler { [weak self] in
            MqttSessions.stats()
        }
        sysTimer?.resume()

        dispatchMain()
    }

    func addNewConnection(socket: Socket) {
        socketLockQueue.sync { [unowned self, socket] in
            self.connectedSockets[socket.socketfd] = socket
        }

        let queue = DispatchQueue.global(qos: .default)
        queue.async { [unowned self, socket] in
            var shouldKeepRunning = true
            var returnCode : MqttReturnCode?
            var cpt: MqttControlPacketType? = nil
            var connectReceived = false
            var mqttSession: MqttSession?
            var user: String?
            var readData = Data(capacity: MqttServer.bufferSize)
            var cp : MqttControlPacket?
            MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/I/new Client",
                                                                     data: "\(Int(Date.init().timeIntervalSince1970)): \(socket.remoteHostname):\(socket.remotePort)".data(using:String.Encoding.utf8),
                                                                     qos: .AtMostOnce,
                                                                     retain: false))

            do {
                repeat {
                    let sockets = try Socket.wait(for:[socket], timeout: 1000, waitForever: false)
                    if (sockets != nil) {
                        let bytesRead = try socket.read(into: &readData)
                        if bytesRead > 0 {
                            var readBuffer = [UInt8](readData)
                            print("readbuffer \(readBuffer)")
                            readData.count = 0

                            while readBuffer.count > 0 {
                                let byte = readBuffer[0]
                                readBuffer.remove(at: 0)

                                if cp == nil {
                                    cp = MqttControlPacket(max: self.maximumPacketSize)
                                }
                                var complete: Bool
                                var processRC : MqttReturnCode?
                                (complete, processRC) = cp!.process(byte: byte)
                                if complete {

                                    /* Maximum Packet Size */
                                    if processRC != nil && processRC! == .PacketTooLarge {
                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.2.2-16")
                                        returnCode = MqttReturnCode.PacketTooLarge
                                        shouldKeepRunning = false
                                    }

                                    /* Control Packet Type */
                                    if (shouldKeepRunning) {
                                        cpt = cp!.mqttControlPacketType()

                                        if (cpt == nil) {
                                            shouldKeepRunning = false
                                        } else if (cpt == .Reserved) {
                                            shouldKeepRunning = false
                                        }
                                    }

                                    if shouldKeepRunning {
                                        if (cpt == MqttControlPacketType.CONNECT) {
                                            if connectReceived {
                                                MqttCompliance.sharedInstance().log(target: "MQTT-3.1.0-1")
                                                returnCode = MqttReturnCode.ProtocolError
                                                shouldKeepRunning = false
                                            }
                                            connectReceived = true

                                            let remainingLength = cp!.mqttRemainingLength()
                                            if shouldKeepRunning && (remainingLength == nil || remainingLength! < 10) {
                                                MqttCompliance.sharedInstance().log(target: "CONNECT remaining length < 10")
                                                returnCode = MqttReturnCode.MalformedPacket
                                                shouldKeepRunning = false
                                            }

                                            if shouldKeepRunning {
                                                let protocolName = cp!.mqttProtocolName()
                                                if protocolName == nil || protocolName! != "MQTT" {
                                                    MqttCompliance.sharedInstance().log(target: "CONNECT not name MQTT")
                                                    returnCode = MqttReturnCode.MalformedPacket
                                                    shouldKeepRunning = false
                                                }
                                            }

                                            if shouldKeepRunning {
                                                let protocolVersion = cp!.mqttProtocolVersion()
                                                if protocolVersion == nil || protocolVersion! != 5 {
                                                    MqttCompliance.sharedInstance().log(target: "CONNECT not version 5")
                                                    returnCode = MqttReturnCode.UnsupportedProtocolVersion
                                                    shouldKeepRunning = false
                                                }
                                            }

                                            if shouldKeepRunning && self.serverMoved != nil {
                                                MqttCompliance.sharedInstance().log(target: "Server Moved")
                                                returnCode = MqttReturnCode.ServerMoved
                                                shouldKeepRunning = false
                                            }

                                            if shouldKeepRunning && self.serverToUse != nil {
                                                MqttCompliance.sharedInstance().log(target: "Use Another Server")
                                                returnCode = MqttReturnCode.UseAnotherServer
                                                shouldKeepRunning = false
                                            }

                                            var connectFlags : UInt8? = nil
                                            if shouldKeepRunning {
                                                connectFlags = cp!.mqttConnectFlags()
                                            }
                                            var userNameFlag : Bool?
                                            var passwordFlag : Bool?
                                            var willRetain : Bool?
                                            var willQoS : MqttQoS?
                                            var willFlag : Bool = false
                                            var cleanStart : Bool?

                                            if shouldKeepRunning {
                                                if (connectFlags == nil) {
                                                    MqttCompliance.sharedInstance().log(target: "CONNECT no connect flags")
                                                    returnCode = MqttReturnCode.MalformedPacket
                                                    shouldKeepRunning = false
                                                } else {
                                                    userNameFlag = ((connectFlags! >> 7) & 1) == 1
                                                    passwordFlag = ((connectFlags! >> 6) & 1) == 1

                                                    willFlag = ((connectFlags! >> 2) & 1) == 1
                                                    willRetain = ((connectFlags! >> 5) & 1) == 1
                                                    if !willFlag && willRetain! {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-11")
                                                        returnCode = MqttReturnCode.MalformedPacket
                                                        shouldKeepRunning = false
                                                    }
                                                    if shouldKeepRunning && willRetain! && !self.retainAvailable {
                                                        MqttCompliance.sharedInstance().log(target: "RetainNotAvailable")
                                                        returnCode = MqttReturnCode.RetainNotSupported
                                                        shouldKeepRunning = false
                                                    }

                                                    willQoS = MqttQoS(rawValue: ((connectFlags! >> 3) & 3))

                                                    if shouldKeepRunning && willQoS! == MqttQoS.Reserved {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-10")
                                                        returnCode = MqttReturnCode.MalformedPacket
                                                        shouldKeepRunning = false
                                                    }
                                                    if shouldKeepRunning && !willFlag &&
                                                        willQoS! != MqttQoS.AtMostOnce {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-09")
                                                        returnCode = MqttReturnCode.MalformedPacket
                                                        shouldKeepRunning = false
                                                    }
                                                    if shouldKeepRunning && willQoS! == MqttQoS.AtLeastOnce &&
                                                        self.maximumQoS == MqttQoS.AtMostOnce {
                                                        MqttCompliance.sharedInstance().log(target: "QoS1 not supported")
                                                        returnCode = MqttReturnCode.QoSNotSupported
                                                        shouldKeepRunning = false
                                                    }
                                                    if shouldKeepRunning && willQoS! == MqttQoS.ExactlyOnce &&
                                                        self.maximumQoS != MqttQoS.ExactlyOnce {
                                                        MqttCompliance.sharedInstance().log(target: "QoS2 not supported")
                                                        returnCode = MqttReturnCode.QoSNotSupported
                                                        shouldKeepRunning = false
                                                    }

                                                    cleanStart = ((connectFlags! >> 1) & 1) == 1

                                                    if shouldKeepRunning && (connectFlags! & 1) != 0 {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-1")
                                                        returnCode = MqttReturnCode.MalformedPacket
                                                        shouldKeepRunning = false
                                                    }
                                                }
                                            }

                                            var clientId: String?
                                            var willTopic: String?
                                            var willMessage: Data?
                                            var userName: String?
                                            var password: Data?

                                            if (shouldKeepRunning) {
                                                clientId = cp!.mqttConnectStringField(position: 0)
                                                if willFlag {
                                                    willTopic = cp!.mqttConnectStringField(position: 1)
                                                    willMessage = cp!.mqttConnectDataField(position: 2)
                                                    if userNameFlag! {
                                                        userName = cp!.mqttConnectStringField(position: 3)
                                                        if passwordFlag! {
                                                            password = cp!.mqttConnectDataField(position: 4)
                                                        }
                                                    } else {
                                                        if passwordFlag! {
                                                        }
                                                    }
                                                } else {
                                                    if userNameFlag! {
                                                        userName = cp!.mqttConnectStringField(position: 1)
                                                        if passwordFlag! {
                                                            password = cp!.mqttConnectDataField(position: 2)
                                                        }
                                                    } else {
                                                        if passwordFlag! {
                                                            password = cp!.mqttConnectDataField(position: 1)
                                                        }
                                                    }
                                                }
                                            }

                                            /* Client ID */
                                            if (shouldKeepRunning) {
                                                if clientId == nil || clientId!.characters.count == 0 {
                                                    MqttCompliance.sharedInstance().log(target: "MQTT-3.1.3-6")
                                                    clientId = String("m5s\(abs(UUID().hashValue))")
                                                }

                                                if shouldKeepRunning && clientId!.characters.count > self.maximumClientIdLength {
                                                    MqttCompliance.sharedInstance().log(target: "MQTT-3.1.3-8")
                                                    returnCode = MqttReturnCode.ClientIdentifierNotValid
                                                    shouldKeepRunning = false
                                                }

                                                if shouldKeepRunning && self.restrictedClientId {
                                                    let charset = CharacterSet(charactersIn: "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

                                                    if (clientId!.trimmingCharacters(in: charset) != "") {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.1.3-5")
                                                        returnCode = MqttReturnCode.ClientIdentifierNotValid
                                                        shouldKeepRunning = false
                                                    }
                                                }
                                            }

                                            /* User Name/Password */
                                            if (shouldKeepRunning) {
                                                if userNameFlag! {
                                                    if userName == nil {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-15")
                                                        returnCode = MqttReturnCode.MalformedPacket
                                                        shouldKeepRunning = false
                                                    } else {
                                                        let requiredPassword = self.users[userName!]
                                                        var givenPassword = ""
                                                        if passwordFlag! {
                                                            if password == nil {
                                                                MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-17")
                                                                returnCode = MqttReturnCode.MalformedPacket
                                                                shouldKeepRunning = false
                                                            } else {
                                                                let s = String(data: password!, encoding: String.Encoding.utf8)
                                                                givenPassword = s != nil ? s! : ""
                                                            }
                                                        } else {

                                                        }
                                                        if requiredPassword == givenPassword {
                                                            user = userName
                                                        } else {
                                                            MqttCompliance.sharedInstance().log(target: "Bad user name or password")
                                                            returnCode = MqttReturnCode.BadUserNameOrPassword
                                                            shouldKeepRunning = false
                                                        }
                                                    }
                                                } else {
                                                    if passwordFlag! {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-22")
                                                        returnCode = MqttReturnCode.MalformedPacket
                                                        shouldKeepRunning = false
                                                    } else {
                                                        if password == nil {
                                                            if self.allowAnonymous {
                                                                user = ""
                                                            } else {
                                                                MqttCompliance.sharedInstance().log(target: "Not Authorized")
                                                                returnCode = MqttReturnCode.NotAuthorized
                                                                shouldKeepRunning = false
                                                            }
                                                        } else {
                                                            MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-20")
                                                            returnCode = MqttReturnCode.MalformedPacket
                                                            shouldKeepRunning = false
                                                        }
                                                    }
                                                }
                                            }
                                            /* Will Topic/Message */
                                            if (shouldKeepRunning) {
                                                if willFlag {
                                                    if willTopic == nil || willMessage == nil {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-9")
                                                        returnCode = MqttReturnCode.MalformedPacket
                                                        shouldKeepRunning = false
                                                    }
                                                }
                                            }

                                            let mqttProperties = cp!.mqttProperties()

                                            /* Session */
                                            if shouldKeepRunning {
                                                mqttSession = MqttSessions.sharedInstance().get(clientId: clientId!)
                                                if mqttSession == nil {
                                                    MqttCompliance.sharedInstance().log(target: "MQTT-3.2.2-4")
                                                    mqttSession = MqttSession(clientId: clientId!)
                                                    MqttSessions.sharedInstance().store(session: mqttSession!)
                                                } else {
                                                    if cleanStart! {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.1.2-2")
                                                        mqttSession = MqttSession(clientId: clientId!)
                                                        MqttSessions.sharedInstance().store(session: mqttSession!)
                                                    } else {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.2.2-3")
                                                        mqttSession!.present = true
                                                    }
                                                }

                                                if mqttProperties != nil {
                                                    if (mqttProperties!.willDelayInterval != nil) {
                                                        mqttSession!.willDelayInterval = mqttProperties!.willDelayInterval!
                                                    }
                                                    if (mqttProperties!.sessionExpiryInterval != nil) {
                                                        mqttSession!.sessionExpiryInterval = mqttProperties!.sessionExpiryInterval!
                                                    }
                                                    /* Maximum Packet Size */
                                                    if (mqttProperties!.maximumPacketSize != nil) {
                                                        if mqttProperties!.maximumPacketSize! == 0 ||
                                                            mqttProperties!.maximumPacketSize! >  2684354565 {
                                                            MqttCompliance.sharedInstance().log(target: "Maximum Packet Size illegal Value")
                                                            returnCode = MqttReturnCode.ProtocolError
                                                            shouldKeepRunning = false
                                                        } else {
                                                            mqttSession!.maximumPacketSize = mqttProperties!.maximumPacketSize
                                                        }
                                                    }
                                                }

                                                if (shouldKeepRunning) {
                                                    mqttSession!.willFlag = willFlag
                                                    mqttSession!.willTopic = willTopic
                                                    mqttSession!.willMessage = willMessage
                                                    mqttSession!.willQoS = willQoS
                                                    mqttSession!.willRetain = willRetain

                                                    mqttSession!.socket = socket
                                                    /* Todo add kickout same clientId handling */

                                                    MqttCompliance.sharedInstance().log(target: "MQTT-3.2.0-1")
                                                    MqttCompliance.sharedInstance().log(target: "MQTT-3.2.0-2")

                                                    mqttSession!.connectKeepAlive = cp!.mqttConnectKeepAlive()!
                                                    mqttSession!.serverKeepAlive = cp!.mqttConnectKeepAlive()!
                                                    mqttSession!.lastPacket = Date()
                                                    var connack = Data()
                                                    var u : UInt8
                                                    u = MqttControlPacketType.CONNACK.rawValue << 4
                                                    connack.append(u)

                                                    MqttCompliance.sharedInstance().log(target: "MQTT-3.2.2-1")

                                                    var variableData = Data()

                                                    u = 0
                                                    if mqttSession!.present != nil && mqttSession!.present! {
                                                        u = u | 1
                                                    }
                                                    variableData.append(u)
                                                    u = 0
                                                    if returnCode != nil {
                                                        u = returnCode!.rawValue
                                                    }
                                                    variableData.append(u)

                                                    var remaining = Data()
                                                    u = MqttPropertyIdentifier.ReceiveMaximum.rawValue
                                                    remaining.append(u)
                                                    remaining.append(MqttControlPacket.mqttTwoByte(variable: self.receiveMaximum))

                                                    u = MqttPropertyIdentifier.MaximumQoS.rawValue
                                                    remaining.append(u)
                                                    u = self.maximumQoS.rawValue
                                                    remaining.append(u)

                                                    u = MqttPropertyIdentifier.RetainAvailable.rawValue
                                                    remaining.append(u)
                                                    u = self.retainAvailable ? 1 : 0
                                                    remaining.append(u)

                                                    u = MqttPropertyIdentifier.MaximumPacketSize.rawValue
                                                    remaining.append(u)
                                                    remaining.append(MqttControlPacket.mqttFourByte(variable: self.maximumPacketSize))

                                                    u = MqttPropertyIdentifier.AssignedClientIdentifier.rawValue
                                                    remaining.append(u)
                                                    remaining.append(MqttControlPacket.mqttUtf8(variable: clientId!))

                                                    u = MqttPropertyIdentifier.TopicAliasMaximum.rawValue
                                                    remaining.append(u)
                                                    remaining.append(MqttControlPacket.mqttTwoByte(variable: self.topicAliasMaximum))

                                                    u = MqttPropertyIdentifier.ResponseInformation.rawValue
                                                    remaining.append(u)
                                                    remaining.append(MqttControlPacket.mqttUtf8(variable: "Todo"))

                                                    u = MqttPropertyIdentifier.WildcardSubscriptionAvailable.rawValue
                                                    remaining.append(u)
                                                    u = self.wildcardSubscritionAvailable ? 1 : 0
                                                    remaining.append(u)

                                                    u = MqttPropertyIdentifier.SubscriptionIdentifiersAvailable.rawValue
                                                    remaining.append(u)
                                                    u = self.subscriptionIdentifiersAvailable ? 1 : 0
                                                    remaining.append(u)

                                                    u = MqttPropertyIdentifier.SharedSubscriptionAvailable.rawValue
                                                    remaining.append(u)
                                                    u = self.sharedSubscriptionAvailable ? 1 : 0
                                                    remaining.append(u)

                                                    if (self.serverKeepAlive != nil) {
                                                        mqttSession!.serverKeepAlive = self.serverKeepAlive!
                                                        u = MqttPropertyIdentifier.ServerKeepAlive.rawValue
                                                        remaining.append(u)
                                                        remaining.append(MqttControlPacket.mqttTwoByte(variable: self.serverKeepAlive!))
                                                    }

                                                    if (self.serverMoved != nil) {
                                                        u = MqttPropertyIdentifier.ServerReference.rawValue
                                                        remaining.append(u)
                                                        remaining.append(MqttControlPacket.mqttUtf8(variable: self.serverMoved!))
                                                    } else if (self.serverToUse != nil) {
                                                        u = MqttPropertyIdentifier.ServerReference.rawValue
                                                        remaining.append(u)
                                                        remaining.append(MqttControlPacket.mqttUtf8(variable: self.serverToUse!))
                                                    }

                                                    u = MqttPropertyIdentifier.ResponseInformation.rawValue
                                                    remaining.append(u)
                                                    remaining.append(MqttControlPacket.mqttUtf8(variable: "Todo"))

                                                    for (propertyName, propertyValue) in self.userProperties {
                                                        u = MqttPropertyIdentifier.UserProperty.rawValue
                                                        remaining.append(u)
                                                        remaining.append(MqttControlPacket.mqttUtf8(variable:propertyName))
                                                        remaining.append(MqttControlPacket.mqttUtf8(variable: propertyValue))
                                                    }

                                                    /* Auth Method */
                                                    /* Auth Data */

                                                    variableData.append(MqttControlPacket.mqttVariableByte(variable: remaining.count))
                                                    variableData.append(remaining)

                                                    connack.append(MqttControlPacket.mqttVariableByte(variable: variableData.count))
                                                    connack.append(variableData)

                                                    print ("remaining \(remaining)")
                                                    print ("variableData \(variableData)")
                                                    print ("connack \(connack)")

                                                    mqttSession!.debug(s:"CONNACK sent", p:nil)
                                                    try socket.write(from: connack)
                                                }

                                                if returnCode != nil && returnCode != MqttReturnCode.Success {
                                                    shouldKeepRunning = false
                                                }
                                            }
                                        } else {
                                            if !connectReceived {
                                                MqttCompliance.sharedInstance().log(target: "MQTT-3.1.0-2")
                                                returnCode = MqttReturnCode.ProtocolError
                                                shouldKeepRunning = false
                                            }
                                            if shouldKeepRunning {
                                                if cpt == MqttControlPacketType.PUBLISH {
                                                    print ("PUBLISH received")
                                                    mqttSession!.lastPacket = Date()

                                                    let topic = cp!.mqttTopic()

                                                    if topic == nil {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.3.2-1")
                                                        returnCode = MqttReturnCode.TopicNameInvalid
                                                        shouldKeepRunning = false
                                                    }

                                                    if shouldKeepRunning && topic!.characters.count == 0 {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-4.7.3-1")
                                                        returnCode = MqttReturnCode.TopicNameInvalid
                                                        shouldKeepRunning = false
                                                    }

                                                    if shouldKeepRunning && topic!.contains("+") {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.3.2-2")
                                                        returnCode = MqttReturnCode.TopicNameInvalid
                                                        shouldKeepRunning = false
                                                    }

                                                    if shouldKeepRunning && topic!.contains("#") {
                                                        MqttCompliance.sharedInstance().log(target: "MQTT-3.3.2-2")
                                                        returnCode = MqttReturnCode.TopicNameInvalid
                                                        shouldKeepRunning = false
                                                    }

                                                    if shouldKeepRunning {
                                                        /* Todo check if allowed to write here */

                                                        let retain = cp!.mqttControlPacketRetain()
                                                        let qos = cp!.mqttControlPacketQoS()
                                                        let mqttProperties = cp!.mqttProperties()

                                                        if qos == nil {
                                                            MqttCompliance.sharedInstance().log(target: "MQTT-3.3.1-4")
                                                            returnCode = MqttReturnCode.ProtocolError
                                                            shouldKeepRunning = false
                                                        }

                                                        if shouldKeepRunning {
                                                            let payload = cp!.mqttPayload()
                                                            var message: MqttMessage?
                                                            if payload != nil {
                                                                message = MqttMessage(topic:topic!,
                                                                                      data:payload!,
                                                                                      qos:qos!,
                                                                                      retain:retain!)
                                                                message!.payloadFormatIndicator = mqttProperties != nil ? mqttProperties!.payloadFormatIndicator : 0
                                                                message!.publicationExpiryInterval = mqttProperties != nil ? mqttProperties!.publicationExpiryInterval : nil
                                                                message!.responseTopic = mqttProperties != nil ? mqttProperties!.responseTopic : nil
                                                                message!.correlationData = mqttProperties != nil ? mqttProperties!.correlationData : nil
                                                                message!.contentType = mqttProperties != nil ? mqttProperties!.contentType : nil
                                                                message!.topicAlias = mqttProperties != nil ? mqttProperties!.topicAlias : nil
                                                                message!.subscriptionIdentifiers = mqttProperties != nil ? mqttProperties!.subscriptionIdentifiers : nil
                                                            }

                                                            if (qos == MqttQoS.AtMostOnce) {
                                                                MqttSessions.processReceivedMessage(message: message!)
                                                            } else if (qos == MqttQoS.AtLeastOnce) {
                                                                MqttSessions.processReceivedMessage(message: message!)

                                                                var puback = Data()
                                                                var u : UInt8
                                                                u = MqttControlPacketType.PUBACK.rawValue << 4
                                                                puback.append(u)
                                                                let pid = cp!.mqttPacketIdentifier()
                                                                if pid != nil {
                                                                    u = 4
                                                                    puback.append(u)
                                                                    puback.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                                                                    u = 0
                                                                    puback.append(u)
                                                                    puback.append(MqttControlPacket.mqttVariableByte(variable: 0))

                                                                    mqttSession!.debug(s:"PUBACK sent", p:pid)
                                                                    try socket.write(from: puback)
                                                                }
                                                            } else if (qos == MqttQoS.ExactlyOnce) {
                                                                let pid = cp!.mqttPacketIdentifier()
                                                                if message != nil {
                                                                    message!.packetIdentifier = pid
                                                                    mqttSession!.inflightIn.store(message:message!)
                                                                }
                                                                var pubrec = Data()
                                                                var u : UInt8
                                                                u = (MqttControlPacketType.PUBREC.rawValue << 4)
                                                                pubrec.append(u)
                                                                if pid != nil {
                                                                    u = 4
                                                                    pubrec.append(u)
                                                                    pubrec.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                                                                    u = 0
                                                                    pubrec.append(u)
                                                                    pubrec.append(MqttControlPacket.mqttVariableByte(variable: 0))

                                                                    mqttSession!.debug(s:"PUBREC sent", p:pid)
                                                                    try socket.write(from: pubrec)
                                                                }
                                                            }
                                                        }
                                                    }

                                                } else if cpt == MqttControlPacketType.PUBACK {
                                                    mqttSession!.lastPacket = Date()
                                                    let pid = cp!.mqttPacketIdentifier()
                                                    mqttSession!.debug(s:"PUBACK received", p:pid)

                                                    let message = mqttSession!.inflightOut.find(pid:pid)
                                                    if message != nil {
                                                        mqttSession!.inflightOut.remove(pid:pid)
                                                        if mqttSession!.socket != nil {
                                                            let message = mqttSession!.queued.get()
                                                            if message != nil {
                                                                mqttSession!.send(message:message)
                                                                mqttSession!.inflightOut.store(message:message)
                                                            }
                                                        }
                                                    } else {
                                                        MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/E/puback",
                                                                                                                 data: "\(Int(Date.init().timeIntervalSince1970)): \(mqttSession!.clientId) pid not found \(pid != nil ? pid! : -1)".data(using:String.Encoding.utf8),
                                                                                                                 qos: .AtMostOnce,
                                                                                                                 retain: false))
                                                    }

                                                } else if cpt == MqttControlPacketType.PUBREC {
                                                    mqttSession!.lastPacket = Date()
                                                    let pid = cp!.mqttPacketIdentifier()
                                                    mqttSession!.debug(s:"PUBREC received", p:pid)

                                                    let message = mqttSession!.inflightOut.find(pid:pid)
                                                    if message != nil {
                                                        message!.pubrel = true
                                                        mqttSession!.inflightOut.update(message:message)

                                                        var pubrel = Data()
                                                        var u : UInt8
                                                        u = (MqttControlPacketType.PUBREL.rawValue << 4) | 0x02
                                                        pubrel.append(u)

                                                        u = 4
                                                        pubrel.append(u)
                                                        pubrel.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                                                        u = 0
                                                        pubrel.append(u)
                                                        pubrel.append(MqttControlPacket.mqttVariableByte(variable: 0))

                                                        mqttSession!.debug(s:"PUBREL sent", p:pid)
                                                        try socket.write(from: pubrel)
                                                    } else {
                                                        MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/E/pubrel",
                                                                                                                 data: "\(Int(Date.init().timeIntervalSince1970)): \(mqttSession!.clientId) pid not found \(pid != nil ? pid! : -1)".data(using:String.Encoding.utf8),
                                                                                                                 qos: .AtMostOnce,
                                                                                                                 retain: false))
                                                    }

                                                } else if cpt == MqttControlPacketType.PUBREL {
                                                    mqttSession!.lastPacket = Date()
                                                    let pid = cp!.mqttPacketIdentifier()
                                                    mqttSession!.debug(s:"PUBREL received", p:pid)

                                                    let message = mqttSession!.inflightIn.find(pid:pid)
                                                    if message != nil {
                                                        MqttSessions.processReceivedMessage(message: message!)
                                                        mqttSession!.inflightIn.remove(pid:pid)

                                                        var pubcomp = Data()
                                                        var u : UInt8
                                                        u = (MqttControlPacketType.PUBCOMP.rawValue << 4)
                                                        pubcomp.append(u)

                                                        u = 4
                                                        pubcomp.append(u)
                                                        pubcomp.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                                                        u = 0
                                                        pubcomp.append(u)
                                                        pubcomp.append(MqttControlPacket.mqttVariableByte(variable: 0))

                                                        mqttSession!.debug(s:"PUBCOMP sent", p:pid)
                                                        try socket.write(from: pubcomp)
                                                    } else {
                                                        MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/E/pubrel",
                                                                                                                 data: "\(Int(Date.init().timeIntervalSince1970)): \(mqttSession!.clientId) pid not found \(pid != nil ? pid! : -1)".data(using:String.Encoding.utf8),
                                                                                                                 qos: .AtMostOnce,
                                                                                                                 retain: false))
                                                    }

                                                } else if (cpt == MqttControlPacketType.PUBCOMP) {
                                                    mqttSession!.lastPacket = Date()
                                                    let pid = cp!.mqttPacketIdentifier()
                                                    mqttSession!.debug(s:"PUBCOMP received", p:pid)

                                                    let message = mqttSession!.inflightOut.find(pid:pid)
                                                    if message != nil {
                                                        mqttSession!.inflightOut.remove(pid:pid)
                                                        if mqttSession!.socket != nil {
                                                            let message = mqttSession!.queued.get()
                                                            if message != nil {
                                                                mqttSession!.send(message:message)
                                                                mqttSession!.inflightOut.store(message:message)
                                                            }
                                                        }
                                                    } else {
                                                        MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/E/pubcomp",
                                                                                                                 data: "\(Int(Date.init().timeIntervalSince1970)): \(mqttSession!.clientId) pid not found \(pid != nil ? pid! : -1)".data(using:String.Encoding.utf8),
                                                                                                                 qos: .AtMostOnce,
                                                                                                                 retain: false))
                                                    }
                                                    
                                                    
                                                } else if (cpt == MqttControlPacketType.SUBSCRIBE) {
                                                    print ("SUBSCRIBE received \(cp!.mqttSubscribeTopicFilters())")
                                                    mqttSession!.lastPacket = Date()
                                                    
                                                    let sf = cp!.mqttSubscribeTopicFilters()
                                                    let p = cp!.mqttProperties()
                                                    
                                                    for (subscription) in sf {
                                                        mqttSession!.store(subscription: subscription)
                                                        MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/M/subscribe",
                                                                                                                 data: "\(Int(Date.init().timeIntervalSince1970)): \(mqttSession!.clientId) \(subscription.qos) n=\(subscription.noLocal) r=\(subscription.retainAsPublish) \(subscription.retainHandling) i=\(subscription.subscriptionIdentifier != nil ? subscription.subscriptionIdentifier! : nil) \(subscription.topicFilter!)".data(using:String.Encoding.utf8),
                                                                                                                 qos: .AtMostOnce,
                                                                                                                 retain: true)
                                                        )
                                                        
                                                    }
                                                    
                                                    var suback = Data()
                                                    var u : UInt8
                                                    u = (MqttControlPacketType.SUBACK.rawValue << 4)
                                                    suback.append(u)
                                                    
                                                    let remainingLength = 2 + 1 + sf.count
                                                    let remainingLengthData = MqttControlPacket.mqttVariableByte(variable: remainingLength)
                                                    suback.append(remainingLengthData)
                                                    
                                                    let pid = cp!.mqttPacketIdentifier()
                                                    if pid != nil {
                                                        suback.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                                                    }
                                                    u = 0
                                                    suback.append(u)
                                                    
                                                    for (subscription) in cp!.mqttSubscribeTopicFilters() {
                                                        suback.append(subscription.qos.rawValue)
                                                    }
                                                    
                                                    mqttSession!.debug(s:"SUBACK sent", p:pid)
                                                    try socket.write(from: suback)
                                                    
                                                } else if (cpt == MqttControlPacketType.UNSUBSCRIBE) {
                                                    mqttSession!.debug(s:"UNSUBSCRIBE received", p:nil)
                                                    
                                                    mqttSession!.lastPacket = Date()
                                                    
                                                    let uf = cp!.mqttUnsubscribeTopicFilters()
                                                    
                                                    var unsuback = Data()
                                                    var u : UInt8
                                                    u = (MqttControlPacketType.UNSUBACK.rawValue << 4)
                                                    unsuback.append(u)
                                                    
                                                    let remainingLength = 2 + 1 + uf.count
                                                    let remainingLengthData = MqttControlPacket.mqttVariableByte(variable: remainingLength)
                                                    unsuback.append(remainingLengthData)
                                                    
                                                    let pid = cp!.mqttPacketIdentifier()
                                                    if pid != nil {
                                                        unsuback.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                                                    }
                                                    
                                                    u = 0
                                                    unsuback.append(u)
                                                    
                                                    for (topicFilter) in uf {
                                                        let existingTopicFilter = mqttSession!.get(topicFilter:topicFilter)
                                                        if existingTopicFilter == nil {
                                                            u = MqttReturnCode.NoSubscriptionExisted.rawValue
                                                        } else {
                                                            mqttSession!.remove(topicFilter:topicFilter)
                                                            u = 0
                                                        }
                                                        MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/M/unsubscribe",
                                                                                                                 data: "\(Int(Date.init().timeIntervalSince1970)): \(mqttSession!.clientId) \(topicFilter)".data(using:String.Encoding.utf8),
                                                                                                                 qos: .AtMostOnce,
                                                                                                                 retain: true))
                                                        
                                                        unsuback.append(u)
                                                    }
                                                    
                                                    mqttSession!.debug(s:"UNSUBACK sent", p:pid)
                                                    try socket.write(from: unsuback)
                                                    
                                                } else if (cpt == MqttControlPacketType.PINGREQ) {
                                                    mqttSession!.debug(s:"PINGREQ received", p:nil)
                                                    mqttSession!.lastPacket = Date()
                                                    
                                                    var pingresp = Data()
                                                    var u : UInt8
                                                    u = MqttControlPacketType.PINGRESP.rawValue << 4
                                                    pingresp.append(u)
                                                    u = 0
                                                    pingresp.append(u)
                                                    
                                                    mqttSession!.debug(s:"PINGRESP received", p:nil)
                                                    try socket.write(from: pingresp)
                                                    
                                                } else if (cpt == MqttControlPacketType.DISCONNECT) {
                                                    mqttSession!.debug(s:"DISCONNECT received", p:nil)
                                                    mqttSession!.lastPacket = Date()
                                                    
                                                    let rc = cp!.mqttReturnCode()
                                                    if rc != nil {
                                                        if rc == MqttReturnCode.DisconnectWithWillMessage {
                                                            mqttSession!.askForWill()
                                                        }
                                                    }
                                                    
                                                    let mqttProperties = cp!.mqttProperties()
                                                    if mqttProperties != nil {
                                                        if mqttProperties!.sessionExpiryInterval != nil {
                                                            mqttSession!.sessionExpiryInterval = mqttProperties!.sessionExpiryInterval
                                                        }
                                                    }
                                                    
                                                    shouldKeepRunning = false
                                                    
                                                } else if (cpt == MqttControlPacketType.AUTH) {
                                                    print ("AUTH received")
                                                    mqttSession!.lastPacket = Date()
                                                    
                                                    shouldKeepRunning = false
                                                }
                                            }
                                        }
                                        cp = nil
                                    }
                                }
                            }
                        } else if bytesRead == 0 {
                            // TODO use wait
                        } else {
                            print ("bytesRead < 0")
                            shouldKeepRunning = false
                        }
                    }
                } while shouldKeepRunning && (mqttSession == nil || mqttSession!.shouldKeepRunning)
                
                var disconnect = Data()
                var u : UInt8
                u = MqttControlPacketType.DISCONNECT.rawValue << 4
                disconnect.append(u)
                u = 0
                if returnCode != nil {
                    u = 1
                    disconnect.append(u)
                    u = returnCode!.rawValue
                } else if mqttSession != nil && mqttSession!.returnCode != nil {
                    u = 1
                    disconnect.append(u)
                    u = mqttSession!.returnCode!.rawValue
                }
                disconnect.append(u)
                
                var payloadString = "\(Int(Date.init().timeIntervalSince1970)): disconnect sent"
                MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/D",
                                                                         data: payloadString.data(using:String.Encoding.utf8),
                                                                         qos: .AtMostOnce,
                                                                         retain: false))
                mqttSession!.debug(s:"DISCONNECT sent \(returnCode)", p:nil)
                try socket.write(from: disconnect)
                
                payloadString = "\(Int(Date.init().timeIntervalSince1970)): socket closed \(socket.remoteHostname):\(socket.remotePort)"
                MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/D",
                                                                         data: payloadString.data(using:String.Encoding.utf8),
                                                                         qos: .AtMostOnce,
                                                                         retain: false))
                socket.close()
                
                if mqttSession != nil {
                    mqttSession!.socket = nil
                }
                
                self.socketLockQueue.sync { [unowned self, socket] in
                    self.connectedSockets[socket.socketfd] = nil
                }
            }
            catch let error {
                guard let socketError = error as? Socket.Error else {
                    print("Unexpected error by connection at \(socket.remoteHostname):\(socket.remotePort)...")
                    return
                }
                if self.continueRunning {
                    print("Error reported by connection at \(socket.remoteHostname):\(socket.remotePort):\n \(socketError.description)")
                    socket.close()
                    if mqttSession != nil {
                        mqttSession!.socket = nil
                    }
                }
            }
        }
    }
    
}
