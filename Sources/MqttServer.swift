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
    let accessControl: [String: [String: Bool]]

    let mqttCompliance: MqttCompliance
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
        self.accessControl = accessControl

        self.mqttCompliance = MqttCompliance(verbose: self.verbose)
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

        do {
            try MqttSessions.processReceivedMessage(topic: "$SYS/broker/version",
                                            payload: "m5s 0.0.1".data(using:String.Encoding.utf8)!,
                                            retain: true,
                                            qos: .AtMostOnce)
            try MqttSessions.processReceivedMessage(topic: "$SYS/broker/timestamp",
                                            payload: Date().description.data(using:String.Encoding.utf8)!,
                                            retain: true,
                                            qos: .AtMostOnce)
        } catch {

        }


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
            do {
                repeat {
                    let sockets = try Socket.wait(for:[socket], timeout: 1000, waitForever: false)
                    if (sockets != nil) {
                        let bytesRead = try socket.read(into: &readData)
                        if bytesRead > 0 {
                            print("Server received from \(socket.remoteHostname):\(socket.remotePort): \(readData) ")
                            var readBuffer = [UInt8](readData)
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
                                        self.mqttCompliance.log(target: "MQTT-3.2.2-16")
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
                                                self.mqttCompliance.log(target: "MQTT-3.1.0-1")
                                                returnCode = MqttReturnCode.ProtocolError
                                                shouldKeepRunning = false
                                            }
                                            connectReceived = true
                                            let remainingLength = cp!.mqttRemainingLength()
                                            if shouldKeepRunning && (remainingLength == nil || remainingLength! < 10) {
                                                self.mqttCompliance.log(target: "CONNECT remaining length < 10")
                                                returnCode = MqttReturnCode.MalformedPacket
                                                shouldKeepRunning = false
                                            }

                                            let protocolName = cp!.mqttProtocolName()
                                            if shouldKeepRunning && (protocolName == nil || protocolName! != "MQTT") {
                                                self.mqttCompliance.log(target: "CONNECT not name MQTT")
                                                returnCode = MqttReturnCode.MalformedPacket
                                                shouldKeepRunning = false
                                            }

                                            let protocolVersion = cp!.mqttProtocolVersion()
                                            if shouldKeepRunning && (protocolVersion == nil || protocolVersion! != 5) {
                                                self.mqttCompliance.log(target: "CONNECT not version 5")
                                                returnCode = MqttReturnCode.UnsupportedProtocolVersion
                                                shouldKeepRunning = false
                                            }

                                            if shouldKeepRunning && self.serverMoved != nil {
                                                self.mqttCompliance.log(target: "Server Moved")
                                                returnCode = MqttReturnCode.ServerMoved
                                                shouldKeepRunning = false
                                            }

                                            if shouldKeepRunning && self.serverToUse != nil {
                                                self.mqttCompliance.log(target: "Use Another Server")
                                                returnCode = MqttReturnCode.UseAnotherServer
                                                shouldKeepRunning = false
                                            }

                                            let connectFlags = cp!.mqttConnectFlags()
                                            var userNameFlag : Bool?
                                            var passwordFlag : Bool?
                                            var willRetain : Bool?
                                            var willQoS : MqttQoS?
                                            var willFlag : Bool = false
                                            var cleanStart : Bool?

                                            if shouldKeepRunning && (connectFlags == nil) {
                                                self.mqttCompliance.log(target: "CONNECT no connect flags")
                                                returnCode = MqttReturnCode.MalformedPacket
                                                shouldKeepRunning = false
                                            } else {
                                                userNameFlag = ((connectFlags! >> 7) & 1) == 1
                                                passwordFlag = ((connectFlags! >> 6) & 1) == 1

                                                willFlag = ((connectFlags! >> 2) & 1) == 1
                                                willRetain = ((connectFlags! >> 5) & 1) == 1
                                                if !willFlag && willRetain! {
                                                    self.mqttCompliance.log(target: "MQTT-3.1.2-11")
                                                    returnCode = MqttReturnCode.MalformedPacket
                                                    shouldKeepRunning = false
                                                }
                                                if shouldKeepRunning && willRetain! && !self.retainAvailable {
                                                    self.mqttCompliance.log(target: "RetainNotAvailable")
                                                    returnCode = MqttReturnCode.RetainNotSupported
                                                    shouldKeepRunning = false
                                                }

                                                willQoS = MqttQoS(rawValue: ((connectFlags! >> 3) & 3))

                                                if shouldKeepRunning && willQoS! == MqttQoS.Reserved {
                                                    self.mqttCompliance.log(target: "MQTT-3.1.2-10")
                                                    returnCode = MqttReturnCode.MalformedPacket
                                                    shouldKeepRunning = false
                                                }
                                                if shouldKeepRunning && !willFlag &&
                                                    willQoS! != MqttQoS.AtMostOnce {
                                                    self.mqttCompliance.log(target: "MQTT-3.1.2-09")
                                                    returnCode = MqttReturnCode.MalformedPacket
                                                    shouldKeepRunning = false
                                                }
                                                if shouldKeepRunning && willQoS! == MqttQoS.AtLeastOnce &&
                                                    self.maximumQoS == MqttQoS.AtMostOnce {
                                                    self.mqttCompliance.log(target: "QoS1 not supported")
                                                    returnCode = MqttReturnCode.QoSNotSupported
                                                    shouldKeepRunning = false
                                                }
                                                if shouldKeepRunning && willQoS! == MqttQoS.ExactlyOnce &&
                                                    self.maximumQoS != MqttQoS.ExactlyOnce {
                                                    self.mqttCompliance.log(target: "QoS2 not supported")
                                                    returnCode = MqttReturnCode.QoSNotSupported
                                                    shouldKeepRunning = false
                                                }

                                                cleanStart = ((connectFlags! >> 1) & 1) == 1

                                                if shouldKeepRunning && (connectFlags! & 1) != 0 {
                                                    self.mqttCompliance.log(target: "MQTT-3.1.2-1")
                                                    returnCode = MqttReturnCode.MalformedPacket
                                                    shouldKeepRunning = false
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

                                            print ("ClientId \(clientId)")
                                            print ("WillFlag \(willFlag)")
                                            print ("willTopic \(willTopic)")
                                            print ("willMessage \(willMessage)")
                                            print ("userNameFlag \(userNameFlag)")
                                            print ("userName \(userName)")
                                            print ("passwordFlag \(passwordFlag)")
                                            print ("password \(password)")

                                            /* Client ID */
                                            if (shouldKeepRunning) {
                                                if clientId == nil || clientId!.characters.count == 0 {
                                                    self.mqttCompliance.log(target: "MQTT-3.1.3-6")
                                                    clientId = String("m5s\(abs(UUID().hashValue))")
                                                    print ("ClientId \(clientId)")
                                                }

                                                if shouldKeepRunning && clientId!.characters.count > self.maximumClientIdLength {
                                                    self.mqttCompliance.log(target: "MQTT-3.1.3-8")
                                                    returnCode = MqttReturnCode.ClientIdentifierNotValid
                                                    shouldKeepRunning = false
                                                }

                                                if shouldKeepRunning && self.restrictedClientId {
                                                    let charset = CharacterSet(charactersIn: "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

                                                    if (clientId!.trimmingCharacters(in: charset) != "") {
                                                        self.mqttCompliance.log(target: "MQTT-3.1.3-5")
                                                        returnCode = MqttReturnCode.ClientIdentifierNotValid
                                                        shouldKeepRunning = false
                                                    }
                                                }
                                            }

                                            /* User Name/Password */
                                            if (shouldKeepRunning) {
                                                if userNameFlag! {
                                                    if userName == nil {
                                                        self.mqttCompliance.log(target: "MQTT-3.1.2-15")
                                                        returnCode = MqttReturnCode.MalformedPacket
                                                        shouldKeepRunning = false
                                                    } else {
                                                        let requiredPassword = self.users[userName!]
                                                        var givenPassword = ""
                                                        if passwordFlag! {
                                                            if password == nil {
                                                                self.mqttCompliance.log(target: "MQTT-3.1.2-17")
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
                                                            self.mqttCompliance.log(target: "Bad user name or password")
                                                            returnCode = MqttReturnCode.BadUserNameOrPassword
                                                            shouldKeepRunning = false
                                                        }
                                                    }
                                                } else {
                                                    if passwordFlag! {
                                                        self.mqttCompliance.log(target: "MQTT-3.1.2-22")
                                                        returnCode = MqttReturnCode.MalformedPacket
                                                        shouldKeepRunning = false
                                                    } else {
                                                        if password == nil {
                                                            if self.allowAnonymous {
                                                                user = ""
                                                            } else {
                                                                self.mqttCompliance.log(target: "Not Authorized")
                                                                returnCode = MqttReturnCode.NotAuthorized
                                                                shouldKeepRunning = false
                                                            }
                                                        } else {
                                                            self.mqttCompliance.log(target: "MQTT-3.1.2-20")
                                                            returnCode = MqttReturnCode.MalformedPacket
                                                            shouldKeepRunning = false
                                                        }
                                                    }
                                                }
                                            }

                                            let mqttProperties = cp!.mqttProperties()

                                            /* Session */
                                            if shouldKeepRunning {
                                                mqttSession = MqttSessions.sharedInstance().get(clientId: clientId!)
                                                if mqttSession == nil {
                                                    self.mqttCompliance.log(target: "MQTT-3.2.2-4")
                                                    mqttSession = MqttSession(clientId: clientId!)
                                                    MqttSessions.sharedInstance().store(session: mqttSession!)
                                                } else {
                                                    if cleanStart! {
                                                        self.mqttCompliance.log(target: "MQTT-3.1.2-2")
                                                        mqttSession = MqttSession(clientId: clientId!)
                                                        MqttSessions.sharedInstance().store(session: mqttSession!)
                                                    } else {
                                                        self.mqttCompliance.log(target: "MQTT-3.2.2-3")
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
                                                }

                                                mqttSession!.willFlag = willFlag
                                                mqttSession!.willTopic = willTopic
                                                mqttSession!.willMessage = willMessage
                                                mqttSession!.willQoS = willQoS
                                                mqttSession!.willRetain = willRetain
                                                
                                                mqttSession!.socket = socket
                                                /* Todo add kickout same clientId handling */

                                                self.mqttCompliance.log(target: "MQTT-3.2.0-1")
                                                self.mqttCompliance.log(target: "MQTT-3.2.0-2")

                                                mqttSession!.connectKeepAlive = cp!.mqttConnectKeepAlive()!
                                                mqttSession!.serverKeepAlive = cp!.mqttConnectKeepAlive()!
                                                mqttSession!.lastPacket = Date()
                                                var connack = Data()
                                                var u : UInt8
                                                u = MqttControlPacketType.CONNACK.rawValue << 4
                                                connack.append(u)

                                                self.mqttCompliance.log(target: "MQTT-3.2.2-1")

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

                                                /* Response Information */
                                                /* Auth Method */
                                                /* Auth Data */

                                                variableData.append(MqttControlPacket.mqttVariableByte(variable: remaining.count))
                                                variableData.append(remaining)

                                                connack.append(MqttControlPacket.mqttVariableByte(variable: variableData.count))
                                                connack.append(variableData)

                                                print ("remaining \(remaining)")
                                                print ("variableData \(variableData)")
                                                print ("connack \(connack)")

                                                try socket.write(from: connack)

                                                if returnCode != nil && returnCode != MqttReturnCode.Success {
                                                    shouldKeepRunning = false
                                                }
                                            }
                                        } else {
                                            if !connectReceived {
                                                self.mqttCompliance.log(target: "MQTT-3.1.0-2")
                                                returnCode = MqttReturnCode.ProtocolError
                                                shouldKeepRunning = false
                                            }
                                            if shouldKeepRunning {
                                                if (cpt == MqttControlPacketType.PUBLISH) {
                                                    print ("PUBLISH received")
                                                    mqttSession!.lastPacket = Date()

                                                    let topic = cp!.mqttTopic()
                                                    print ("Topic \(topic)")

                                                    if topic == nil {
                                                        self.mqttCompliance.log(target: "MQTT-3.3.2-1")
                                                        returnCode = MqttReturnCode.TopicNameInvalid
                                                        shouldKeepRunning = false
                                                    }

                                                    if shouldKeepRunning && topic!.characters.count == 0 {
                                                        self.mqttCompliance.log(target: "MQTT-4.7.3-1")
                                                        returnCode = MqttReturnCode.TopicNameInvalid
                                                        shouldKeepRunning = false
                                                    }

                                                    if shouldKeepRunning && topic!.contains("+") {
                                                        self.mqttCompliance.log(target: "MQTT-3.3.2-2")
                                                        returnCode = MqttReturnCode.TopicNameInvalid
                                                        shouldKeepRunning = false
                                                    }

                                                    if shouldKeepRunning && topic!.contains("#") {
                                                        self.mqttCompliance.log(target: "MQTT-3.3.2-2")
                                                        returnCode = MqttReturnCode.TopicNameInvalid
                                                        shouldKeepRunning = false
                                                    }

                                                    if shouldKeepRunning {
                                                        /* Todo check if allowed to write here */

                                                        let retain = cp!.mqttControlPacketRetain()
                                                        let qos = cp!.mqttControlPacketQoS()

                                                        let payload = cp!.mqttPayload()
                                                        print ("Payload \(payload)")
                                                        if payload != nil {
                                                            try MqttSessions.processReceivedMessage(topic: topic!,
                                                                                                    payload: payload!,
                                                                                                    retain: retain!,
                                                                                                    qos: qos!)
                                                        }

                                                        if (qos == MqttQoS.AtLeastOnce) {
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

                                                                try socket.write(from: puback)
                                                            }
                                                        } else if (qos == MqttQoS.ExactlyOnce) {
                                                            var pubrec = Data()
                                                            var u : UInt8
                                                            u = MqttControlPacketType.PUBREC.rawValue << 4
                                                            pubrec.append(u)
                                                            let pid = cp!.mqttPacketIdentifier()
                                                            if pid != nil {
                                                                u = 4
                                                                pubrec.append(u)
                                                                pubrec.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                                                                u = 0
                                                                pubrec.append(u)
                                                                pubrec.append(MqttControlPacket.mqttVariableByte(variable: 0))

                                                                try socket.write(from: pubrec)
                                                            }
                                                        }
                                                    }
                                                } else if (cpt == MqttControlPacketType.PUBACK) {
                                                    print ("PUBACK received")
                                                    mqttSession!.lastPacket = Date()

                                                } else if (cpt == MqttControlPacketType.PUBREL) {
                                                    print ("PUBREL received")
                                                    mqttSession!.lastPacket = Date()

                                                    var pubcomp = Data()
                                                    var u : UInt8
                                                    u = MqttControlPacketType.PUBCOMP.rawValue << 4
                                                    pubcomp.append(u)
                                                    let pid = cp!.mqttPacketIdentifier()
                                                    if pid != nil {
                                                        u = 4
                                                        pubcomp.append(u)
                                                        pubcomp.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                                                        u = 0
                                                        pubcomp.append(u)
                                                        pubcomp.append(MqttControlPacket.mqttVariableByte(variable: 0))

                                                        try socket.write(from: pubcomp)
                                                    }
                                                } else if (cpt == MqttControlPacketType.PUBCOMP) {
                                                    print ("PUBCOMP received")
                                                    mqttSession!.lastPacket = Date()

                                                } else if (cpt == MqttControlPacketType.SUBSCRIBE) {
                                                    print ("SUBSCRIBE received")
                                                    mqttSession!.lastPacket = Date()

                                                    var suback = Data()
                                                    var u : UInt8
                                                    u = MqttControlPacketType.SUBACK.rawValue << 4
                                                    suback.append(u)
                                                    let sf = cp!.mqttSubscribeTopicFilters()
                                                    print ("sf \(sf)")

                                                    let remainingLength = 2 + 1 + sf.count
                                                    let remainingLengthData = MqttControlPacket.mqttVariableByte(variable: remainingLength)
                                                    suback.append(remainingLengthData)

                                                    let pid = cp!.mqttPacketIdentifier()
                                                    if pid != nil {
                                                        suback.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                                                    }
                                                    u = 0
                                                    suback.append(u)

                                                    for (_, qos) in sf {
                                                        u = qos.rawValue
                                                        suback.append(u)
                                                    }

                                                    try socket.write(from: suback)

                                                } else if (cpt == MqttControlPacketType.UNSUBSCRIBE) {
                                                    print ("UNSUBSCRIBE received")
                                                    mqttSession!.lastPacket = Date()

                                                    let uf = cp!.mqttUnsubscribeTopicFilters()
                                                    print ("uf \(uf)")
                                                    var unsuback = Data()
                                                    var u : UInt8
                                                    u = (MqttControlPacketType.UNSUBACK.rawValue << 4)
                                                    unsuback.append(u)

                                                    let remainingLength = 2 + uf.count
                                                    let remainingLengthData = MqttControlPacket.mqttVariableByte(variable: remainingLength)
                                                    unsuback.append(remainingLengthData)

                                                    let pid = cp!.mqttPacketIdentifier()
                                                    if pid != nil {
                                                        unsuback.append(MqttControlPacket.mqttTwoByte(variable: pid!))
                                                    }

                                                    for _ in uf {
                                                        u = 0
                                                        unsuback.append(u)
                                                    }

                                                    try socket.write(from: unsuback)
                                                    
                                                } else if (cpt == MqttControlPacketType.PINGREQ) {
                                                    print ("PINGREQ received")
                                                    mqttSession!.lastPacket = Date()
                                                    
                                                    var pingresp = Data()
                                                    var u : UInt8
                                                    u = MqttControlPacketType.PINGRESP.rawValue << 4
                                                    pingresp.append(u)
                                                    u = 0
                                                    pingresp.append(u)
                                                    try socket.write(from: pingresp)
                                                    
                                                } else if (cpt == MqttControlPacketType.DISCONNECT) {
                                                    print ("DISCONNECT received")
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
                try socket.write(from: disconnect)
                print("Socket: \(socket.remoteHostname):\(socket.remotePort) closed...")
                socket.close()
                if mqttSession != nil {
                    mqttSession!.socket = nil
                }
                
                self.socketLockQueue.sync { [unowned self, socket] in
                    self.connectedSockets[socket.socketfd] = nil
                }
                self.mqttCompliance.summary()
                MqttRetained.sharedInstance().summary()
                MqttSessions.sharedInstance().summary()
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
