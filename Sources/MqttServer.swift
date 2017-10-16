import Foundation
import Socket
import Dispatch

class MqttServer {
    private static var theInstance: MqttServer?

    public class func sharedInstance() -> MqttServer {
        return theInstance!
    }

    static let bufferSize = 4096

    let port: Int
    let serverKeepAlive: Int?
    let verbose: Bool
    let responseInformation: Bool
    let retainAvailable: Bool?
    let maximumQoS: MqttQoS?
    let maximumClientIdLength: Int
    let restrictedClientId: Bool
    let maximumSessionExpiryInterval: Int
    let receiveMaximum: Int?
    let maximumPacketSize: Int?
    let topicAliasMaximum: Int?
    let wildcardSubscritionAvailable: Bool?
    let subscriptionIdentifiersAvailable: Bool?
    let sharedSubscriptionAvailable: Bool?
    let serverToUse: String?
    let serverMoved: String?
    let authMethods: [String]
    let allowAnonymous: Bool
    let users: [String: String]
    let userProperties: [String: String]
    let accessControl: [String: [String: Bool]]

    let start = Date()

    var listenSocket: Socket? = nil
    var connectedSockets = [Int32: Socket]()
    let socketLockQueue = DispatchQueue(label: "MqttSocketLockQueue")

    init(verbose: Bool = false,
         responseInformation: Bool = false,
         port: Int = 1883,
         serverKeepAlive: Int? = nil,
         retainAvailable: Bool?,
         maximumQoS: MqttQoS? = nil,
         maximumClientIdLength: Int = 23,
         restrictedClientId: Bool = true,
         maximumSessionExpiryInterval: Int = 0,
         receiveMaximum: Int? = nil,
         maximumPacketSize: Int? = nil,
         topicAliasMaximum: Int? = nil,
         wildcardSubscritionAvailable: Bool?,
         subscriptionIdentifiersAvailable: Bool?,
         sharedSubscriptionAvailable: Bool?,
         serverToUse: String? = nil,
         serverMoved: String? = nil,
         authMethods: [String] = [],
         allowAnonymous: Bool = false,
         users: [String: String] = [:],
         userProperties: [String: String] = [:],
         accessControl: [String: [String: Bool]] = [:]) {
        self.verbose = verbose
        self.responseInformation = responseInformation
        self.port = port
        self.serverKeepAlive = serverKeepAlive
        self.retainAvailable = retainAvailable
        self.maximumQoS = maximumQoS
        self.maximumClientIdLength = maximumClientIdLength
        self.restrictedClientId = restrictedClientId
        self.maximumSessionExpiryInterval = maximumSessionExpiryInterval
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
        MqttServer.theInstance = self
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
                while true {
                    let newSocket = try socket.acceptClientConnection()
                    print("Accepted connection from: \(newSocket.remoteHostname) on port \(newSocket.remotePort)")
                    print("Socket Signature: \(newSocket.signature?.description)")
                    self.addNewConnection(socket: newSocket)
                }
            }
            catch let error {
                guard let socketError = error as? Socket.Error else {
                    print("Unexpected error...")
                    return
                }
                print("Error reported:\n \(socketError.description)")
            }
        }

        MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/version",
                                                                 data: "m5s 0.0.1".data(using:String.Encoding.utf8),
                                                                 qos: .AtMostOnce,
                                                                 retain: true),
                                            from:"")
        MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/timestamp",
                                                                 data: Date().description.data(using:String.Encoding.utf8),
                                                                 qos: .AtMostOnce,
                                                                 retain: true),
                                            from:"")
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
            var returnCode : MqttReturnCode = MqttReturnCode.Success
            var cpt: MqttControlPacketType? = nil
            var connectReceived = false
            var mqttSession: MqttSession?
            var readData = Data(capacity: MqttServer.bufferSize)
            var cp : MqttControlPacket?

            MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/I/new Client",
                                                                     data: "\(Int(Date.init().timeIntervalSince1970)): \(socket.remoteHostname):\(socket.remotePort)".data(using:String.Encoding.utf8),
                                                                     qos: .AtMostOnce,
                                                                     retain: false),
                                                from:"")

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
                                                    if shouldKeepRunning && willRetain! && self.retainAvailable != nil && !self.retainAvailable! {
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
                                            var assignedClientId: String?
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
                                                    assignedClientId = clientId
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
                                                        if requiredPassword != givenPassword {
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
                                                            if !self.allowAnonymous {
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

                                            /* Session */
                                            if shouldKeepRunning {
                                                mqttSession = MqttSessions.sharedInstance().get(clientId: clientId!)
                                                if mqttSession == nil {
                                                    MqttCompliance.sharedInstance().log(target: "MQTT-3.2.2-4")
                                                    mqttSession = MqttSession(clientId: clientId!)
                                                    mqttSession!.assignedClientIdentifier = assignedClientId
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

                                                mqttSession!.socket = socket

                                                /* Todo add kickout same clientId handling */
                                                mqttSession!.user = userName
                                                mqttSession!.willFlag = willFlag
                                                mqttSession!.willTopic = willTopic
                                                mqttSession!.willMessage = willMessage
                                                mqttSession!.willQoS = willQoS
                                                mqttSession!.willRetain = willRetain

                                                (shouldKeepRunning, returnCode) = mqttSession!.connect(cp:cp)
                                            }
                                        } else {
                                            if !connectReceived {
                                                MqttCompliance.sharedInstance().log(target: "MQTT-3.1.0-2")
                                                returnCode = MqttReturnCode.ProtocolError
                                                shouldKeepRunning = false
                                            }
                                            if shouldKeepRunning {
                                                if cpt == MqttControlPacketType.PUBLISH {
                                                    (shouldKeepRunning, returnCode) = mqttSession!.publish(cp:cp)

                                                } else if cpt == MqttControlPacketType.PUBACK {
                                                    shouldKeepRunning = mqttSession!.puback(cp:cp)

                                                } else if cpt == MqttControlPacketType.PUBREC {
                                                    shouldKeepRunning = mqttSession!.pubrec(cp:cp)

                                                } else if cpt == MqttControlPacketType.PUBREL {
                                                    shouldKeepRunning = mqttSession!.pubrel(cp:cp)

                                                } else if (cpt == MqttControlPacketType.PUBCOMP) {
                                                    shouldKeepRunning = mqttSession!.pubcomp(cp:cp)

                                                } else if (cpt == MqttControlPacketType.SUBSCRIBE) {
                                                    (shouldKeepRunning, returnCode) = mqttSession!.subscribe(cp:cp!)

                                                } else if (cpt == MqttControlPacketType.UNSUBSCRIBE) {
                                                    shouldKeepRunning = mqttSession!.unsubscribe(cp:cp!)

                                                } else if (cpt == MqttControlPacketType.PINGREQ) {
                                                    shouldKeepRunning = mqttSession!.pingreq(cp:cp!)

                                                } else if (cpt == MqttControlPacketType.DISCONNECT) {
                                                    shouldKeepRunning = mqttSession!.disconnect(cp:cp!)
                                                    
                                                } else if (cpt == MqttControlPacketType.AUTH) {
                                                    shouldKeepRunning = mqttSession!.auth(cp:cp!)
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
                if returnCode != MqttReturnCode.Success {
                    u = 1
                    disconnect.append(u)
                    u = returnCode.rawValue
                } else if mqttSession != nil && mqttSession!.returnCode != nil {
                    u = 1
                    disconnect.append(u)
                    u = mqttSession!.returnCode!.rawValue
                }
                disconnect.append(u)

                /* TODO Reason String */
                /* TODO User Properties */
                
                var payloadString = "\(Int(Date.init().timeIntervalSince1970)): disconnect sent"
                MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/D",
                                                                         data: payloadString.data(using:String.Encoding.utf8),
                                                                         qos: .AtMostOnce,
                                                                         retain: false),
                                                    from:"")
                if mqttSession != nil {
                    mqttSession!.debug(s:"DISCONNECT sent \(returnCode)", p:nil)
                }
                try socket.write(from: disconnect)
                
                payloadString = "\(Int(Date.init().timeIntervalSince1970)): socket closed \(socket.remoteHostname):\(socket.remotePort)"
                MqttSessions.processReceivedMessage(message: MqttMessage(topic: "$SYS/broker/log/D",
                                                                         data: payloadString.data(using:String.Encoding.utf8),
                                                                         qos: .AtMostOnce,
                                                                         retain: false),
                                                    from:"")
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
                print("Error reported by connection at \(socket.remoteHostname):\(socket.remotePort):\n \(socketError.description)")
                socket.close()
                if mqttSession != nil {
                    mqttSession!.socket = nil
                }
            }
        }
    }

}



