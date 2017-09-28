import Foundation

var verbose = false
var port: Int = 1883
var serverKeepAlive: Int? = nil
var retainAvailable = false
var maximumQoS = MqttQoS.AtMostOnce
var maximumClientIdLength = 23
var restrictedClientId = true
var receiveMaximum = 1
var maximumPacketSize = 10000
var topicAliasMaximum = 0
var wildcardSubscritionAvailable = false
var subscriptionIdentifiersAvailable = false
var sharedSubscriptionAvailable = false
var serverToUse: String? = nil
var serverMoved: String? = nil
var authMethods = [String]()
var allowAnonymous = false
var users: [String: String] = [:]
var userProperties: [String: String] = [:]
var accessControl: [String: [String: Bool]] = [:]

while case let option = getopt(CommandLine.argc, CommandLine.unsafeArgv, "achik:M:p:P:Q:rR:sS:T:u:U:vw?"),
    option != -1 {
        switch UnicodeScalar(CUnsignedChar(option)) {
        case "a":
            allowAnonymous = true
        case "c":
            restrictedClientId = false
        case "i":
            subscriptionIdentifiersAvailable = true

        case "M":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                serverMoved = s!
            }
        case "p":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                let p = Int(s!)
                if p != nil {
                    port = p!
                }
            }
        case "k":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                let p = Int(s!)
                if p != nil {
                    serverKeepAlive = p!
                }
            }
        case "P":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                let p = Int(s!)
                if p != nil {
                    maximumPacketSize = p!
                }
            }
        case "Q":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                let i = UInt8(s!)
                if i != nil {
                    let q = MqttQoS(rawValue: i!)
                    if q != nil {
                        maximumQoS = q!
                    }
                }
            }
        case "r":
            retainAvailable = true

        case "R":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                let r = Int(s!)
                if r != nil {
                    receiveMaximum = r!
                }
            }

        case "s":
            sharedSubscriptionAvailable = true

        case "S":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                serverToUse = s!
            }

        case "T":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                let t = Int(s!)
                if t != nil {
                    topicAliasMaximum = t!
                }
            }
        case "u":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                let components = s!.components(separatedBy: ":")
                if components.count == 1 {
                    users[components[0]] = ""
                } else if components.count == 2 {
                    users[components[0]] = components[1]
                }
            }
        case "U":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                let components = s!.components(separatedBy: "=")
                if  components.count == 2 {
                    userProperties[components[0]] = components[1]
                }
            }
        case "v":
            verbose = true
        case "w":
            wildcardSubscritionAvailable = true


        default:
            let command = String(format:"%s", CommandLine.unsafeArgv[0]!)
            print("Usage \(command) [Options]")
            print("\t-a allow anonymous (default off)")
            print("\t-c do not restrict Client ID to charactes and letters (default on)")
            print("\t-i subscription identifiers available (default off)")
            print("\t-k server keep alive (default none)")
            print("\t-M server server moved (default none)")
            print("\t-p port listen to port (default 1883)")
            print("\t-P max maximum packet size (default 10.000)")
            print("\t-Q max QoS supported (default 0)")
            print("\t-r RETAIN is available (default not)")
            print("\t-R max receive (input) Maximum (default 1)")
            print("\t-s shared subscriptions available (default off)")
            print("\t-S server use another server (default none)")
            print("\t-T max topic alias Maximum (default 0)")
            print("\t-u user[:password] add user with password (default none)")
            print("\t-U name=value add user property")
            print("\t-v verbose (default off)")
            print("\t-w wildcard subscriptions available (default off)")

            exit(1)
        }
}

if verbose {
    print("A Minimal MQTT v5.0 Conforming Broker in Swift")
    print("© 2017 Christoph Krey <c@ckrey.de>")
    print("\tport \(port)")
    if (serverKeepAlive != nil) {
        print("\tserverKeepAlive \(serverKeepAlive!)")
    } else {
        print("\tserverKeepAlive none")

    }
    print("\tretainAvailable \(retainAvailable)")
    print("\tmaximumQoS \(maximumQoS)")
    print("\tmaximumClientIdLength \(maximumClientIdLength)")
    print("\trestrictedClientId \(restrictedClientId)")
    print("\treceiveMaximum \(receiveMaximum)")
    print("\tmaximumPacketSize \(maximumPacketSize)")
    print("\ttopicAliasMaximum \(topicAliasMaximum)")
    print("\twildcardSubscritionAvailable \(wildcardSubscritionAvailable)")
    print("\tsubscriptionIdentifiersAvailable \(subscriptionIdentifiersAvailable)")
    print("\tsharedSubscriptionAvailable \(sharedSubscriptionAvailable)")
    print("\tserverToUse \(serverToUse != nil ? serverToUse! : "n/a")")
    print("\tserverMoved \(serverMoved != nil ? serverMoved! : "n/a")")
    print("\tauthMethods \(authMethods)")
    print("\tallowAnonymous \(allowAnonymous)")
    print("\tusers \(users)")
    print("\tuserProperties \(userProperties)")
    print("\taccessControl \(accessControl)")
    print()
}

let server = MqttServer(verbose: verbose,
                        port: port,
                        serverKeepAlive: serverKeepAlive,
                        retainAvailable: retainAvailable,
                        maximumQoS: maximumQoS,
                        maximumClientIdLength: maximumClientIdLength,
                        restrictedClientId: restrictedClientId,
                        receiveMaximum: receiveMaximum,
                        maximumPacketSize: maximumPacketSize,
                        topicAliasMaximum: topicAliasMaximum,
                        wildcardSubscritionAvailable: wildcardSubscritionAvailable,
                        subscriptionIdentifiersAvailable: subscriptionIdentifiersAvailable,
                        sharedSubscriptionAvailable: sharedSubscriptionAvailable,
                        serverToUse: serverToUse,
                        serverMoved: serverMoved,
                        authMethods: authMethods,
                        allowAnonymous: allowAnonymous,
                        users: users,
                        userProperties: userProperties,
                        accessControl: accessControl)
server.run()

