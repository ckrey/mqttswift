import Foundation

var verbose = false
var port: Int = 1883
var serverKeepAlive: Int? = nil
var retainAvailable : Bool?
var responseInformation = false
var maximumQoS : MqttQoS? = nil
var maximumClientIdLength = 23
var restrictedClientId = true
var receiveMaximum : Int? = nil
var maximumSessionExpiryInterval = 0
var maximumPacketSize : Int? = nil
var topicAliasMaximum : Int? = nil
var wildcardSubscritionAvailable : Bool?
var subscriptionIdentifiersAvailable : Bool?
var sharedSubscriptionAvailable : Bool?
var serverToUse: String? = nil
var serverMoved: String? = nil
var authMethods = [String]()
var allowAnonymous = false
var users: [String: String] = [:]
var userProperties: [String: String] = [:]
var accessControl: [String: [String: Bool]] = [:]

while case let option = getopt(CommandLine.argc, CommandLine.unsafeArgv, "aA:ce:hiIk:M:O:p:P:Q:rRsST:u:U:vwWx?"),
    option != -1 {
        switch UnicodeScalar(CUnsignedChar(option)) {
        case "a":
            allowAnonymous = true

        case "A": // use Another server
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                serverToUse = s!
            }

        case "O": // server mOved
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                serverMoved = s!
            }


        case "c":
            restrictedClientId = false

        case "i":
            subscriptionIdentifiersAvailable = false

        case "I":
            subscriptionIdentifiersAvailable = true

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
        case "e":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                let p = Int(s!)
                if p != nil {
                    maximumSessionExpiryInterval = p!
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
            retainAvailable = false

        case "R":
            retainAvailable = true

        case "M":
            let s = String.init(cString: optarg, encoding: String.Encoding.utf8)
            if s != nil {
                let r = Int(s!)
                if r != nil {
                    receiveMaximum = r!
                }
            }

        case "s":
            sharedSubscriptionAvailable = false

        case "S":
            sharedSubscriptionAvailable = true

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
            wildcardSubscritionAvailable = false
        case "W":
            wildcardSubscritionAvailable = true

        case "x":
            responseInformation = true

        default:
            let command = String(format:"%s", CommandLine.unsafeArgv[0]!)
            print("Usage \(command) [Options]")

            print("\t-a allow anonymous (default off)")
            print("\t-A server use Another server (default none)")

            print("\t-c do not restrict Client ID to charactes and letters (default on)")
            print("\t-e max session expiry interval (default 0)")

            print("\t-i subscription identifiers not available (default implicit true)")
            print("\t-I subscription identifiers available (default implicit true)")

            print("\t-k server keep alive (default none)")
            print("\t-M max receive (input) Maximum (default 1)")

            print("\t-O server server mOved (default none)")

            print("\t-p port listen to port (default 1883)")
            print("\t-P max maximum packet size (default 10.000)")
            print("\t-Q max QoS supported (default 0)")

            print("\t-r RETAIN is not vailable (default implicit true)")
            print("\t-R RETAIN is available (default implicit true)")

            print("\t-s shared subscriptions not available (default implicit true)")
            print("\t-S shared subscriptions available (default implicit true)")

            print("\t-T max topic alias Maximum (default 0)")
            print("\t-u user[:password] add user with password (default none)")
            print("\t-U name=value add user property")
            print("\t-v verbose (default off)")

            print("\t-w wildcard subscriptions not available (default implicit true)")
            print("\t-W wildcard subscriptions available (default implicit true)")

            print("\t-x provide response information (default false)")

            exit(1)
        }
}

if verbose {
    print("A Minimal MQTT v5.0 Conforming Broker in Swift")
    print("© 2017 Christoph Krey <c@ckrey.de>")
    print("\tport \(port)")

    print("\tserverKeepAlive \(serverKeepAlive != nil ? String(serverKeepAlive!) : "(not specified)")")
    print("\tretainAvailable \(retainAvailable != nil ? String(retainAvailable!) : "(not specified implicit true)")")
    print("\tmaximumQoS \(maximumQoS != nil ? String(maximumQoS!.rawValue) : "(not specified, defaults to 2)")")
    print("\tmaximumClientIdLength \(maximumClientIdLength)")
    print("\trestrictedClientId \(restrictedClientId)")
    print("\tmaximumSessionExpiryInterval \(maximumSessionExpiryInterval)")
    print("\treceiveMaximum \(receiveMaximum != nil ? String(receiveMaximum!) : "(not specified, defaults to 65,535)")")
    print("\tmaximumPacketSize \(maximumPacketSize != nil ? String(maximumPacketSize!) : "(not specified)")")
    print("\ttopicAliasMaximum \(topicAliasMaximum != nil ? String(topicAliasMaximum!) : "(not specified, defaults to 0)")")
    print("\twildcardSubscritionAvailable \(wildcardSubscritionAvailable != nil ? String(wildcardSubscritionAvailable!) : "(not specified implicit true)")")
    print("\tsubscriptionIdentifiersAvailable \(subscriptionIdentifiersAvailable != nil ? String(subscriptionIdentifiersAvailable!) : "(not specified implicit true)")")
    print("\tsharedSubscriptionAvailable \(sharedSubscriptionAvailable != nil ? String(sharedSubscriptionAvailable!) : "(not specified implicit true)")")
    print("\tserverToUse \(serverToUse != nil ? serverToUse! : "(not specified)")")
    print("\tserverMoved \(serverMoved != nil ? serverMoved! : "(not specified)")")
    print("\tauthMethods \(authMethods)")
    print("\tallowAnonymous \(allowAnonymous)")
    print("\tusers \(users)")
    print("\tuserProperties \(userProperties)")
    print("\taccessControl \(accessControl)")
    print("\tverbose \(verbose)")
    print("\tresponseInformation \(responseInformation)")
    print()
}

let server = MqttServer(verbose: verbose,
                        responseInformation: responseInformation,
                        port: port,
                        serverKeepAlive: serverKeepAlive,
                        retainAvailable: retainAvailable,
                        maximumQoS: maximumQoS,
                        maximumClientIdLength: maximumClientIdLength,
                        restrictedClientId: restrictedClientId,
                        maximumSessionExpiryInterval: maximumSessionExpiryInterval,
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

