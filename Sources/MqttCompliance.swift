import Foundation

class MqttCompliance {
    private static var theInstance: MqttCompliance?

    public class func sharedInstance() -> MqttCompliance {
        if theInstance == nil {
            theInstance = MqttCompliance()
        }
        return theInstance!
    }

    /*
     [MQTT-1.5.4-1]     The character data in a UTF-8 Encoded String MUST be well-formed UTF-8 as defined by the Unicode specification [Unicode] and restated in RFC 3629 [RFC3629]. In particular, the character data MUST NOT include encodings of code points between U+D800 and U+DFFF.
     [MQTT-1.5.4-2]     A UTF-8 Encoded String MUST NOT include an encoding of the null character U+0000.
     [MQTT-1.5.4-3]     A UTF-8 encoded sequence 0xEF 0xBB 0xBF is always interpreted as U+FEFF ("ZERO WIDTH NO-BREAK SPACE") wherever it appears in a string and MUST NOT be skipped over or stripped off by a packet receiver.
     [MQTT-1.5.5-1]     The encoded value MUST use the minimum number of bytes necessary to represent the value.
     [MQTT-1.5.7-1]     Both strings MUST comply with the requirements for UTF-8 Encoded Strings.
     [MQTT-2.1.3-1]     Where a flag bit is marked as “Reserved” in Table 2.2 - Flag Bits, it is reserved for future use and MUST be set to the value listed in that table.
     [MQTT-2.2.1-2]     A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is set to 0.
     [MQTT-2.2.1-3]     Each time a Client sends a new SUBSCRIBE, UNSUBSCRIBE,or PUBLISH (where QoS > 0) MQTT Control Packet it MUST assign it a non-zero Packet Identifier that is currently unused.
     [MQTT-2.2.1-4]     Each time a Server sends a new PUBLISH (with QoS > 0) MQTT Control Packet it MUST assign it a non zero Packet Identifier that is currently unused.
     [MQTT-2.2.1-5]     A PUBACK, PUBREC, PUBREL, or PUBCOMP packet MUST contain the same Packet Identifier as the PUBLISH packet that was originally sent.
     [MQTT-2.2.1-6]     A SUBACK and UNSUBACK MUST contain the Packet Identifier that was used in the corresponding SUBSCRIBE and UNSUBSCRIBE packet respectively.
     [MQTT-2.2.2-1]     If there are no properties, this MUST be indicated by including a Property Length of zero.
     [MQTT-3.1.0-1]     After a Network Connection is established by a Client to a Server, the first packet sent from the Client to the Server MUST be a CONNECT packet.
     [MQTT-3.1.0-2]     The Server MUST process a second CONNECT packet sent from a Client as a Protocol Error and close the Network Connection.
     [MQTT-3.1.2-1]     The protocol name MUST be the UTF-8 String "MQTT". If the Server does not want to accept the CONNECT, and wishes to reveal that it is an MQTT Server it MAY send a CONNACK packet with Reason Code of 0x84 (Unsupported Protocol Version), and then it MUST close the Network Connection.
     [MQTT-3.1.2-2]     If the Protocol Version is not 5 and the Server does not want to accept the CONNECT packet, the Server MAY send a CONNACK packet with Reason Code 0x84 (Unsupported Protocol Version) and then MUST close the Network Connection
     [MQTT-3.1.2-3]     The Server MUST validate that the reserved flag in the CONNECT packet is set to 0.
     [MQTT-3.1.2-4]     If a CONNECT packet is received with Clean Start is set to 1, the Client and Server MUST discard any existing Session and start a new Session.
     [MQTT-3.1.2-5]     If a CONNECT packet is received with Clean Start set to 0 and there is a Session associated with the Client Identifier, the Server MUST resume communications with the Client based on state from the existing Session.
     [MQTT-3.1.2-6]     If a CONNECT packet is received with Clean Start set to 0 and there is no Session associated with the Client Identifier, the Server MUST create a new Session.
     [MQTT-3.1.2-7]     If the Will Flag is set to 1 this indicates that, a Will Message MUST be stored on the Server and associated with the Session.
     [MQTT-3.1.2-8]     The Will Message MUST be published after the Network Connection is subsequently closed and either the Will Delay Interval has elapsed or the Session ends, unless the Will Message has been deleted by the Server on receipt of a DISCONNECT packet with Reason Code 0x00 (Normal disconnection) or a new Network Connection for the ClientID is opened before the Will Delay Interval has elapsed.
     [MQTT-3.1.2-9]     If the Will Flag is set to 1, the Will QoS and Will Retain fields in the Connect Flags will be used by the Server, and the Will Topic and Will Message fields MUST be present in the Payload.
     [MQTT-3.1.2-10]     The Will Message MUST be removed from the stored Session State in the Server once it has been published or the Server has received a DISCONNECT packet with a Reason Code of 0x00 (Normal disconnection) from the Client.
     [MQTT-3.1.2-11]     If the Will Flag is set to 0, the Will QoS and Will Retain fields in the Connect Flags MUST be set to 0 and the Will Topic and Will Message fields MUST NOT be present in the Payload.
     [MQTT-3.1.2-12]     If the Will Flag is set to 0, the Server MUST NOT publish a Will Message.
     [MQTT-3.1.2-13]     If the Will Flag is set to 0, then the Will QoS MUST be set to 0 (0x00).
     [MQTT-3.1.2-14]     If the Will Flag is set to 1, the value of Will QoS can be 0 (0x00), 1 (0x01), or 2 (0x02).
     [MQTT-3.1.2-15]     If the Will Flag is set to 0, then Will Retain MUST be set to 0.
     [MQTT-3.1.2-16]     If the Will Flag is set to 1 and Will Retain is set to 0, the Server MUST publish the Will Message as a non-retained message.
     [MQTT-3.1.2-17]     If the Will Flag is set to 1 and Will Retain is set to 1, the Server MUST publish the Will Message as a retained message.
     [MQTT-3.1.2-18]     If the User Name Flag is set to 0, a User Name MUST NOT be present in the Payload.
     [MQTT-3.1.2-19]     If the User Name Flag is set to 1, a User Name MUST be present in the Payload.
     [MQTT-3.1.2-20]     If the Password Flag is set to 0, a Password MUST NOT be present in the Payload.
     [MQTT-3.1.2-21]     If the Password Flag is set to 1, a Password MUST be present in the Payload.
     [MQTT-3.1.2-22]     If Keep Alive is non-zero and in the absence of sending any other MQTT Control Packets, the Client MUST send a PINGREQ packet.
     [MQTT-3.1.2-23]     If the Server returns a Server Keep Alive on the CONNACK packet, the Client MUST use that value instead of the value it sent as the Keep Alive.
     [MQTT-3.1.2-24]     If the Keep Alive value is non-zero and the Server does not receive an MQTT Control Packet from the Client within one and a half times the Keep Alive time period, it MUST close the Network Connection to the Client as if the network had failed.
     [MQTT-3.1.2-25]     The Client and Server MUST store the Session State after the Network Connection is closed if the Session Expiry Interval is absent or greater than 0.
     [MQTT-3.1.2-26]     If a new Network Connection to this Session is made before the Will Delay Interval has passed, the Server MUST NOT send the Will Message.
     [MQTT-3.1.2-27]     The Server MUST NOT send packets exceeding Maximum Packet Size to the Client.
     [MQTT-3.1.2-28]     Where a Packet is too large to send, the Server MUST discard it without sending it and then behave as if it had completed sending that publication.
     [MQTT-3.1.2-29]     The Server MUST NOT send a Topic Alias in a PUBLISH packet to the Client greater than Topic Alias Maximum.
     [MQTT-3.1.2-30]     If Topic Alias Maximum is absent or zero, the Server MUST NOT send any Topic Aliases to the Client.
     [MQTT-3.1.2-31]     A value of 0 indicates that the Server MUST NOT return Response Information.
     [MQTT-3.1.2-32]     If the value of Request Problem Information is 0, the Server MAY return a Reason String or User Properties on a CONNACK or DISCONNECT packet, but MUST NOT send a Reason String or User Properties on any packet other than PUBLISH, CONNACK, or DISCONNECT.
     [MQTT-3.1.2-33]     If a Client sets an Authentication Method in the CONNECT, the Client MUST NOT send any packets other than AUTH or DISCONNECT packets until it has received a CONNACK packet.
     [MQTT-3.1.3-1]     The Payload of the CONNECT packet contains one or more length-prefixed fields, whose presence is determined by the flags in the Variable Header. These fields, if present, MUST appear in the order Client Identifier, Will Topic, Will Message, User Name, Password.
     [MQTT-3.1.3-2]     The ClientID MUST be used by Clients and by Servers to identify state that they hold relating to this MQTT Session between the Client and the Server.
     [MQTT-3.1.3-3]     The ClientID MUST be present and is the first field in the CONNECT packet Payload.
     [MQTT-3.1.3-4]     The ClientID MUST be a UTF-8 Encoded String.
     [MQTT-3.1.3-5]     The Server MUST allow ClientID’s which are between 1 and 23 UTF-8 encoded bytes in length, and that contain only the characters
     "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".
     [MQTT-3.1.3-6]     A Server MAY allow a Client to supply a ClientID that has a length of zero bytes, however if it does so the Server MUST treat this as a special case and assign a unique ClientID to that Client.
     [MQTT-3.1.3-7]     It MUST then process the CONNECT packet as if the Client had provided that unique ClientID, and MUST return the Assigned Client Identifier in the CONNACK packet.
     [MQTT-3.1.3-8]     If the Server rejects the ClientID it MAY respond to the CONNECT packet with a CONNACK using Reason Code 0x85 (Client Identifier not valid) as described in section 4.13 Handling errors, and then it MUST close the Network Connection.
     [MQTT-3.1.3-9]     If the Will Flag is set to 1, the Will Topic is the next field in the Payload. The Will Topic MUST be a UTF-8 Encoded String.
     [MQTT-3.1.3-10]     If the User Name Flag is set to 1, the User Name is the next field in the Payload. The User Name MUST be a UTF-8 Encoded String.
     [MQTT-3.1.4-1]     The Server MUST validate that the CONNECT packet conforms to section 3.1 and close the Network Connection if it does not conform.
     [MQTT-3.1.4-2]     The Server MAY check that the contents of the CONNECT packet meet any further restrictions and SHOULD perform authentication and authorization checks. If any of these checks fail, it MUST close the Network Connection.
     [MQTT-3.1.4-3]     If the ClientID represents a Client already connected to the Server, the Server sends a DISCONNECT packet to the existing Client with Reason Code of 0x8E (Session taken over) as described in section 4.13 and MUST close the Network Connection of the existing Client.
     [MQTT-3.1.4-4]     The Server MUST perform the processing of Clean Start.
     [MQTT-3.1.4-5]     The Server MUST acknowledge the CONNECT packet with a CONNACK packet containing a 0x00 (Success) Reason Code.
     [MQTT-3.1.4-6]     If the Server rejects the CONNECT, it MUST NOT process any data sent by the Client after the CONNECT packet except AUTH packets.
     [MQTT-3.2.0-1]     The Server MUST send a CONNACK with a 0x00 (Success) Reason Code before sending any Packet other than AUTH.
     [MQTT-3.2.0-2]     The Server MUST NOT send more than one CONNACK in a Network Connection.
     [MQTT-3.2.2-1]     Byte 1 is the "Connect Acknowledge Flags". Bits 7-1 are reserved and MUST be set to 0.
     [MQTT-3.2.2-2]     If the Server accepts a connection with Clean Start set to 1, the Server MUST set Session Present to 0 in the CONNACK packet in addition to setting a 0x00 (Success) Reason Code in the CONNACK packet.
     [MQTT-3.2.2-3]     If the Server accepts a connection with Clean Start set to 0 and the Server has Session State for the ClientID, it MUST set Session Present to 1 in the CONNACK packet, otherwise it MUST set Session Present to 0 in the CONNACK packet. In both cases it MUST set a 0x00 (Success) Reason Code in the CONNACK packet.
     [MQTT-3.2.2-4]     If the Client does not have Session State and receives Session Present set to 1 it MUST close the Network Connection.
     [MQTT-3.2.2-5]     If the Client does have Session State and receives Session Present set to 0 it MUST discard its Session State if it continues with the Network Connection.
     [MQTT-3.2.2-6]     If a Server sends a CONNACK packet containing a non-zero Reason Code it MUST set Session Present to 0.
     [MQTT-3.2.2-7]     If a Server sends a CONNACK packet containing a Reason code of 0x80 or greater it MUST then close the Network Connection.
     [MQTT-3.2.2-8]     The Server MUST use one of the Reason Code values in Table 3 1 - Connect Reason Code values.
     [MQTT-3.2.2-9]     If a Server does not support QoS 1 or QoS 2 PUBLISH packets it MUST send a Maximum QoS in the CONNACK packet specifying the highest QoS it supports.
     [MQTT-3.2.2-10]     A Server that does not support QoS 1 or QoS 2 PUBLISH packets MUST still accept SUBSCRIBE packets containing a Requested QoS of 0, 1 or 2.
     [MQTT-3.2.2-11]     If a Client receives a Maximum QoS from a Server, it MUST NOT send PUBLISH packets at a QoS level exceeding the Maximum QoS level specified.
     [MQTT-3.2.2-12]     If a Server receives a CONNECT packet containing a Will QoS that exceeds its capabilities, it MUST reject the connection. It SHOULD use a CONNACK packet with Reason Code 0x9B (QoS not supported) as described in section 4.13 Handling errors, and MUST close the Network Connection.
     [MQTT-3.2.2-13]     If a Server receives a CONNECT packet containing a Will Message with the Will Retain 1, and it does not support retained publications, the Server MUST reject the connection request. It SHOULD send CONNACK with Reason Code 0x9A (Retain not supported) and then it MUST close the Network Connection.
     [MQTT-3.2.2-14]     A Client receiving Retain Available from the Server MUST NOT send a PUBLISH packet with the RETAIN flag set to 1.
     [MQTT-3.2.2-15]     The Client MUST NOT send packets exceeding Maximum Packet Size to the Server.
     [MQTT-3.2.2-16]     If the Client connects using a zero length Client Identifier, the Server MUST respond with a CONNACK containing an Assigned Client Identifier. The Assigned Client Identifier MUST be a new Client Identifier not used by any other Session currently in the Server.
     [MQTT-3.2.2-17]     The Client MUST NOT send a Topic Alias in a PUBLISH packet to the Server greater than this value.
     [MQTT-3.2.2-18]     Topic Alias Maximum is absent, the Client MUST NOT send any Topic Aliases on to the Server.
     [MQTT-3.2.2-19]     The Server MUST NOT send this property if it would increase the size of the CONNACK packet beyond the Maximum Packet Size specified by the Client.
     [MQTT-3.2.2-20]     The Server MUST NOT send this property if it would increase the size of the CONNACK packet beyond the Maximum Packet Size specified by the Client.
     [MQTT-3.2.2-21]     If the Server sends a Server Keep Alive on the CONNACK packet, the Client MUST use this value instead of the Keep Alive value the Client sent on CONNECT.
     [MQTT-3.2.2-22]     If the Server does not send the Server Keep Alive, the Server MUST use the Keep Alive value set by the Client on CONNECT.
     [MQTT-3.3.1-1]     The DUP flag MUST be set to 1 by the Client or Server when it attempts to re-deliver a PUBLISH packet.
     [MQTT-3.3.1-2]     The DUP flag MUST be set to 0 for all QoS 0 messages.
     [MQTT-3.3.1-3]     The DUP flag in the outgoing PUBLISH packet is set independently to the incoming PUBLISH packet, its value MUST be determined solely by whether the outgoing PUBLISH packet is a retransmission.
     [MQTT-3.3.1-4]     A PUBLISH Packet MUST NOT have both QoS bits set to 1.
     [MQTT-3.3.1-5]     If the RETAIN flag is set to 1 in a PUBLISH packet sent by a Client to a Server, the Server MUST replace any existing retained message for this topic and store the Application Message and its QoS.
     [MQTT-3.3.1-6]     If the Payload contains zero bytes it is processed normally by the Server but any retained message with the same topic name MUST be removed and any future subscribers for the topic will not receive a retained message.
     [MQTT-3.3.1-7]     A zero length retained message MUST NOT be stored as a retained message on the Server.
     [MQTT-3.3.1-8]     If the RETAIN flag is 0 in a PUBLISH packet sent by a Client to a Server, the Server MUST NOT store the message as a retained message and MUST NOT remove or replace any existing retained message.
     [MQTT-3.3.1-9]     If Retain Handling is set to 0 the Server MUST send all retained messages matching the Topic Filter of the subscription to the Client.
     [MQTT-3.3.1-10]     If Retain Handling is set to 1 and the subscription did not already exist, the Server MUST send all retained message matching the Topic Filter of the subscription to the Client.
     [MQTT-3.3.1-11]     If Retain Handling is set to 2, the Server MUST NOT send retained messages at the time of the subscribe.
     [MQTT-3.3.1-12]     If the value of Retain As Published subscription option is set to 0, the Server MUST set the RETAIN flag to 0 when forwarding an Application Message regardless of how the RETAIN flag was set in the received PUBLISH packet.
     [MQTT-3.3.1-13]     If the value of Retain As Published subscription option is set to 1, the Server MUST set the RETAIN flag equal to the RETAIN flag in the received PUBLISH packet.
     [MQTT-3.3.2-1]     The Topic Name MUST be present as the first field in the PUBLISH packet Variable Header. It MUST be a UTF-8 Encoded String.
     [MQTT-3.3.2-2]     The Topic Name in the PUBLISH packet MUST NOT contain wildcard characters.
     [MQTT-3.3.2-3]     The Topic Name in a PUBLISH packet sent by a Server to a subscribing Client MUST match the Subscription’s Topic Filter.
     [MQTT-3.3.2-4]     A Server MUST send the Payload Format Indicator unaltered to all subscribers receiving the publication.
     [MQTT-3.3.2-5]     If the Publication Expiry Interval has passed and the Server has not managed to start onward delivery to a matching subscriber, then it MUST delete the copy of the message for that subscriber.
     [MQTT-3.3.2-6]     The PUBLISH packet sent to a Client by the Server MUST contain a Publication Expiry Interval set to the received value minus the time that the publication has been waiting in the Server.
     [MQTT-3.3.2-7]     A receiver MUST NOT carry forward any Topic Alias mappings from one Network Connection to another.
     [MQTT-3.3.2-8]     A sender MUST NOT send a PUBLISH packet containing a Topic Alias which has the value 0.
     [MQTT-3.3.2-9]     A Client MUST NOT send a PUBLISH packet with a Topic Alias greater than the Topic Alias Maximum value returned by the Server in the CONNACK packet.
     [MQTT-3.3.2-10]     A Client MUST accept all Topic Alias values greater than 0 and less than or equal to the Topic Alias Maximum value that it sent in the CONNECT packet.
     [MQTT-3.3.2-11]     A Server MUST NOT send a PUBLISH packet with a Topic Alias greater than the Topic Alias Maximum value sent by the Client in the CONNECT packet.
     [MQTT-3.3.2-12]     A Server MUST accept all Topic Alias values greater than 0 and less than or equal to the Topic Alias Maximum value that it returned in the CONNACK packet.
     [MQTT-3.3.2-13]     The Response Topic MUST be a UTF-8 Encoded String.
     [MQTT-3.3.2-14]     The Response Topic MUST NOT contain wildcard characters.
     [MQTT-3.3.2-15]     The Server MUST send the Response Topic unaltered to all subscribers receiving the publication.
     [MQTT-3.3.2-16]     The Server MUST send the Correlation Data unaltered to all subscribers receiving the publication.
     [MQTT-3.3.2-17]     The Server MUST send all User Properties unaltered in a PUBLISH packet when forwarding the Application Message to a Client.
     [MQTT-3.3.2-18]     The Server MUST maintain the order of User Properties when forwarding the Application Message.
     [MQTT-3.3.2-19]     The Content Type MUST be a UTF-8 Encoded String.
     [MQTT-3.3.2-20]     A Server MUST send the Content Type unaltered to all subscribers receiving the publication.
     [MQTT-3.3.4-1]     The receiver of a PUBLISH Packet MUST respond according to Table 3.4 - Expected PUBLISH packet response as determined by the QoS in the PUBLISH Packet.
     [MQTT-3.3.4-2]     In this case the Server MUST deliver the message to the Client respecting the maximum QoS of all the matching subscriptions.
     [MQTT-3.3.4-3]     If the Client specified a Subscription Identifier for any of the overlapping subscriptions the Server MUST send those Subscription Identifiers in the message which is published as the result of the subscriptions.
     [MQTT-3.3.4-4]     If the Server sends a single copy of the message it MUST include in the PUBLISH packet the Subscription Identifiers for all matching subscriptions which have a Subscription Identifiers, their order is not significant.
     [MQTT-3.3.4-5]     If the Server sends multiple PUBLISH packets it MUST send, in each of them, the Subscription Identifier of the matching subscription if it has a Subscription Identifier.
     [MQTT-3.3.4-6]     A PUBLISH packet sent from a Client to a Server MUST NOT contain a Subscription Identifier.
     [MQTT-3.3.4-7]     The Client MUST NOT send more than Receive Maximum QoS 1 and QoS 2 PUBLISH packets for which it has not received PUBACK, PUBCOMP, or PUBREC with a Reason Code of 128 or greater from the Server.
     [MQTT-3.3.4-8]     The Client MUST NOT delay the sending of any packets other than PUBLISH packets due to having sent Receive Maximum PUBLISH packets without receiving acknowledgements for them.
     [MQTT-3.3.4-9]     The Server MUST NOT send more than Receive Maximum QoS 1 and QoS 2 PUBLISH packets for which it has not received PUBACK, PUBCOMP, or PUBREC with a Reason Code of 128 or greater from the Client.
     [MQTT-3.3.4-10]     The Server MUST NOT delay the sending of any packets other than PUBLISH packets due to having sent Receive Maximum PUBLISH packets without receiving acknowledgements for them.
     [MQTT-3.4.2-1]     The Client or Server sending the PUBACK MUST use one of the PUBACK Reason Codes.
     [MQTT-3.4.2-2]     The sender MUST NOT send this property if it would increase the size of the PUBACK packet beyond the Maximum Packet Size specified by the receiver.
     [MQTT-3.4.2-3]     The sender MUST NOT send this property if it would increase the size of the PUBACK packet beyond the Maximum Packet Size specified by the receiver.
     [MQTT-3.5.2-1]     The Client or Server sending the PUBREC MUST use one of the PUBREC Reason Codes.
     [MQTT-3.5.2-2]     The sender MUST NOT send this property if it would increase the size of the PUBREC packet beyond the Maximum Packet Size specified by the receiver.
     [MQTT-3.5.2-3]     The sender MUST NOT send this property if it would increase the size of the PUBREC packet beyond the Maximum Packet Size specified by the receiver.
     [MQTT-3.6.1-1]     Bits 3,2,1 and 0 of the Fixed Header in the PUBREL packet are reserved and MUST be set to 0,0,1 and 0 respectively. The Server MUST treat any other value as malformed and close the Network Connection.
     [MQTT-3.6.2-1]     The Client or Server sending the PUBREL MUST use one of the PUBREL Reason Codes.
     [MQTT-3.6.2-2]     The sender MUST NOT send this Property if it would increase the size of the PUBREL packet beyond the Maximum Packet Size specified by the receiver.
     [MQTT-3.6.2-3]     The sender MUST NOT send this property if it would increase the size of the PUBREL packet beyond the Maximum Packet Size specified by the receiver.
     [MQTT-3.7.2-1]     The Client or Server sending the PUBCOMP MUST use one of the PUBCOMP Reason Codes.
     [MQTT-3.7.2-2]     The sender MUST NOT use this Property if it would increase the size of the PUBCOMP packet beyond the Maximum Packet Size specified by the receiver.
     [MQTT-3.7.2-3]     The sender MUST NOT send this property if it would increase the size of the PUBCOMP packet beyond the Maximum Packet Size specified by receiver.
     [MQTT-3.8.1-1]     Bits 3,2,1 and 0 of the Fixed Header of the SUBSCRIBE packet are reserved and MUST be set to 0,0,1 and 0 respectively. The Server MUST treat any other value as malformed and close the Network Connection
     [MQTT-3.8.3-1]
     The Topic Filters MUST be a UTF-8 Encoded String.
     [MQTT-3.8.3-2]     The Payload MUST contain at least one Topic Filter and Subscription Options pair.
     [MQTT-3.8.3-3]     Bit 2 of the Subscription Options represents the No Local option. If the value is 1, Application Messages MUST NOT be forwarded to a connection with a ClientID equal to the ClientID of the publishing connection.
     [MQTT-3.8.3-4]     It is a Protocol Error to set the No Local bit to 1 on a Shared Subscription.
     [MQTT-3.8.3-5]     The Server MUST treat a SUBSCRIBE packet as malformed if any of Reserved bits in the Payload are non-zero.
     [MQTT-3.8.4-1]     When the Server receives a SUBSCRIBE packet from a Client, the Server MUST respond with a SUBACK packet.
     [MQTT-3.8.4-2]     The SUBACK packet MUST have the same Packet Identifier as the SUBSCRIBE packet that it is acknowledging.
     [MQTT-3.8.4-3]     If a Server receives a SUBSCRIBE packet containing a Topic Filter that is identical to a Non-Shared Subscription’s Topic Filter for the current Session then it MUST completely replace that existing Subscription with a new Subscription.
     [MQTT-3.8.4-4]     If the Retain Handling option is 0, any existing retained messages matching the Topic Filter MUST be re-sent, but publications MUST NOT be lost due to replacing the Subscription.
     [MQTT-3.8.4-5]     If a Server receives a SUBSCRIBE packet that contains multiple Topic Filters it MUST handle that packet as if it had received a sequence of multiple SUBSCRIBE packets, except that it combines their responses into a single SUBACK response.
     [MQTT-3.8.4-6]     The SUBACK packet sent by the Server to the Client MUST contain a Reason Code for each Topic Filter/Subscription Option pair.
     [MQTT-3.8.4-7]     This Reason Code MUST either show the maximum QoS that was granted for that Subscription or indicate that the subscription failed.
     [MQTT-3.8.4-8]     The QoS of Payload Messages sent in response to a Subscription MUST be the minimum of the QoS of the originally published message and the Maximum QoS granted by the Server.
     [MQTT-3.9.2-1]     The Server MUST NOT send this Property if it would increase the size of the SUBACK packet beyond the Maximum Packet Size specified by the Client.
     [MQTT-3.9.2-2]     The Server MUST NOT send this property if it would increase the size of the SUBACK packet beyond the Maximum Packet Size specified by the Client.
     [MQTT-3.9.3-1]     The order of Reason Codes in the SUBACK packet MUST match the order of Topic Filters in the SUBSCRIBE packet.
     [MQTT-3.9.3-2]     The Server MUST send one of the Reason Codes listed in Table 3 10 - Subscribe Reason Codes for each subscription received.
     [MQTT-3.10.1-1]     Bits 3,2,1 and 0 of the Fixed Header of the UNSUBSCRIBE packet are reserved and MUST be set to 0,0,1 and 0 respectively. The Server MUST treat any other value as malformed and close the Network Connection
     [MQTT-3.10.3-1]     The Topic Filters in an UNSUBSCRIBE packet MUST be UTF-8 Encoded Strings.
     [MQTT-3.10.3-2]     The Payload of an UNSUBSCRIBE packet MUST contain at least one Topic Filter.
     [MQTT-3.10.4-1]     The Topic Filters (whether they contain wildcards or not) supplied in an UNSUBSCRIBE packet MUST be compared character-by-character with the current set of Topic Filters held by the Server for the Client. If any filter matches exactly then its owning Subscription MUST be deleted.
     [MQTT-3.10.4-2]     When a Server receives UNSUBSCRIBE It MUST stop adding any new messages which match the Topic Filters, for delivery to the Client.
     [MQTT-3.10.4-3]     When a Server receives UNSUBSCRIBE It MUST complete the delivery of any QoS 1 or QoS 2 messages which match the Topic Filters and it has started to send to the Client.
     [MQTT-3.10.4-4]     The Server MUST respond to an UNSUBSCRIBE request by sending an UNSUBACK packet.
     [MQTT-3.10.4-5]     The UNSUBACK packet MUST have the same Packet Identifier as the UNSUBSCRIBE packet. Even where no Topic Subscriptions are deleted, the Server MUST respond with an UNSUBACK.
     [MQTT-3.10.4-6]     If a Server receives an UNSUBSCRIBE packet that contains multiple Topic Filters, it MUST process that packet as if it had received a sequence of multiple UNSUBSCRIBE packets, except that it sends just one UNSUBACK response.
     [MQTT-3.11.2-1]     The Server MUST NOT send this Property if it would increase the size of the UNSUBACK packet beyond the Maximum Packet Size specified by the Client.
     [MQTT-3.11.2-2]     The Server MUST NOT send this property if it would increase the size of the UNSUBACK packet beyond the Maximum Packet Size specified by the receiver.
     [MQTT-3.11.3-1]     The order of Reason Codes in the UNSUBACK packet MUST match the order of Topic Filters in the UNSUBSCRIBE packet.
     [MQTT-3.11.3-2]     The Server MUST use one of the UNSUBSCRIBE Reason Code.
     [MQTT-3.12.4-1]     The Server MUST send a PINGRESP packet in response to a PINGREQ packet.
     [MQTT-3.14.0-1]     A Server MUST NOT send a DISCONNECT until after it has sent a CONNACK with Reason Code of less than 0x80.
     [MQTT-3.14.1-1]     The Client or Server MUST validate that reserved bits are set to 0. If they are not zero it sends a DISCONNECT packet with a Reason code of 0x81 (Malformed Packet).
     [MQTT-3.14.2-1]     The Client or Server sending the DISCONNECT packet MUST use one of the DISCONNECT Reason Codes.
     [MQTT-3.14.2-2]     The Session Expiry Interval MUST NOT be sent on a DISCONNECT by the Server.
     [MQTT-3.14.2-3]     The sender MUST NOT use this Property if it would increase the size of the DISCONNECT packet beyond the Maximum Packet Size specified by the receiver.
     [MQTT-3.14.2-4]     The sender MUST NOT send this property if it would increase the size of the DISCONNECT packet beyond the Maximum Packet Size specified by the receiver.
     [MQTT-3.14.4-1]     After sending a DISCONNECT packet the sender MUST NOT send any more MQTT Control Packets on that Network Connection.
     [MQTT-3.14.4-2]     After sending a DISCONNECT packet the sender MUST close the Network Connection.
     [MQTT-3.14.4-3]     On receipt of DISCONNECT with a Reason Code of 0x00 (Success) the Server MUST discard any Will Message associated with the current Connection without publishing it.
     [MQTT-3.15.1-1]     Bits 3,2,1 and 0 of the Fixed Header of the AUTH packet are reserved and MUST all be set to 0. The Client or Server MUST treat any other value as malformed and close the Network Connection.
     [MQTT-3.15.2-1]     The sender of the AUTH Packet MUST use one of the Authenticate Reason Codes.
     [MQTT-3.15.2-2]     The sender MUST NOT send this property if it would increase the size of the AUTH packet beyond the Maximum Packet Size specified by the receiver.
     [MQTT-4.1.0-1]     The Client and Server MUST NOT discard the Session State while the Network Connection is open.
     [MQTT-4.1.0-2]     The Server MUST discard the Session State when the Network Connection is closed and the Session Expiry Interval has passed.
     [MQTT-4.3.1-1]     In the QoS 0 delivery protocol, the sender MUST send a PUBLISH packet with QoS 0 and DUP flag set to 0.
     [MQTT-4.3.2-1]     In the QoS 1 delivery protocol, the sender MUST assign an unused Packet Identifier each time it has a new Application Message to publish.
     [MQTT-4.3.2-2]     In the QoS 1 delivery protocol, the sender MUST send a PUBLISH packet containing this Packet Identifier with QoS 1 and DUP flag set to 0.
     [MQTT-4.3.2-3]     In the QoS 1 delivery protocol, the sender MUST treat the PUBLISH packet as “unacknowledged” until it has received the corresponding PUBACK packet from the receiver.
     [MQTT-4.3.2-4]     In the QoS 1 delivery protocol, the receiver MUST respond with a PUBACK packet containing the Packet Identifier from the incoming PUBLISH packet, having accepted ownership of the Application Message.
     [MQTT-4.3.2-5]     In the QoS 1 delivery protocol, the receiver after it has sent a PUBACK packet the receiver MUST treat any incoming PUBLISH packet that contains the same Packet Identifier as being a new publication, irrespective of the setting of its DUP flag.
     [MQTT-4.3.3-1]     In the QoS 2 delivery protocol, the sender MUST assign an unused Packet Identifier when it has a new Application Message to publish.
     [MQTT-4.3.3-2]     In the QoS 2 delivery protocol, the sender MUST send a PUBLISH packet containing this Packet Identifier with QoS 2 and DUP flag set to 0.
     [MQTT-4.3.3-3]     In the QoS 2 delivery protocol, the sender MUST treat the PUBLISH packet as “unacknowledged” until it has received the corresponding PUBREC packet from the receiver.
     [MQTT-4.3.3-4]     In the QoS 2 delivery protocol, the sender MUST send a PUBREL packet when it receives a PUBREC packet from the receiver with a Reason Code value less than 0x80. This PUBREL packet MUST contain the same Packet Identifier as the original PUBLISH packet.
     [MQTT-4.3.3-5]     In the QoS 2 delivery protocol, the sender MUST treat the PUBREL packet as “unacknowledged” until it has received the corresponding PUBCOMP packet from the receiver.
     [MQTT-4.3.3-6]     In the QoS 2 delivery protocol, the sender MUST NOT re-send the PUBLISH once it has sent the corresponding PUBREL packet.
     [MQTT-4.3.3-7]     In the QoS 2 delivery protocol, the sender MUST NOT apply Publication expiry if a PUBLISH packet has been sent.
     [MQTT-4.3.3-8]     In the QoS 2 delivery protocol, the receiver MUST respond with a PUBREC containing the Packet Identifier from the incoming PUBLISH packet, having accepted ownership of the Application Message.
     [MQTT-4.3.3-9]     In the QoS 2 delivery protocol, the receiver if it has sent a PUBREC with a Reason Code of 0x80 or greater, the receiver MUST treat any subsequent PUBLISH packet that contains that Packet Identifier as being a new publication.
     [MQTT-4.3.3-10]     In the QoS 2 delivery protocol, the receiver until it has received the corresponding PUBREL packet, the receiver MUST acknowledge any subsequent PUBLISH packet with the same Packet Identifier by sending a PUBREC. It MUST NOT cause duplicate messages to be delivered to any onward recipients in this case.
     [MQTT-4.3.3-11]     In the QoS 2 delivery protocol, the receiver MUST respond to a PUBREL packet by sending a PUBCOMP packet containing the same Packet Identifier as the PUBREL.
     [MQTT-4.3.3-12]     In the QoS 2 delivery protocol, the receiver After it has sent a PUBCOMP, the receiver MUST treat any subsequent PUBLISH packet that contains that Packet Identifier as being a new publication.
     [MQTT-4.3.3-13]     In the QoS 2 delivery protocol, the receiver MUST continue the QoS 2 acknowledgement sequence even if it has applied Publication expiry.
     [MQTT-4.4.0-1]     When a Client reconnects with Clean Start set to 0 and a session is present, both the Client and Server MUST resend any unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using their original Packet Identifiers. This is the only circumstance where a Client or Server is REQUIRED to resend messages. Clients and Servers MUST NOT resend messages at any other time.
     [MQTT-4.4.0-2]     If PUBACK or PUBREC is received containing a Reason Code of 0x80 or greater the corresponding PUBLISH packet is treated as acknowledged, and MUST NOT be retransmitted.
     [MQTT-4.5.0-1]     When a Server takes ownership of an incoming Application Message it MUST add it to the Session State for those Clients that have matching Subscriptions.
     [MQTT-4.5.0-2]     The Client MUST acknowledge any Publish packet it receives according to the applicable QoS rules regardless of whether it elects to process the Application Message that it contains.
     [MQTT-4.6.0-1]     When the Client re-sends any PUBLISH packets, it MUST re-send them in the order in which the original PUBLISH packets were sent (this applies to QoS 1 and QoS 2 messages).
     [MQTT-4.6.0-2]     The Client MUST send PUBACK packets in the order in which the corresponding PUBLISH packets were received (QoS 1 messages).
     [MQTT-4.6.0-3]     The Client MUST send PUBREC packets in the order in which the corresponding PUBLISH packets were received (QoS 2 messages).
     [MQTT-4.6.0-4]     The Client MUST send PUBREL packets in the order in which the corresponding PUBREC packets were received (QoS 2 messages).
     [MQTT-4.6.0-5]     When a Server processes a message that has been published to an Ordered Topic, it MUST send PUBLISH packets to consumers (for the same Topic and QoS) in the order that they were received from any given Client.
     [MQTT-4.6.0-6]     A Server MUST treat every, Topic as an Ordered Topic when it is forwarding messages on Non-Shared Subscriptions.
     [MQTT-4.7.0-1]     The wildcard characters can be used in Topic Filters, but MUST NOT be used within a Topic Name.
     [MQTT-4.7.1-1]     The multi-level wildcard character MUST be specified either on its own or following a topic level separator. In either case it MUST be the last character specified in the Topic Filter.
     [MQTT-4.7.1-2]     The single-level wildcard can be used at any level in the Topic Filter, including first and last levels. Where it is used, it MUST occupy an entire level of the filter.
     [MQTT-4.7.2-1]     The Server MUST NOT match Topic Filters starting with a wildcard character (# or +) with Topic Names beginning with a $ character.
     [MQTT-4.7.3-1]     All Topic Names and Topic Filters MUST be at least one character long.
     [MQTT-4.7.3-2]     Topic Names and Topic Filters MUST NOT include the null character (Unicode U+0000).
     [MQTT-4.7.3-3]     Topic Names and Topic Filters are UTF-8 Encoded Strings; they MUST NOT encode to more than 65,535 bytes.
     [MQTT-4.7.3-4]     When it performs subscription matching the Server MUST NOT perform any normalization of Topic Names or Topic Filters, or any modification or substitution of unrecognized characters.
     [MQTT-4.8.2-1]     A Shared Subscription's Topic Filter MUST start with $share/ and MUST contain a ShareName that is at least one character long.
     [MQTT-4.8.2-2]     The ShareName MUST NOT contain the characters "/", "+" or "#", but MUST be followed by a "/" character. This "/" character MUST be followed by a Topic Filter.
     [MQTT-4.8.2-3]     The Server MUST respect the granted QoS for the Clients subscription.
     [MQTT-4.8.2-4]     The Server MUST complete the delivery of the message to that Client when it reconnects.
     [MQTT-4.8.2-5]     If the Clients Session terminates before the Client reconnects, the Server MUST NOT send the Application Message to any other subscribed Client.
     [MQTT-4.8.2-6]     If a Client responds with a PUBACK or PUBREC containing a Reason Code of 0x80 or greater to a PUBLISH packet from the Server, the Server MUST discard the Application Message and not attempt to send it to any other Subscriber.
     [MQTT-4.9.0-1]     The Client or Server MUST set its initial send quota to a non-zero value not exceeding the Receive Maximum.
     [MQTT-4.9.0-2]     Each time the Client or Server sends a PUBLISH packet at QoS > 0, it decrements the send quota. If the send quota reaches zero, the Client or Server MUST NOT send any more PUBLISH packets with QoS > 0.
     [MQTT-4.9.0-3]     The Client and Server MUST continue to process and respond to all other MQTT Control Packets even if the quota is zero.
     [MQTT-4.12.0-1]     If the Server does not support the Authentication Method supplied by the Client, it MAY send a CONNACK with a Reason Code of 0x8C (Bad authentication method) or 0x87 (Not Authorized) as described in section 4.13 and MUST close the Network Connection.
     [MQTT-4.12.0-2]     If the Server requires additional information to complete the authorization, it can send an AUTH packet to the Client. This packet MUST contain a Reason Code of 0x18 (Continue authentication).
     [MQTT-4.12.0-3]     The Client responds to an AUTH packet from the Server by sending a further AUTH packet. This packet MUST contain a Reason Code of 0x18 (Continue authentication).
     [MQTT-4.12.0-4]     The Server can reject the authentication at any point in this process. It MAY send a CONNACK with a Reason Code of 0x80 or above as described in section 4.13, and MUST close the Network Connection.
     [MQTT-4.12.0-5]     If the initial CONNECT packet included an Authentication Method property then all AUTH packets, and any successful CONNACK packet MUST include an Authentication Method Property with the same value as in the CONNECT packet.
     [MQTT-4.12.0-6]     If the Client does not include an Authentication Method in the CONNECT, the Server MUST NOT send an AUTH packet, and it MUST NOT send an Authentication Method in the CONNACK packet.
     [MQTT-4.12.0-7]     If the Client does not include an Authentication Method in the CONNECT, the Client MUST NOT send an AUTH packet to the Server.
     [MQTT-4.12.1-1]     If the Client supplied an Authentication Method in the CONNECT packet it can initiate a re-authentication at any time after receiving a CONNACK.. It does this by sending an AUTH packet with a Reason Code of 0x19 (Re-authentication). The Client MUST set the Authentication Method to the same value as the Authentication Method originally used to authenticate the Network Connection.
     [MQTT-4.12.1-2]     If the re-authentication fails, the Client or Server SHOULD send DISCONNECT with an appropriate Reason Code and MUST close the Network Connection.
     [MQTT-4.13.1-1]     When a Server detects a Malformed Packet or Protocol Error, and a Reason Code is given in the specification, it MUST close the Network Connection.
     [MQTT-4.13.2-1]     The CONNACK and DISCONNECT packets allow a Reason Code of 0x80 or greater to indicate that the Network Connection will be closed. If a Reason Code of 0x80 or greater is specified, then the Network Connection MUST be closed whether or not the CONNACK or DISCONNECT is sent.
     [MQTT-6.0.0-1]     MQTT Control Packets MUST be sent in WebSocket binary data frames. If any other type of data frame is received the recipient MUST close the Network Connection.
     [MQTT-6.0.0-2]     A single WebSocket data frame can contain multiple or partial MQTT Control Packets. The receiver MUST NOT assume that MQTT Control Packets are aligned on WebSocket frame boundaries.
     [MQTT-6.0.0-3]     The Client MUST include “mqtt” in the list of WebSocket Sub Protocols it offers.
     [MQTT-6.0.0-4]     The WebSocket Subprotocol name selected and returned by the Server MUST be “mqtt”.
     [MQTT-7.0.0-1]     A Server that both accepts inbound connections and establishes outbound connections to other Servers MUST conform as both an MQTT Client and MQTT Server.
     [MQTT-7.0.0-2]     Conformant implementations MUST NOT require the use of any extensions defined outside of this specification in order to interoperate with any other conformant implementation.
     [MQTT-7.1.1-1]     A conformant Server MUST support the use of one or more underlying transport protocols that provide an ordered, lossless, stream of bytes from the Client to Server and Server to Client.
     [MQTT-7.1.2-1]     A conformant Client MUST support the use of one or more underlying transport protocols that provide an ordered, lossless, stream of bytes from the Client to Server and Server to Client.
     */

    static let clientFeatures : Set = [
        "MQTT-7.1.2-1"
    ]
    static let clientEvents: Set = [
        "MQTT-3.3.1-4"
    ]
    static let serverFeatures: Set = [
        "MQTT-3.2.0-2"
    ]
    static let serverEvents: Set = [
        "MQTT-3.1.0-1", "MQTT-3.1.0-2"
    ]

    var targets : [String: Int]

    init() {
        self.targets = [String: Int]()
    }

    func log(target: String) {
        var count = self.targets[target]
        if (count == nil) {
            count = 0
        }
        self.targets[target] = count! + 1
    }

    func stats() {
        var checkedClientFeatures: Set<String> = []
        var checkedClientEvents: Set<String> = []
        var checkedServerFeatures: Set<String> = []
        var checkedServerEvents: Set<String> = []

        for (target, count) in self.targets {
            MqttSessions.statMessage(topic: "$SYS/compliance/target/\(target)", payload: "\(count)")
            if MqttCompliance.clientFeatures.contains(target) {
                checkedClientFeatures.insert(target)
            }
            if MqttCompliance.clientEvents.contains(target) {
                checkedClientEvents.insert(target)
            }
            if MqttCompliance.serverFeatures.contains(target) {
                checkedServerFeatures.insert(target)
            }
            if MqttCompliance.serverEvents.contains(target) {
                checkedServerEvents.insert(target)
            }
        }

        let uncheckedClientFeatures = MqttCompliance.clientFeatures.subtracting(checkedClientFeatures)

        MqttSessions.statMessage(topic: "$SYS/compliance/clientFeatures/checked", payload: "\(checkedClientFeatures)")
        MqttSessions.statMessage(topic: "$SYS/compliance/clientFeatures/unchecked", payload: "\(uncheckedClientFeatures)")
        MqttSessions.statMessage(topic: "$SYS/compliance/clientFeatures/coverage", payload: "\(checkedClientFeatures.count / MqttCompliance.clientFeatures.count)")

        let uncheckedClientEvents = MqttCompliance.clientEvents.subtracting(checkedClientEvents)
        MqttSessions.statMessage(topic: "$SYS/compliance/clientEvents/checked", payload: "\(checkedClientEvents)")
        MqttSessions.statMessage(topic: "$SYS/compliance/clientEvents/unchecked", payload: "\(uncheckedClientEvents)")
        MqttSessions.statMessage(topic: "$SYS/compliance/clientEvents/coverage", payload: "\(checkedClientEvents.count / MqttCompliance.clientEvents.count)")
        
        let uncheckedServerFeatures = MqttCompliance.serverFeatures.subtracting(checkedServerFeatures)
        MqttSessions.statMessage(topic: "$SYS/compliance/serverFeatures/checked", payload: "\(checkedServerFeatures)")
        MqttSessions.statMessage(topic: "$SYS/compliance/serverFeatures/unchecked", payload: "\(uncheckedServerFeatures)")
        MqttSessions.statMessage(topic: "$SYS/compliance/serverFeatures/coverage", payload: "\(checkedServerFeatures.count / MqttCompliance.serverFeatures.count)")
        
        let uncheckedServerEvents = MqttCompliance.clientFeatures.subtracting(checkedServerEvents)
        MqttSessions.statMessage(topic: "$SYS/compliance/serverEvents/checked", payload: "\(checkedServerEvents)")
        MqttSessions.statMessage(topic: "$SYS/compliance/serverEvents/unchecked", payload: "\(uncheckedServerEvents)")
        MqttSessions.statMessage(topic: "$SYS/compliance/serverEvents/coverage", payload: "\(checkedServerEvents.count / MqttCompliance.serverEvents.count)")
        
    }
}
