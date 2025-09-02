package com.solace.quarkus.messaging.fault;

import java.util.Properties;

import com.solace.quarkus.messaging.converters.SolaceMessageUtils;
import com.solacesystems.jcsmp.*;

class OutboundErrorMessageMapper {

    public BytesXMLMessage mapError(JCSMPSession solace, BytesXMLMessage inputMessage,
            boolean dmqEligible, Long timeToLive) {
        Properties extendedMessageProperties = new Properties();
        BytesXMLMessage outboundMessage = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
        outboundMessage.setDMQEligible(dmqEligible);
        if (timeToLive != null) {
            outboundMessage.setTimeToLive(timeToLive);
        }
        outboundMessage.writeAttachment(SolaceMessageUtils.getPayloadAsBytes(inputMessage));
        return outboundMessage;
    }
}
