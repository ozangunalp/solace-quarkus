package com.solace.quarkus.messaging.fault;

import com.solace.quarkus.messaging.i18n.SolaceLogging;
import com.solace.quarkus.messaging.incoming.SolaceInboundMessage;
import com.solacesystems.jcsmp.*;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;

import java.util.concurrent.CountDownLatch;

class SolaceErrorTopicPublisherHandler {

    private final JCSMPSession solace;
    private final XMLMessageProducer publisher;
    private final OutboundErrorMessageMapper outboundErrorMessageMapper;
    public SolaceErrorTopicPublisherHandler(JCSMPSession solace) {
        this.solace = solace;
        try {
            publisher = solace.getMessageProducer(new PublisherEventHandler());
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }
        outboundErrorMessageMapper = new OutboundErrorMessageMapper();
    }

    public Uni<Object> handle(SolaceInboundMessage<?> message,
            String errorTopic,
            boolean dmqEligible, Long timeToLive) {
        BytesXMLMessage outboundMessage = outboundErrorMessageMapper.mapError(this.solace,
                message.getMessage(),
                dmqEligible, timeToLive);
        //        }
        return Uni.createFrom().<Object> emitter(e -> {
            try {
                // always wait for error message publish receipt to ensure it is successfully spooled on broker.
                outboundMessage.setCorrelationKey("test");
                publisher.send(outboundMessage, JCSMPFactory.onlyInstance().createTopic(errorTopic));
            } catch (Throwable t) {
                e.fail(t);
            }
        }).onFailure().invoke(t -> SolaceLogging.log.publishException(errorTopic, t));
    }

    static class PublisherEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {
        @Override
        public void responseReceivedEx(Object o) {
            if (o instanceof UniEmitter) {
                ((UniEmitter<?>) o).complete(null);
            }
        }

        @Override
        public void handleErrorEx(Object o, JCSMPException e, long l) {
            if (o instanceof UniEmitter) {
                ((UniEmitter<?>) o).fail(e);
            }
        }
    }
}
