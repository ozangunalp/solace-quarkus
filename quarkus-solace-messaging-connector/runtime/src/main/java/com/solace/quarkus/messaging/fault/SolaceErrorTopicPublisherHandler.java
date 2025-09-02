package com.solace.quarkus.messaging.fault;

import com.solace.quarkus.messaging.i18n.SolaceLogging;
import com.solace.quarkus.messaging.incoming.SolaceInboundMessage;
import com.solacesystems.jcsmp.*;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;

import java.util.concurrent.CountDownLatch;

class SolaceErrorTopicPublisherHandler implements JCSMPStreamingPublishCorrelatingEventHandler{

    private final JCSMPSession solace;
    private final XMLMessageProducer publisher;
    private final OutboundErrorMessageMapper outboundErrorMessageMapper;
    private UniEmitter<Object> uniEmitter;

    CountDownLatch latch = new CountDownLatch(1);

    public SolaceErrorTopicPublisherHandler(JCSMPSession solace) {
        this.solace = solace;

        try {
            publisher = solace.getMessageProducer(this);
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
                this.uniEmitter = e;
                // always wait for error message publish receipt to ensure it is successfully spooled on broker.
                publisher.send(outboundMessage, JCSMPFactory.onlyInstance().createTopic(errorTopic));
            } catch (Throwable t) {
                e.fail(t);
            }
        }).onFailure().invoke(t -> SolaceLogging.log.publishException(errorTopic, t));
    }

    @Override
    public void responseReceivedEx(Object o) {
        uniEmitter.complete("SUCCESS");
    }

    @Override
    public void handleErrorEx(Object o, JCSMPException e, long l) {
        uniEmitter.fail(e);
    }
}
