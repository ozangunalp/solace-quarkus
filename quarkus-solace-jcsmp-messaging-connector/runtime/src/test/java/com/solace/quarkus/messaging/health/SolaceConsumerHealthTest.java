package com.solace.quarkus.messaging.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import com.solace.quarkus.messaging.base.WeldTestBase;
import com.solace.quarkus.messaging.converters.SolaceMessageUtils;
import com.solace.quarkus.messaging.incoming.SolaceInboundMessage;
import com.solacesystems.jcsmp.*;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class SolaceConsumerHealthTest extends WeldTestBase {

    @Test
    void solaceConsumerHealthCheck() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.in.connector", "quarkus-solace-jcsmp")
                .with("mp.messaging.incoming.in.consumer.queue.name", queue)
                .with("mp.messaging.incoming.in.consumer.queue.add-additional-subscriptions", "true")
                .with("mp.messaging.incoming.in.consumer.queue.missing-resource-creation-strategy", "create-on-start")
                .with("mp.messaging.incoming.in.consumer.subscriptions", topic);

        // Run app that consumes messages
        MyConsumer app = runApplication(config, MyConsumer.class);

        await().until(() -> isStarted() && isReady());

        // Produce messages
        XMLMessageProducer publisher = null;
        try {
            publisher = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
                @Override
                public void responseReceivedEx(Object o) {

                }

                @Override
                public void handleErrorEx(Object o, JCSMPException e, long l) {

                }
            });
            Topic tp = JCSMPFactory.onlyInstance().createTopic(topic);
            for (int i = 1; i <= 5; i++) {
                sendTextMessage(Integer.toString(i), publisher, tp);
            }
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        await().until(() -> isAlive());

        HealthReport startup = getHealth().getStartup();
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(startup.isOk()).isTrue();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(startup.getChannels()).hasSize(1);
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);

    }

    @Test
    void solaceConsumerLivenessCheck() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.in.connector", "quarkus-solace-jcsmp")
                .with("mp.messaging.incoming.in.consumer.queue.name", queue)
                .with("mp.messaging.incoming.in.consumer.queue.add-additional-subscriptions", "true")
                .with("mp.messaging.incoming.in.consumer.queue.missing-resource-creation-strategy", "create-on-start")
                .with("mp.messaging.incoming.in.consumer.subscriptions", topic);

        // Run app that consumes messages
        MyErrorConsumer app = runApplication(config, MyErrorConsumer.class);

        await().until(() -> isStarted() && isReady());

        // Produce messages
        XMLMessageProducer publisher = null;
        Topic tp = JCSMPFactory.onlyInstance().createTopic(topic);
        try {
            publisher = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
                @Override
                public void responseReceivedEx(Object o) {

                }

                @Override
                public void handleErrorEx(Object o, JCSMPException e, long l) {

                }
            });

            for (int i = 1; i <= 2; i++) {
                sendTextMessage(Integer.toString(i), publisher, tp);
            }
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        await().until(() -> isAlive());

        HealthReport startup = getHealth().getStartup();
        HealthReport liveness = getHealth().getLiveness();
        HealthReport readiness = getHealth().getReadiness();

        assertThat(startup.isOk()).isTrue();
        assertThat(liveness.isOk()).isTrue();
        assertThat(readiness.isOk()).isTrue();
        assertThat(startup.getChannels()).hasSize(1);
        assertThat(liveness.getChannels()).hasSize(1);
        assertThat(readiness.getChannels()).hasSize(1);

        sendTextMessage("3", publisher, tp);

        await().until(() -> {
            HealthReport healthReport = getHealth().getLiveness();
            return (healthReport.isOk() == false && !healthReport.getChannels().get(0).getMessage().isEmpty());
        });

        sendTextMessage("4", publisher, tp);
        sendTextMessage("5", publisher, tp);
        await().until(() -> getHealth().getLiveness().isOk() == true);
    }

    @ApplicationScoped
    static class MyConsumer {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("in")
        void in(BytesXMLMessage msg) {
            received.add(SolaceMessageUtils.getPayloadAsString(msg));
        }

        public List<String> getReceived() {
            return received;
        }
    }

    @ApplicationScoped
    static class MyErrorConsumer {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("in")
        CompletionStage<Void> in(SolaceInboundMessage<byte[]> msg) {
            String payload = new String(msg.getPayload(), StandardCharsets.UTF_8);
            if (payload.equals("3")) {
                return msg.nack(new IllegalArgumentException("Nacking message with payload 3"));
            }

            return msg.ack();
        }
    }

    private void sendTextMessage(String payload, XMLMessageProducer publisher, Topic tp) {
        TextMessage textMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        textMessage.setText(payload);
        textMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
        try {
            publisher.send(textMessage, tp);
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }
    }
}
