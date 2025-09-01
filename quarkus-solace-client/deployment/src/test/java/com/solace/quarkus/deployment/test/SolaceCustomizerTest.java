package com.solace.quarkus.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessageProducer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.solace.quarkus.MessagingServiceClientCustomizer;

import io.quarkus.test.QuarkusUnitTest;

public class SolaceCustomizerTest {

    @RegisterExtension
    static final QuarkusUnitTest unitTest = new QuarkusUnitTest()
            .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class)
                    .addClasses(MyCustomizer.class));

    @Inject
    MyCustomizer customizer;
    @Inject
    JCSMPSession solace;

    @Test
    public void test() {
        try {
            XMLMessageProducer prod = solace.getMessageProducer(null);
            assertThat(customizer.called()).isTrue();
            prod.close();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

//        DirectMessagePublisher publisher = solace.createDirectMessagePublisherBuilder()
//                .build().start();
    }

    @Singleton
    public static class MyCustomizer implements MessagingServiceClientCustomizer {

        AtomicBoolean called = new AtomicBoolean();

        @Override
        public JCSMPProperties customize(JCSMPProperties builder) {
            called.set(true);
            builder.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES_RECONNECT_RETRIES, 0);
            return builder;
        }

        public boolean called() {
            return called.get();
        }
    }
}
