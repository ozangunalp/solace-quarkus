package com.solace.quarkus.runtime;

import java.util.Map;
import java.util.function.Function;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;

import com.solace.quarkus.MessagingServiceClientCustomizer;
import com.solacesystems.jcsmp.*;

import io.quarkus.arc.SyntheticCreationalContext;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class SolaceRecorder {
    private JCSMPSession service;
    private static final TypeLiteral<Instance<MessagingServiceClientCustomizer>> CUSTOMIZER = new TypeLiteral<>() {
    };

    public Function<SyntheticCreationalContext<JCSMPSession>, JCSMPSession> init(SolaceConfig config,
            ShutdownContext shutdown) {
        return new Function<>() {
            @Override
            public JCSMPSession apply(SyntheticCreationalContext<JCSMPSession> context) {
                JCSMPProperties properties = new JCSMPProperties();
                properties.setProperty(JCSMPProperties.HOST, config.host());
                properties.setProperty(JCSMPProperties.VPN_NAME, config.vpn());
                for (Map.Entry<String, String> entry : config.extra().entrySet()) {
                    properties.setProperty(entry.getKey(), entry.getValue());
                    if (!entry.getKey().startsWith("solace.messaging.")) {
                        properties.setProperty("solace.messaging." + entry.getKey(), entry.getValue());
                    }
                }

                Instance<MessagingServiceClientCustomizer> reference = context.getInjectedReference(CUSTOMIZER);
                OidcProvider oidcProvider = context.getInjectedReference(OidcProvider.class);

                String authScheme = (String) properties.getProperty(JCSMPProperties.AUTHENTICATION_SCHEME);

                if (oidcProvider != null && authScheme != null && "AUTHENTICATION_SCHEME_OAUTH2".equals(authScheme)) {
                    properties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2,
                            oidcProvider.getToken().getAccessToken());
                }

                try {
                    if (reference.isUnsatisfied()) {
                        service = JCSMPFactory.onlyInstance().createSession(properties, null,
                                sessionEventArgs -> SolaceRecorder.this.handleEvent(sessionEventArgs, oidcProvider,
                                        authScheme));
                    } else {
                        if (!reference.isResolvable()) {
                            throw new IllegalStateException("Multiple MessagingServiceClientCustomizer instances found");
                        } else {
                            service = JCSMPFactory.onlyInstance().createSession(reference.get().customize(properties), null,
                                    sessionEventArgs -> SolaceRecorder.this.handleEvent(sessionEventArgs, oidcProvider,
                                            authScheme));
                        }
                    }

                    if ("AUTHENTICATION_SCHEME_OAUTH2".equals(authScheme)) {
                        oidcProvider.init(service);
                    }
                    var tmp = service;
                    shutdown.addLastShutdownTask(() -> {
                        if (!tmp.isClosed()) {
                            tmp.closeSession();
                        }
                    });

                    service.connect();

                } catch (JCSMPException e) {
                    throw new IllegalStateException(e);
                }
                return service;
            }
        };
    }

    private void handleEvent(SessionEventArgs sessionEventArgs, OidcProvider oidcProvider, String authScheme) {
        if (sessionEventArgs.getEvent().equals(SessionEvent.RECONNECTING)) {
            Log.info("Reconnecting to Solace broker due to " + sessionEventArgs.getInfo());
            if (oidcProvider != null && authScheme != null && "AUTHENTICATION_SCHEME_OAUTH2".equals(authScheme)) {
                try {
                    service.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, oidcProvider.getToken().getAccessToken());
                } catch (JCSMPException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}
