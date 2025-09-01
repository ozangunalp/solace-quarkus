package com.solace.quarkus.runtime;

import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;

import com.solace.quarkus.MessagingServiceClientCustomizer;
import com.solacesystems.jcsmp.*;

import io.quarkus.arc.SyntheticCreationalContext;
import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class SolaceRecorder {

    private static final TypeLiteral<Instance<MessagingServiceClientCustomizer>> CUSTOMIZER = new TypeLiteral<>() {
    };

    public Function<SyntheticCreationalContext<JCSMPSession>, JCSMPSession> init(SolaceConfig config,
            ShutdownContext shutdown) {
        return new Function<>() {
            @Override
            public JCSMPSession apply(SyntheticCreationalContext<JCSMPSession> context) {
                Properties properties = new Properties();
                properties.put(JCSMPProperties.HOST, config.host());
                properties.put(JCSMPProperties.VPN_NAME, config.vpn());
                for (Map.Entry<String, String> entry : config.extra().entrySet()) {
                    properties.put(entry.getKey(), entry.getValue());
                    if (!entry.getKey().startsWith("solace.messaging.")) {
                        properties.put("solace.messaging." + entry.getKey(), entry.getValue());
                    }
                }

                Instance<MessagingServiceClientCustomizer> reference = context.getInjectedReference(CUSTOMIZER);
                OidcProvider oidcProvider = context.getInjectedReference(OidcProvider.class);

                String authScheme = (String) properties.get(JCSMPProperties.AUTHENTICATION_SCHEME);

                if (oidcProvider != null && authScheme != null && "AUTHENTICATION_SCHEME_OAUTH2".equals(authScheme)) {
                    properties.put(JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2,
                            oidcProvider.getToken().getAccessToken());
                }

                JCSMPProperties builder = JCSMPProperties
                        .fromProperties(properties);
                JCSMPSession service;
                if (reference.isUnsatisfied()) {
                    try {
                        service = JCSMPFactory.onlyInstance().createSession(builder);
                    } catch (InvalidPropertiesException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    if (!reference.isResolvable()) {
                        throw new IllegalStateException("Multiple MessagingServiceClientCustomizer instances found");
                    } else {
                        try {
                            service = JCSMPFactory.onlyInstance().createSession(reference.get().customize(builder));
                        } catch (InvalidPropertiesException e) {
                            throw new RuntimeException(e);
                        }
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

                // Update access token on reconnect to make sure invalid token is not sent. This can happen when a reconnection happens event before scheduled token expiry.
                //                service.addReconnectionAttemptListener(serviceEvent -> {
                //                    Log.info("Reconnecting to Solace broker due to " + serviceEvent.getMessage());
                //                    if (oidcProvider != null && authScheme != null && "AUTHENTICATION_SCHEME_OAUTH2".equals(authScheme)) {
                //                        service.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, oidcProvider.getToken().getAccessToken());
                //                    }
                //                });

                try {
                    service.connect();
                } catch (JCSMPException e) {
                    throw new RuntimeException(e);
                }

                return service;
            }
        };
    }

}
