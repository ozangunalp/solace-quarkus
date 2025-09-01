package com.solace.quarkus;

import com.solacesystems.jcsmp.JCSMPProperties;
import jakarta.enterprise.context.ApplicationScoped;


@ApplicationScoped
public class SolaceCustomizer implements MessagingServiceClientCustomizer {
    @Override
    public JCSMPProperties customize(JCSMPProperties builder) {
        return builder;
    }
}
