package com.solace.quarkus.runtime;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.oidc.client.OidcClient;
import io.quarkus.oidc.client.Tokens;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;

import com.solace.messaging.MessagingService;

@ApplicationScoped
public class OidcProvider {

    @ConfigProperty(name = "solace.messaging.oidc.refresh.internal", defaultValue = "5s")
    Duration duration;

    @Inject
    OidcClient client;

    private volatile Tokens lastToken;
    private MessagingService service;

    Tokens getToken() {
        Tokens firstToken = client.getTokens().await().indefinitely();
        lastToken = firstToken;
        return firstToken;
    }

    void init(MessagingService service) {
        this.service = service;
    }

    void startup(@Observes StartupEvent event) {
        Multi.createFrom().ticks().every(duration)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .filter(aLong -> {
                    if (lastToken.isAccessTokenWithinRefreshInterval()) {
                        return true;
                    } else
                        return false;
                }).call(() -> client.refreshTokens(lastToken.getRefreshToken()).invoke(tokens -> {
                    lastToken = tokens;
                }))
                .invoke(() -> service.updateProperty("authentication.scheme", lastToken.getAccessToken()))
                .subscribe().with(aLong -> {});
    }
}
