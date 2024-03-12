package com.solace.quarkus.runtime;

import static com.solace.messaging.config.SolaceProperties.AuthenticationProperties.SCHEME_OAUTH2_ACCESS_TOKEN;

import java.time.Duration;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.solace.messaging.MessagingService;

import io.quarkus.logging.Log;
import io.quarkus.oidc.client.OidcClient;
import io.quarkus.oidc.client.OidcClients;
import io.quarkus.oidc.client.Tokens;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;

@ApplicationScoped
public class OidcProvider {

    @ConfigProperty(name = "quarkus.solace.oidc.refresh.interval", defaultValue = "60s")
    Duration duration;

    @ConfigProperty(name = "quarkus.solace.oidc.client-name")
    Optional<String> oidcClientName;

    @Inject
    OidcClients clients;

    private volatile Tokens lastToken;

    Tokens getToken() {
        OidcClient client = getClient();
        Tokens firstToken = client.getTokens().await().indefinitely();
        lastToken = firstToken;
        return firstToken;
    }

    void init(MessagingService service) {
        OidcClient client = getClient();
        Multi.createFrom().ticks().every(duration)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .filter(x -> lastToken == null
                        || lastToken.getRefreshTokenTimeSkew() == null
                        || lastToken.isAccessTokenWithinRefreshInterval())
                .call(() -> {
                    if (lastToken != null && lastToken.getRefreshToken() != null) {
                        Log.info("Refreshing access token for Solace connection");
                        return client.refreshTokens(lastToken.getRefreshToken()).invoke(tokens -> lastToken = tokens);
                    } else {
                        Log.info("Acquiring access token for Solace connection");
                        return client.getTokens().invoke(tokens -> lastToken = tokens);
                    }
                })
                .onFailure().call(t -> {
                    Log.error("Failed to acquire access token for Solace connection", t);
                    // ignore the refresh
                    return Uni.createFrom().voidItem();
                })
                .subscribe().with(x -> service.updateProperty(SCHEME_OAUTH2_ACCESS_TOKEN, lastToken.getAccessToken()));
    }

    OidcClient getClient() {
        return oidcClientName.map(clients::getClient)
                .orElseGet(clients::getClient);
    }

}
