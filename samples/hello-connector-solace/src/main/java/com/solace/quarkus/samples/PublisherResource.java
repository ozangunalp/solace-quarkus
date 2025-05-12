package com.solace.quarkus.samples;

import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import com.solace.quarkus.messaging.outgoing.SolaceOutboundMetadata;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;

@Path("/hello")
public class PublisherResource {

    @Channel("publisher-out")
    MutinyEmitter<Person> foobar;

    /**
     * Publishes to static topic configured in application.properties
     *
     * @param person
     * @return
     */
    @POST
    @Path("/publish")
    public Uni<Void> publish(Person person) {
        return foobar.send(person);
    }

    /**
     * Publishes to dynamic topic test/topic/<name-field-in-person-object and also add additional headers on message(ex:
     * ApplicationMessageID)
     *
     * @param person
     * @return
     */
    @POST
    @Path("/dynamictopic")
    public Uni<Void> publishToDynamicTopic(Person person) {

        SolaceOutboundMetadata outboundMetadata = SolaceOutboundMetadata.builder()
                .setApplicationMessageId("test").setDynamicDestination("test/topic/" + person.name)
                .createPubSubOutboundMetadata();
        Message<Person> personMessage = Message.of(person, Metadata.of(outboundMetadata));
        return foobar.sendMessage(personMessage);
    }

    @Incoming("publisher-out")
    @Outgoing("hello-out")
    public Multi<Message<Person>> transformToTopic(Multi<Message<Person>> person) {
        return person.map(m -> {
            var metadataBuilder = m.getMetadata(SolaceOutboundMetadata.class)
                    .map(PublisherResource::from)
                    .orElse(SolaceOutboundMetadata.builder());
            metadataBuilder.setDynamicDestination("hello/person");
            return m.addMetadata(metadataBuilder.createPubSubOutboundMetadata());
        });
    }

    /**
     * Creates a new SolaceOutboundMetadata object from the given SolaceOutboundMetadata object.
     * This could've been a copy constructor
     *
     * @param outboundMetadata
     * @return
     */
    static SolaceOutboundMetadata.PubSubOutboundMetadataBuilder from(SolaceOutboundMetadata outboundMetadata) {
        SolaceOutboundMetadata.PubSubOutboundMetadataBuilder builder = SolaceOutboundMetadata.builder();
        if (outboundMetadata.getApplicationMessageId() != null) {
            builder.setApplicationMessageId(outboundMetadata.getApplicationMessageId());
        }
        if (outboundMetadata.getDynamicDestination() != null) {
            builder.setDynamicDestination(outboundMetadata.getDynamicDestination());
        }
        if (outboundMetadata.getExpiration() != null) {
            builder.setExpiration(outboundMetadata.getExpiration());
        }
        if (outboundMetadata.getPriority() != null) {
            builder.setPriority(outboundMetadata.getPriority());
        }
        if (outboundMetadata.getSenderId() != null) {
            builder.setSenderId(outboundMetadata.getSenderId());
        }
        if (outboundMetadata.getProperties() != null) {
            builder.setProperties(outboundMetadata.getProperties());
        }
        if (outboundMetadata.getApplicationMessageType() != null) {
            builder.setApplicationMessageType(outboundMetadata.getApplicationMessageType());
        }
        if (outboundMetadata.getTimeToLive() != null) {
            builder.setTimeToLive(outboundMetadata.getTimeToLive());
        }
        if (outboundMetadata.getClassOfService() != null) {
            builder.setClassOfService(outboundMetadata.getClassOfService());
        }
        if (outboundMetadata.getPartitionKey() != null) {
            builder.setPartitionKey(outboundMetadata.getPartitionKey());
        }
        if (outboundMetadata.getCorrelationId() != null) {
            builder.setCorrelationId(outboundMetadata.getCorrelationId());
        }
        return builder;
    }

}
