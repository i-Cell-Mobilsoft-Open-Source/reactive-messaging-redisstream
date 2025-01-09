package hu.icellmobilsoft.quarkus.sample;

import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.jboss.logging.MDC;

@Path("/hello")
@ApplicationScoped
public class GreetingResource {

    private final MyMessagingApplication eventAction;
    private int counter = 0;

    @Inject
    public GreetingResource(MyMessagingApplication eventAction) {
        this.eventAction = eventAction;
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        MDC.put("extSessionId", UUID.randomUUID().toString());
        String message = "Hello Quarkus " + counter++;
        eventAction.sendMessage(message);
        return message;
    }

    @GET
    @Path("/error")
    @Produces(MediaType.TEXT_PLAIN)
    public String error() {
        MDC.put("extSessionId", UUID.randomUUID().toString());
        String message = "error";
        eventAction.sendMessage(message);
        return message;
    }
}
