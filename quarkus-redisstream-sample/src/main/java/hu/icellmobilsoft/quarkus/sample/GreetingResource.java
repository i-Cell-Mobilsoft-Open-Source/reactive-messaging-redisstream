package hu.icellmobilsoft.quarkus.sample;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

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
        String message = "Hello Quarkus " + counter++;
        eventAction.sendMessage(message);
        return message;
    }
}
