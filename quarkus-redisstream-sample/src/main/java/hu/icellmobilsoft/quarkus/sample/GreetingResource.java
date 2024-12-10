package hu.icellmobilsoft.quarkus.sample;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/hello")
public class GreetingResource {

    private final MyMessagingApplication eventAction;

    @Inject
    public GreetingResource(MyMessagingApplication eventAction) {
        this.eventAction = eventAction;
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        eventAction.sendMessages();
        return "Hello from Quarkus REST";
    }
}
