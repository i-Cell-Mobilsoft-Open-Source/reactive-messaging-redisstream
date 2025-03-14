/*-
 * #%L
 * reactive-messaging-redisstream
 * %%
 * Copyright (C) 2025 i-Cell Mobilsoft Zrt.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package hu.icellmobilsoft.quarkus.sample;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.MDC;

import java.util.UUID;

/**
 * Rest endpoint for /hello
 */
@Path("/hello")
@ApplicationScoped
public class GreetingResource {

    private final MyMessagingApplication eventAction;

    private int counter = 0;

    /**
     * Constructor with injections
     *
     * @param eventAction injected eventAction
     */
    @Inject
    public GreetingResource(MyMessagingApplication eventAction) {
        this.eventAction = eventAction;
    }

    /**
     * GET /hello
     * @return "Hello Quarkus" with application counter
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        MDC.put("extSessionId", UUID.randomUUID().toString());
        String message = "Hello Quarkus " + counter++;
        eventAction.sendMessage(message);
        return message;
    }

    /**
     * GET /error
     * @return "error" response
     */
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
