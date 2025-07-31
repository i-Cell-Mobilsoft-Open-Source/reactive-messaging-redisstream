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
package hu.icellmobilsoft.reactive.messaging.redisstream;

/**
 * Interface containing constants used for logging purposes.
 * <p>
 * This interface defines values that are commonly used in logging contexts, enabling consistent management of logging metadata, such as session IDs,
 * across application components. It is designed to be used in conjunction with Mapped Diagnostic Context (MDC) for logging enrichment and tracing.
 * 
 * @author martin.nagy
 * @since 1.1.0
 */
public interface LogConstants {

    /**
     * Constant representing the session ID key for MDC (Mapped Diagnostic Context) logging.
     * <p>
     * This key is used to identify and link logs with a specific session or request context. It is utilized in MDC-related operations to set or
     * retrieve the session identifier from incoming or outgoing logging contexts.
     */
    String LOG_SESSION_ID = "extSessionId";

}
