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
package hu.icellmobilsoft.reactive.messaging.redis.streams.metadata;

import java.util.HashMap;
import java.util.Map;

/**
 * General microprofile reactive streams metadata for Redis stream connector.
 * 
 * @since 1.0.0
 * @author mark.petrenyi
 */
public class RedisStreamMetadata {

    private final Map<String, String> additionalFields = new HashMap<>();
    private String flowIdExtension;

    /**
     * Default constructor
     */
    public RedisStreamMetadata() {
        // NOTE: For jdk 21.
    }

    /**
     * Gets the additional fields beside message payload.
     *
     * @return a map containing the additional fields
     */
    public Map<String, String> getAdditionalFields() {
        return additionalFields;
    }

    /**
     * Put key and value parameter into additionalFields map
     *
     * @param key
     *            additionalField's key
     * @param value
     *            value for key in additionalField
     * @return actual object
     */
    public RedisStreamMetadata withAdditionalField(String key, String value) {
        additionalFields.put(key, value);
        return this;
    }

    /**
     * Put key and value parameter into additionalFields map
     *
     * @param additionalFields
     *            additional fields to add
     * @return actual object
     */
    public RedisStreamMetadata withAdditionalFields(Map<String, String> additionalFields) {
        this.additionalFields.putAll(additionalFields);
        return this;
    }

    /**
     * Returns the flow id, which will be appended to the SID
     * 
     * @return the flow id, which will be appended to the SID
     */
    public String getFlowIdExtension() {
        return flowIdExtension;
    }

    /**
     * Sets the flow id, which will be appended to the SID
     * 
     * @param flowIdExtension
     *            the flow id, which will be appended to the SID
     */
    public void setFlowIdExtension(String flowIdExtension) {
        this.flowIdExtension = flowIdExtension;
    }
}
