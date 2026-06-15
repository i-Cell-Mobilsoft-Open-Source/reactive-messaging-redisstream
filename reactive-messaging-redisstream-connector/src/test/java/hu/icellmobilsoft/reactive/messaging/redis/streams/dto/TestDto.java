/*-
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
package hu.icellmobilsoft.reactive.messaging.redis.streams.dto;

import java.util.Objects;

/**
 * Test DTO for verifying JSON-B serialization/deserialization support.
 *
 * @author kornel.danko
 * @since 1.4.0
 */
public class TestDto {

    private String name;
    private int value;

    /**
     * Default constructor for JSON-B.
     */
    public TestDto() {
    }

    /**
     * Constructs a TestDto with the given name and value.
     *
     * @param name
     *            the name
     * @param value
     *            the value
     */
    public TestDto(String name, int value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TestDto testDto = (TestDto) o;
        return value == testDto.value && Objects.equals(name, testDto.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return "TestDto{name='" + name + "', value=" + value + "}";
    }
}
