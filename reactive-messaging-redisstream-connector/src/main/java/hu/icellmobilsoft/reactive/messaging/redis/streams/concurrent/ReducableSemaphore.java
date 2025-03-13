/*-
 * #%L
 * reactive-redisstream-messaging
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
package hu.icellmobilsoft.reactive.messaging.redis.streams.concurrent;

import java.util.concurrent.Semaphore;

/**
 * A semaphore that allows permits to be reduced. This class extends the standard Semaphore class to provide additional functionality for reducing
 * permits.
 * 
 * @since 1.0.0
 * @author mark.petrenyi
 */
public class ReducableSemaphore extends Semaphore {

    /**
     * Constructs a ReducableSemaphore with the given number of permits.
     *
     * @param permits
     *            the initial number of permits available
     */
    public ReducableSemaphore(int permits) {
        super(permits);
    }

    /**
     * Constructs a ReducableSemaphore with the given number of permits and fairness setting.
     *
     * @param permits
     *            the initial number of permits available
     * @param fair
     *            true if this semaphore will guarantee first-in first-out granting of permits under contention, false otherwise
     */
    public ReducableSemaphore(int permits, boolean fair) {
        super(permits, fair);
    }

    /**
     * Reduces the number of available permits by the given reduction.
     *
     * @param reduction
     *            the number of permits to reduce
     */
    @Override
    public void reducePermits(int reduction) {
        super.reducePermits(reduction);
    }
}
