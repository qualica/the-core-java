/*
 * Copyright (c) 2010.  Korwe Software
 *
 *  This file is part of TheCore.
 *
 *  TheCore is free software: you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  TheCore is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with TheCore.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.korwe.thecore.api;

import com.rabbitmq.client.*;
import org.slf4j.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * An adapter to {@link ConnectionFactory ConnectionFactory} used to configure and create a
 * {@link Connection Connection} allowing the success of the initial {@link Connection Connection} to be managed with
 * an acceptable failure limit and retry backoff settings
 *
 * @author <a href="mailto:walker.fraser@gmail.com>Fraser Walker</a>
 * @since 2.1.0-b1
 */
public class CoreConnectionFactory extends ConnectionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CoreConnectionFactory.class);
    /**
     * The default initial connection retries attempt count. Set to less than 0 for infinite retries
     */
    public static final int DEFAULT_INITIAL_CONNECTION_RETRIES = -1;
    /**
     * The default millisecond multiplier for initial connection attempt backoff
     */
    public static final int DEFAULT_INITIAL_CONNECTION_BACKOFF = 2000;
    /**
     * The default maximum backoff in milliseconds for initial connection attempt backoff
     */
    public static final int DEFAULT_INITIAL_CONNECTION_BACKOFF_MAXIMUM = 10000;

    private int initialConnectionRetries = DEFAULT_INITIAL_CONNECTION_RETRIES;
    private int initialConnectionBackoff = DEFAULT_INITIAL_CONNECTION_BACKOFF;
    private int initialConnectionBackoffMaximum = DEFAULT_INITIAL_CONNECTION_BACKOFF_MAXIMUM;

    /**
     * @return The amount of times the initial connection should be attempted before connection attempt is failed.
     * Default is -1 for infinite.
     * @since 2.1.0-b1
     */
    public int getInitialConnectionRetries() {
        return initialConnectionRetries;
    }

    /**
     * @param initialConnectionRetries The amount of times the initial connection should be attempted before connection
     *                                 attempt is failed.
     * @since 2.1.0-b1
     */
    public void setInitialConnectionRetries(int initialConnectionRetries) {
        this.initialConnectionRetries = initialConnectionRetries;
    }

    /**
     * @return The amount, in milliseconds, that each successive initial connection attempt should add to the pause
     * before attempting to connect again after a failure
     * @since 2.1.0-b1
     */
    public int getInitialConnectionBackoff() {
        return initialConnectionBackoff;
    }

    /**
     * @param initialConnectionBackoff The amount, in milliseconds, that each successive initial connection attempt
     *                                 should add to the pause before attempting to connect again after a failure
     * @since 2.1.0-b1
     */
    public void setInitialConnectionBackoff(int initialConnectionBackoff) {
        this.initialConnectionBackoff = initialConnectionBackoff;
    }

    /**
     * @return The maximum amount, in milliseconds, that each successive initial connection attempt should pause before
     * attempting to connect again after a failure
     * @since 2.1.0-b1
     */
    public int getInitialConnectionBackoffMaximum() {
        return initialConnectionBackoffMaximum;
    }

    /**
     * @param initialConnectionBackoffMaximum The maximum amount, in milliseconds, that each successive initial
     *                                        connection attempt should pause before attempting to connect again after a
     *                                        failure
     */
    public void setInitialConnectionBackoffMaximum(int initialConnectionBackoffMaximum) {
        this.initialConnectionBackoffMaximum = initialConnectionBackoffMaximum;
    }

    @Override
    public Connection newConnection(ExecutorService executor, AddressResolver addressResolver,
                                    String clientProvidedName) throws IOException,
                                                                      TimeoutException {
        if (getInitialConnectionRetries() == 0) {
            return super.newConnection(executor, addressResolver, clientProvidedName);
        }
        else {
            return retries(executor, addressResolver, clientProvidedName);
        }
    }

    private Connection retries(ExecutorService executor, AddressResolver addressResolver, String clientProvidedName) {
        int n = 0;
        while (true) {
            try {
                LOG.info("Connection Attempt: {}", n += 1);
                return super.newConnection(executor, addressResolver, clientProvidedName);
            }
            catch (IOException | TimeoutException e) {
                if (n == getInitialConnectionRetries()) {
                    throw new RuntimeException(e);
                }
                LOG.warn(String.format("Connection Attempt Failed: %s", n), e);
                try {
                    Thread.sleep(getInitialConnectionBackoffMaximum()
                            >= (getInitialConnectionBackoff() + (n * getInitialConnectionBackoff()))
                            ? getInitialConnectionBackoffMaximum()
                            : getInitialConnectionBackoff() + (n * getInitialConnectionBackoff()));
                }
                catch (InterruptedException e1) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}
