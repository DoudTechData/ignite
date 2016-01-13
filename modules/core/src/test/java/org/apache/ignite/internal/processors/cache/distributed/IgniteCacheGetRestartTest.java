/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheRebalanceMode.*;

/**
 *
 */
public class IgniteCacheGetRestartTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final long TEST_TIME = 10_000;

    /** */
    private static final int SRVS = 3;

    /** */
    private static final int CLIENTS = 1;

    /** */
    private static final int KEYS = 100_000;

    /** */
    private ThreadLocal<Boolean> client = new ThreadLocal<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        Boolean clientMode = client.get();

        if (clientMode != null) {
            cfg.setClientMode(clientMode);

            client.remove();
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(SRVS);

        for (int i = 0; i < CLIENTS; i++) {
            client.set(true);

            Ignite client = startGrid(SRVS);

            assertTrue(client.configuration().isClientMode());
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @param restartCnt Number of nodes to restart.
     * @throws Exception If failed.
     */
    private void checkRestart(CacheConfiguration ccfg, int restartCnt) throws Exception {
        ignite(0).createCache(ccfg);

        if (ccfg.getNearConfiguration() != null)
            ignite(SRVS).createNearCache(ccfg.getName(), new NearCacheConfiguration<>());

        try (IgniteDataStreamer<Object, Object> streamer = ignite(0).dataStreamer(ccfg.getName())) {
            for (int i = 0; i < KEYS; i++)
                streamer.addData(i, i);
        }

        long stopTime = System.currentTimeMillis() + TEST_TIME;
    }


    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param near If {@code true} near cache is enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(CacheMode cacheMode, int backups, boolean near) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setCacheMode(cacheMode);

        if (cacheMode != CacheMode.REPLICATED)
            ccfg.setBackups(backups);

        if (near)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        ccfg.setRebalanceMode(SYNC);

        return ccfg;
    }
}
