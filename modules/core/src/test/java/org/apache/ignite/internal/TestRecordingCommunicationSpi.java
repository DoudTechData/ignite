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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class TestRecordingCommunicationSpi extends TcpCommunicationSpi {
    /** */
    private Class<?> recordCls;

    /** */
    private List<Object> recordedMsgs = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {
        if (msg instanceof GridIoMessage) {
            Object msg0 = ((GridIoMessage)msg).message();

            synchronized (this) {
                if (recordCls != null && msg0.getClass().equals(recordCls))
                    recordedMsgs.add(msg0);
            }
        }

        super.sendMessage(node, msg, ackC);
    }

    /**
     * @param recordCls Message class to record.
     */
    public void record(@Nullable Class<?> recordCls) {
        synchronized (this) {
            this.recordCls = recordCls;
        }
    }

    /**
     * @return Recorded messages.
     */
    public List<Object> recordedMessages() {
        synchronized (this) {
            List<Object> msgs = recordedMsgs;

            recordedMsgs = new ArrayList<>();

            return msgs;
        }
    }
}
