/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.pv;

import gov.aps.jca.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** A Channel with thread-safe reference count.
 *  @author Kay Kasemir
 */
@SuppressWarnings("nls")
public class RefCountedChannel {
    private Channel channel;
    private static final Logger logger = LogManager.getLogger(RefCountedChannel.class);

    private int refs;

    /** Initialize
     *  @param channel ChannelAccess channel
     *  @throws Error when channel is <code>null</code>
     */
    public RefCountedChannel(final Channel channel) {
        if (channel == null) throw new Error("Channel must not be null");
        this.channel = channel;
        refs = 1;
    }

    /** Increment reference count */
    public synchronized void incRefs() {
        ++refs;
    }

    /** Decrement reference count.
     *  @return Remaining references.
     */
    public synchronized int decRefs() {
        --refs;
        return refs;
    }

    /** @return ChannelAccess channel */
    public Channel getChannel() {
        return channel;
    }

    /** Must be called when all references are gone
     *  @throws Error when channel is still references
     */
    public void dispose() {
        if (refs != 0) throw new Error("Channel destroyed while referenced " + refs + " times");
        try {
            channel.destroy();
        } catch (Exception ex) {
            //     	Activator.getLogger().log(Level.WARNING, "Channel.destroy failed", ex); //$NON-NLS-1$
            logger.warn("exception when  dispose RefCountedChannel", ex);
        }
        channel = null;
    }
}
