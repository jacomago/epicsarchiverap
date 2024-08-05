package org.epics.archiverappliance.etl;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;

/**
 * Event streams can optionally implement bulk transfer friendly methods.
 * If this interface is implemented, then ETL code will use bulk transfers whan moving data.
 * @author mshankar
 *
 */
public interface ETLBulkStream extends EventStream {
	
	/**
	 * Get the first event in this event stream.
	 * If there are no events in this stream, return null.
	 * @param context BasicContext 
	 * @return Event return the first event, or null
	 * @throws IOException  &emsp;
	 */
	public Event getFirstEvent(BasicContext context) throws IOException;

}
