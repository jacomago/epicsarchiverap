package org.epics.archiverappliance.retrieval.postprocessors;

import java.util.LinkedList;

import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeSpan;

/**
 * Some post processors implement a consolidated event stream.
 * That is, they have an internal event stream that contains data obtained by processing the underlying event streams.
 * Typical examples are the binning event streams like Mean, RMS etc.
 * In this case the retrieval servlet, sends the consolidated event stream instead of the underlying streams. 
 * @author mshankar
 *
 */
public interface PostProcessorWithConsolidatedEventStream {
	/**
	 * Get consolidated event stream.
	 * @return Event Stream
	 */
	EventStream getConsolidatedEventStream();
	/**
	 * Get start bin epoch seconds.
	 * @return Start bin epoch seconds
	 */
	long getStartBinEpochSeconds();
	/**
	 * Get end bin epoch seconds.
	 * @return End bin epoch seconds
	 */
	long getEndBinEpochSeconds();
	/**
	 * Get bin timestamps.
	 * @return Bin timestamps
	 */
	LinkedList<TimeSpan> getBinTimestamps();
}
