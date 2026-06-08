package org.epics.archiverappliance.retrieval.postprocessors;

import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeSpan;

import java.util.LinkedList;

/**
 * Some post processors implement a consolidated event stream.
 * That is, they have an internal event stream that contains data obtained by processing the underlying event streams.
 * Typical examples are the binning event streams like Mean, RMS etc.
 * In this case the retrieval servlet, sends the consolidated event stream instead of the underlying streams.
 * @author mshankar
 *
 */
public interface PostProcessorWithConsolidatedEventStream {
    EventStream getConsolidatedEventStream();

    long getStartBinEpochSeconds();

    long getEndBinEpochSeconds();

    LinkedList<TimeSpan> getBinTimestamps();
}
