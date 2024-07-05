package org.epics.archiverappliance.retrieval.postprocessors;

import org.epics.archiverappliance.common.TimeSpan;

import java.util.List;

/**
 * Post processors can optionally implement this interface if the implement timespan specific functionality
 * @author mshankar
 *
 */
public interface TimeSpanDependentProcessing {
    /**
     * The data source resolution will call this method to give the post processor a chance to implement time span dependent post processing.
     * @param timeSpans  &emsp;
     * @return TimeSpanDependentProcessors List
     */
    public List<TimeSpanDependentProcessor> generateTimeSpanDependentProcessors(List<TimeSpan> timeSpans);
}
