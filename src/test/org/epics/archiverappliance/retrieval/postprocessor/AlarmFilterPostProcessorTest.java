package org.epics.archiverappliance.retrieval.postprocessor;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.data.PBScalarDouble;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.CallableEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.AlarmFilterPostProcessor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AlarmFilterPostProcessorTest {
    private static final Logger logger = LogManager.getLogger(AlarmFilterPostProcessorTest.class.getName());
    private static final String pvName = AlarmFilterPostProcessor.class.getSimpleName();

    /**
     * Generates the sample data stream. The data is every second going up in severity to maxSeverity then starting from
     * 0 again.
     *
     * @param year         the year for which the sample data is generated
     * @param pvName       Name of the pv
     * @param samplesCount Number of samples to create
     * @param maxSeverity  Maximum severity to create
     * @return the generated data
     */
    private static ArrayListEventStream generateData(short year, String pvName, int samplesCount, int maxSeverity, int minSeverity) {
        ArrayListEventStream testData =
                new ArrayListEventStream(0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
        for (int s = 0; s < samplesCount; s++) {
            for (int severity = minSeverity; severity <= maxSeverity; severity++) {
                EPICSEvent.ScalarDouble.Builder builder = EPICSEvent.ScalarDouble.newBuilder()
                        .setSecondsintoyear(s)
                        .setNano(0)
                        .setVal(0)
                        .setSeverity(severity);
                ByteArray bar = new ByteArray(LineEscaper.escapeNewLines(builder.build().toByteArray()));
                Event e = new PBScalarDouble(year, bar);
                testData.add(e);
            }
        }
        return testData;
    }

    @Test
    public void testAlarmFilterNone() throws Exception {
        ArrayListEventStream events = generateData((short) 0, pvName, 10, 5, 0);
        AlarmFilterPostProcessor alarmFilterPostProcessor = new AlarmFilterPostProcessor();
        alarmFilterPostProcessor.initialize("_-1", pvName);
        var eventStream = alarmFilterPostProcessor.wrap(CallableEventStream.makeOneStreamCallable(events, null, false));

        assertEquals(events, eventStream.call());
    }

    @Test
    public void testAlarmFilterDefault() throws Exception {
        ArrayListEventStream events = generateData((short) 0, pvName, 10, 5, 0);
        AlarmFilterPostProcessor alarmFilterPostProcessor = new AlarmFilterPostProcessor();
        alarmFilterPostProcessor.initialize("", pvName);
        var eventStream = alarmFilterPostProcessor.wrap(CallableEventStream.makeOneStreamCallable(events, null, false));

        assertEquals(generateData((short) 0, pvName, 10, 5, 1), eventStream.call());
    }

    @Test
    public void testAlarmFilterMid() throws Exception {
        ArrayListEventStream events = generateData((short) 0, pvName, 10, 5, 0);
        AlarmFilterPostProcessor alarmFilterPostProcessor = new AlarmFilterPostProcessor();
        alarmFilterPostProcessor.initialize("_3", pvName);
        var eventStream = alarmFilterPostProcessor.wrap(CallableEventStream.makeOneStreamCallable(events, null, false));

        assertEquals(generateData((short) 0, pvName, 10, 5, 4), eventStream.call());
    }
}
