package org.epics.archiverappliance.retrieval.postprocessors;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.concurrent.Callable;

public class AlarmFilterPostProcessor implements PostProcessor {

    int minSeverity = 0;

    /**
     * @return
     */
    @Override
    public String getIdentity() {
        return "alarmFilter";
    }

    /**
     * @return
     */
    @Override
    public String getExtension() {
        return this.getIdentity();
    }

    /**
     * @param userarg This is the full form (extension) of the identity for the post processor.
     * @param pvName  The name of PV
     * @throws IOException
     */
    @Override
    public void initialize(String userarg, String pvName) throws IOException {
        if (userarg != null && userarg.contains("_")) {
            String[] userParams = userarg.split("_");
            String severityStr = userParams[1];
            this.minSeverity = Integer.parseInt(severityStr);
        }
    }

    /**
     * @param pvName   The name of PV
     * @param typeInfo PVTypeInfo
     * @param start    Timestamp
     * @param end      Timestamp
     * @param req      HttpServletRequest
     * @return
     */
    @Override
    public long estimateMemoryConsumption(
            String pvName, PVTypeInfo typeInfo, Timestamp start, Timestamp end, HttpServletRequest req) {
        return (long) typeInfo.getComputedStorageRate();
    }

    /**
     * @param callable &emsp;
     * @return
     */
    @Override
    public Callable<EventStream> wrap(Callable<EventStream> callable) {
        return () -> {
            try (EventStream stream = callable.call()) {
                ArrayListEventStream buf = new ArrayListEventStream(0, (RemotableEventStreamDesc) stream.getDescription());

                for (Event e : stream) {
                    if (e instanceof DBRTimeEvent timeEvent) {
                        if (timeEvent.getSeverity() > minSeverity) {
                            buf.add(e);
                        }
                    }
                }
                return buf;
            }
        };
    }

}
