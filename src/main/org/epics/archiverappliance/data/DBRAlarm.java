package org.epics.archiverappliance.data;

import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAStructure;

public record DBRAlarm(int severity, int status) {

    public static DBRAlarm convertPVAlarm(PVAStructure alarmPVStructure) {
        return new DBRAlarm(
                alarmPVStructure == null ? 0 : ((PVAInt) alarmPVStructure.get("severity")).get(),
                alarmPVStructure == null ? 0 : ((PVAInt) alarmPVStructure.get("status")).get());
    }
}
