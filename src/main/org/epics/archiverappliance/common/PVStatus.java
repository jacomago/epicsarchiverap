package org.epics.archiverappliance.common;

import java.util.Arrays;
import java.util.NoSuchElementException;

public enum PVStatus {
    BEING_ARCHIVED("Being archived"),
    PAUSED("Paused"),
    INITIAL_SAMPLING("Initial sampling"),
    NOT_BEING_ARCHIVED("Not being archived"),
    APPLIANCE_DOWN("Appliance Down"),
    APPLIANCE_ASSIGNED("Appliance assigned");
    private final String name;

    PVStatus(String s) {
        this.name = s;
    }

    public static PVStatus fromString(String statusString) {
        var val = Arrays.stream(PVStatus.values())
                .filter(v -> v.name.equals(statusString))
                .findAny();
        if (val.isPresent()) {
            return val.get();
        }
        throw new NoSuchElementException("No such " + statusString + " PVStatus");
    }

    @Override
    public String toString() {
        return name;
    }
}
