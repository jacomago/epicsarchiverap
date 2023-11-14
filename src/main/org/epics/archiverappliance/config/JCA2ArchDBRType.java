package org.epics.archiverappliance.config;

import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JCA2ArchDBRType {
    private static final Logger logger = LogManager.getLogger(JCA2ArchDBRType.class.getName());

    JCA2ArchDBRType() {
    }

    /**
     * Get the equivalent archiver data type given a JCA DBR
     *
     * @param d JCA DBR
     * @return ArchDBRTypes  &emsp;
     */
    public static ArchDBRTypes valueOf(DBR d) {

        boolean isVector = (d.getCount() > 1);
        DBRType dt = d.getType();
        if (dt != null) {
            if (dt == DBRType.TIME_STRING) {
                if (isVector) {
                    return ArchDBRTypes.DBR_WAVEFORM_STRING;
                } else {
                    return ArchDBRTypes.DBR_SCALAR_STRING;
                }
            }
            if (dt == DBRType.TIME_SHORT) {
                if (isVector) {
                    return ArchDBRTypes.DBR_WAVEFORM_SHORT;
                } else {
                    return ArchDBRTypes.DBR_SCALAR_SHORT;
                }
            }
            if (dt == DBRType.TIME_FLOAT) {
                if (isVector) {
                    return ArchDBRTypes.DBR_WAVEFORM_FLOAT;
                } else {
                    return ArchDBRTypes.DBR_SCALAR_FLOAT;
                }
            }
            if (dt == DBRType.TIME_ENUM) {
                if (isVector) {
                    return ArchDBRTypes.DBR_WAVEFORM_ENUM;
                } else {
                    return ArchDBRTypes.DBR_SCALAR_ENUM;
                }
            }
            if (dt == DBRType.TIME_BYTE) {
                if (isVector) {
                    return ArchDBRTypes.DBR_WAVEFORM_BYTE;
                } else {
                    return ArchDBRTypes.DBR_SCALAR_BYTE;
                }
            }
            if (dt == DBRType.TIME_INT) {
                if (isVector) {
                    return ArchDBRTypes.DBR_WAVEFORM_INT;
                } else {
                    return ArchDBRTypes.DBR_SCALAR_INT;
                }
            }
            if (dt == DBRType.TIME_DOUBLE) {
                if (isVector) {
                    return ArchDBRTypes.DBR_WAVEFORM_DOUBLE;
                } else {
                    return ArchDBRTypes.DBR_SCALAR_DOUBLE;
                }
            }
        }
        logger.error("Cannot determine ArchDBRType for DBRType " + (dt != null ? dt.getName() : "null") + " and count "
                + d.getCount());
        return null;
    }
}
