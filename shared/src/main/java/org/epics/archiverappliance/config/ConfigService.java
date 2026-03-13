/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

/**
 * Full configuration service interface.
 *
 * <p>All service-neutral methods are in {@link CoreConfigService}.
 * Service-specific runtime-state getters are split into four sub-interfaces:
 * {@link EngineConfigService}, {@link RetrievalConfigService},
 * {@link EtlConfigService}, and {@link MgmtConfigService}.
 *
 * <p>This combined interface is retained as a convenience during the modularisation
 * transition.  New code should prefer {@link CoreConfigService} where the
 * service runtime state is not needed, or one of the four sub-interfaces when
 * only a single service's state is required.
 *
 * @deprecated Prefer {@link CoreConfigService} or the appropriate service-specific
 *             sub-interface. This interface will be removed when the legacy
 *             {@code epicsarchiverap} module is retired (Phase 5).
 */
@Deprecated
public interface ConfigService
        extends EngineConfigService, RetrievalConfigService, EtlConfigService, MgmtConfigService {
    // All methods, constants, and nested types are inherited from the four
    // sub-interfaces (which all extend CoreConfigService).
    // No additional declarations are needed here.
}
