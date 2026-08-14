/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

/**
 * The configuration a {@link org.epics.archiverappliance.StoragePlugin} needs in order to initialize itself.
 * <p>
 * This is a composed view over three existing concerns and declares no methods of its own. Storage plugins
 * span three of them and Java has no intersection type for a parameter, so the alternative would be handing
 * them back the whole config service:
 * <ul>
 * <li>{@link PVTypeInfoStore} — the chunk key and the actual DBR type of a PV.</li>
 * <li>{@link PVNamingConfig} — the fallback PV-name-to-key converter.</li>
 * <li>{@link InstallationProperties} — named flags, used by the ETL in/out predicates.</li>
 * </ul>
 * Nothing in storage reaches cluster membership, the PV directory, policies or the appliance lifecycle,
 * and this interface is what keeps that true.
 *
 * @author mshankar
 */
public interface StoragePluginConfigView extends PVTypeInfoStore, PVNamingConfig, InstallationProperties {}
