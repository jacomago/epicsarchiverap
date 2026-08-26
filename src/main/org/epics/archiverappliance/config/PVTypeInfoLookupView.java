/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

/**
 * Resolving a user-supplied PV name to its {@link PVTypeInfo} means following aliases, so the two concerns
 * are needed together. This composes them and declares no methods of its own; Java has no intersection type
 * for a parameter, and the alternative is handing the resolver the whole config service.
 * <ul>
 * <li>{@link AliasRegistry} — alias to real name.</li>
 * <li>{@link PVTypeInfoStore} — real name to type info.</li>
 * </ul>
 * This is the most common concern pair in the codebase. Use it for name resolution only; a consumer that
 * also mutates type infos or asks which appliance owns a PV wants those concerns named separately.
 *
 * @see PVNames#determineAppropriatePVTypeInfo(String, PVTypeInfoLookupView)
 */
public interface PVTypeInfoLookupView extends AliasRegistry, PVTypeInfoStore {}
