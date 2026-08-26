package org.epics.archiverappliance.mgmt.pva.actions;

import org.epics.pva.data.PVAStructure;
import org.epics.pva.server.RPCService;

/**
 * Wrapper around the {@link RPCService} for the Archiver
 *
 * <p>An action takes the slice of configuration it needs through its constructor, one parameter per
 * concern, and the service that registers it supplies them. Actions vary widely here — some want
 * nothing more than the appliance identity, others reach across most of the config service — and a
 * config service parameter on {@link #request} would hand every action the widest one regardless.
 *
 * @author Kunal Shroff
 *
 */
public interface PvaAction {

    /**
     * Name of the action
     * @return the name of the service
     */
    String getName();

    /**
     * Handles an RPC request to the archiver.
     *
     * @param args Input arguments
     * @throws PvaActionException which is then passed to the serverPV to return the error to the user.
     */
    PVAStructure request(PVAStructure args) throws PvaActionException;
}
