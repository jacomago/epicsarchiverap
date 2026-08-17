package org.epics.archiverappliance.mgmt.pva.actions;

import org.epics.pva.data.PVAStructure;
import org.epics.pva.server.RPCService;

/**
 * Wrapper around the {@link RPCService} for the Archiver
 *
 * <p>The type parameter is the slice of configuration this action actually needs. Actions vary widely
 * here — some want nothing more than the appliance identity, others reach across most of the config
 * service — so a single fixed parameter type would hand every action the widest one. Declare the
 * narrowest interface that compiles, for example {@code implements PvaAction<ClusterTopology>}.
 *
 * @param <C> the configuration concern (or composed view) this action consumes
 * @author Kunal Shroff
 *
 */
public interface PvaAction<C> {

    /**
     * Name of the action
     * @return the name of the service
     */
    String getName();

    /**
     * Handles an RPC request to the archiver.
     *
     * @param args Input arguments
     * @param configService the configuration this action needs, supplied by the dispatcher
     * @throws PvaActionException which is then passed to the serverPV to return the error to the user.
     */
    PVAStructure request(PVAStructure args, C configService) throws PvaActionException;
}
