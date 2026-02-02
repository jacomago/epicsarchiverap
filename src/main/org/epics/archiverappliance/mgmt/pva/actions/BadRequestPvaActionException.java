package org.epics.archiverappliance.mgmt.pva.actions;

/**
 * Exception to return a bad request when a problem happens
 * handling a pvAccess request through a {@link PvaAction} interface
 */
public class BadRequestPvaActionException extends PvaActionException {
    /**
     * Constructor.
     * @param e Exception
     */
    public BadRequestPvaActionException(Exception e) {
        super(e);
    }

    /**
     * Constructor.
     * @param msg Message
     * @param ex Cause
     */
    public BadRequestPvaActionException(String msg, IllegalArgumentException ex) {
        super(msg, ex);
    }

    /**
     * Constructor.
     * @param msg Message
     */
    public BadRequestPvaActionException(String msg) {
        super(msg);
    }
}
