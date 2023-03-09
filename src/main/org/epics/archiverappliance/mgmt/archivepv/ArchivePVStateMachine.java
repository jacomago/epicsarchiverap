package org.epics.archiverappliance.mgmt.archivepv;

public enum ArchivePVStateMachine {
    START,
    METAINFO_REQUESTED,
    METAINFO_GATHERING,
    METAINFO_OBTAINED,
    POLICY_COMPUTED,
    TYPEINFO_STABLE,
    ARCHIVE_REQUEST_SUBMITTED,
    ARCHIVING,
    ABORTED,
    FINISHED
}
