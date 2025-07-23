package org.epics.archiverappliance.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for dealing with various aspects of EPICS PV names and Channel Names.
 * @author mshankar
 *
 */
public class PVNames {
    /**
     * When you intend to connect to the PV's using PVAccess, use this string as a prefix in the UI/archivePV BPL. For example, pva://double01
     * This syntax should be consistent with CSS.
     */
    public static final String V4_PREFIX = "pva://";
    /**
     * When you intend to connect to the PV's using Channel Access, use this string as a prefix in the UI/archivePV BPL. For example, ca://double01
     * This syntax should be consistent with CSS.
     */
    public static final String V3_PREFIX = "ca://";

    private static final Logger logger = LogManager.getLogger(PVNames.class.getName());
    private static final Pattern pvNamePattern = Pattern.compile("[a-zA-Z0-9_\\-+:\\[\\]<>;/,#{}^]+");
    private static final Pattern fieldNamePattern = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
    private static final Pattern fieldModifierPattern = Pattern.compile("\\{[a-zA-Z0-9:()-{}\"',]+}|\\[[0-9]+:[0-9]+]");
    public static final String GROUP_PV_NAME = "PVNAME";
    public static final String GROUP_FIELD_NAME = "FIELDNAME";
    public static final String GROUP_FIELD_MODIFIER = "FIELDMODIFIER";

    /*
     * For certain characters, EPICS will not throw exceptions but generate spurious traffic which is hard to detect.
     * From the <a href="https://docs.epics-controls.org/en/latest/appdevguide/databaseDefinition.html#definitions-8">App dev Guide</a>
     * Valid characters are a-z A-Z 0-9 _ - + : [ ] &lt; &gt; ;
     * And we add the '/' character because some folks at FACET use this.
     * And we add the ',' character because some folks at LBL use this.
     * And we add the '#' character because some folks at FRIB use this.
     * And we add the '{' and the '}' character because some folks at BNL use this.
     * And we add the '^' character because some folks at LNL use this.
     * <p>
     * For field names using the filter support:
     * We add the '.' character for supporting field names as well.
     * And we add the ''' character for filter support: <a href="https://epics.anl.gov/base/R3-15/1-docs/filters.html">filters</a>
     */
    public static final Pattern channelNamePattern = Pattern.compile("(?<" + GROUP_PV_NAME + ">"
            + pvNamePattern.pattern()
            + ")\\.?(?<" + GROUP_FIELD_NAME + ">" + fieldNamePattern.pattern()
            + ")?\\.?(?<" + GROUP_FIELD_MODIFIER + ">" + fieldModifierPattern.pattern() + ")?");

    /**
     * Remove the .VAL, .HIHI etc portion of a channelName and return the plain pvName
     * @param channelName The name of the PV channel.
     * @return String The plain pvName
     */
    public static String channelNamePVName(String channelName) {
        if (StringUtils.isEmpty(channelName)) {
            return "";
        }

        return channelName.split("\\.")[0];
    }

    /**
     * @param channelName Input full name of the channel
     * @return Only the group if exists, "" otherwise.
     */
    private static String getGroupMatch(String channelName, String groupName) {
        if (StringUtils.isEmpty(channelName)) {
            return "";
        }
        Matcher matcher = PVNames.channelNamePattern.matcher(channelName);
        if (!matcher.find()) {
            return "";
        }
        try {
            String result = matcher.group(groupName);
            if (result == null) {
                return "";
            }
            return result;

        } catch (IllegalStateException e) {
            return "";
        }
    }
    /**
     * @param channelName Input full name of the channel
     * @return Only the group if exists, "" otherwise.
     */
    public static String getFieldName(String channelName) {
        return getGroupMatch(channelName, GROUP_FIELD_NAME);
    }

    /**
     * Is this a field or field modifier?
     * @param channelName  The name of PV.
     * @return boolean True or False
     */
    public static boolean isFieldOrFieldModifier(String channelName) {
        return isField(channelName) || isFieldModifier(channelName);
    }

    /**
     * Is this a field?
     * @param channelName  The name of PV.
     * @return boolean True or False
     */
    private static boolean isField(String channelName) {
        String fieldName = getGroupMatch(channelName, GROUP_FIELD_NAME);
        return !StringUtils.equalsAny(fieldName, "", "VAL");
    }

    /**
     * Is this a field modifier?
     * @param channelName  The name of PV.
     * @return boolean True or False
     */
    private static boolean isFieldModifier(String channelName) {

        String fieldModifier = getGroupMatch(channelName, GROUP_FIELD_MODIFIER);
        return !fieldModifier.isEmpty();
    }

    /**
     * Remove .VAL from pv names if present.
     * Returned value is something that can be used to lookup for PVTypeInfo
     * @param channelName The name of PVs.
     * @return String  normalizePVName
     */
    public static String normalizeChannelName(String channelName) {
        if (StringUtils.isEmpty(channelName)) {
            return "";
        }
        if (channelName.contains(".VAL")) {
            String newName = channelNamePVName(channelName);
            if (isFieldModifier(channelName)) {
                return newName + "." + getGroupMatch(channelName, GROUP_FIELD_MODIFIER);
            }
            return newName;
        }
        if (channelName.endsWith("."))
            return channelName.substring(0, channelName.length() - 1);
        return channelName;
    }

    /**
     * Gives you something you can use with caget to get the field associated with a PV even if you have a field already.
     * normalizePVNameWithField("ABC", "NAME") gives "ABC.NAME"
     * normalizePVNameWithField("ABC.HIHI", "NAME") gives "ABC.NAME"
     * @param pvName The name of PV.
     * @param fieldName &emsp;
     * @return String normalizePVNameWithField
     */
    public static String normalizePVNameWithField(String pvName, String fieldName) {
        if (StringUtils.isEmpty(pvName)) {
            return "";
        }
        return channelNamePVName(pvName) + "." + fieldName;
    }

    /**
     * Transfer any fields from the source name to the dest name
     * Transferring ABC:123 onto DEF:456 should give DEF:456
     * Transferring ABC:123.DESC onto DEF:456 should give DEF:456.DESC
     * @param srcName The source name
     * @param destName The destination name
     * @return String transferField
     */
    public static String transferField(String srcName, String destName) {
        if (isFieldOrFieldModifier(srcName)) {
            return normalizePVNameWithField(destName, getGroupMatch(srcName, GROUP_FIELD_NAME));
        } else {
            return normalizeChannelName(destName);
        }
    }

    /**
     * A standard process for dealing with aliases, standard fields and the like and getting to the PVTypeInfo.
     * @param pvName The name of PV.
     * @param configService ConfigService
     * @return PVTypeInfo  &emsp;
     * <p>
     * Places where we look for the typeinfo.
     * <ul>
     * <li> If the PV is not a field PV
     * <ul>
     * <li>Typeinfo for full PV name</li>
     * <li>Alias for full PV name + Typeinfo for full PV name</li>
     * </ul>
     * </li><li>If the PV is a field PV
     * <ul>
     * <li>Typeinfo for fieldless PVName + archiveFields</li>
     * <li>Typeinfo for full PV name</li>
     * <li>Alias for fieldless PVName + Typeinfo for fieldless PVName + archiveFields</li>
     * <li>Alias for full PV name + Typeinfo for full PV name</li>
     * </ul>
     * </ul>
     *
     */
    public static PVTypeInfo determineAppropriatePVTypeInfo(String pvName, ConfigService configService) {
        boolean pvDoesNotHaveField = !PVNames.isFieldOrFieldModifier(pvName);

        if (pvDoesNotHaveField) {
            logger.debug("Looking for typeinfo for fieldless PV " + GROUP_PV_NAME + " " + pvName);
            // Typeinfo for full PV name
            {
                PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
                if (typeInfo != null) {
                    logger.debug("Found typeinfo for pvName " + pvName);
                    return typeInfo;
                }
            }

            // Alias for full PV name + Typeinfo for full PV name
            {
                String realName = configService.getRealNameForAlias(pvName);
                if (realName != null) {
                    PVTypeInfo typeInfo = configService.getTypeInfoForPV(realName);
                    if (typeInfo != null) {
                        logger.debug("Found typeinfo for real pvName " + realName + " which is an alias of " + pvName);
                        return typeInfo;
                    }
                }
            }
        } else {
            logger.debug("Looking for typeinfo for PV " + GROUP_PV_NAME + " with a field " + pvName);
            String pvNameAlone = PVNames.channelNamePVName(pvName);
            String fieldName = PVNames.getGroupMatch(pvName, GROUP_FIELD_NAME);
            // Typeinfo for fieldless PVName + archiveFields
            {
                PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvNameAlone);
                if (typeInfo != null && typeInfo.checkIfFieldAlreadySepcified(fieldName)) {
                    logger.debug(
                            "Found typeinfo for fieldless pvName " + pvNameAlone + " for archiveField " + fieldName);
                    return typeInfo;
                }
            }

            // Typeinfo for full PV name
            {
                PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
                if (typeInfo != null) {
                    logger.debug("Found typeinfo for full pvName with field " + pvName);
                    return typeInfo;
                }
            }

            // Alias for fieldless PVName + Typeinfo for fieldless PVName + archiveFields
            {
                String realName = configService.getRealNameForAlias(pvNameAlone);
                if (realName != null) {
                    PVTypeInfo typeInfo = configService.getTypeInfoForPV(PVNames.channelNamePVName(realName));
                    if (typeInfo != null && typeInfo.checkIfFieldAlreadySepcified(fieldName)) {
                        logger.debug("Found typeinfo for aliased fieldless pvName " + realName + " for archiveField "
                                + fieldName);
                        return typeInfo;
                    }
                }
            }

            // Alias for full PV name + Typeinfo for full PV name
            {
                String realName = configService.getRealNameForAlias(pvName);
                if (realName != null) {
                    PVTypeInfo typeInfo = configService.getTypeInfoForPV(realName);
                    if (typeInfo != null) {
                        logger.debug("Found typeinfo for real pvName " + realName + " which is an alias of " + pvName);
                        return typeInfo;
                    }
                }
            }
        } // Ends Looking for typeinfo for PV name with a field " + pvName

        logger.debug("Did not find typeinfo for pvName " + pvName);
        return null;
    }

    /**
     * A standard process for dealing with aliases, standard fields and the like and checking to see if the PV is in the archive workflow.
     * @param pvName
     * @param configService
     * @return True if the PV or its avatars are in the archive workflow.
     * It is not possible to state this accurately for all fields.
     * For example, for fields that are archived as part of the stream, if the main PV is in the archive workflow, then the field is as well.
     * But it is impossible to state this before the PVTypeInfo has been computed.
     * So we resort to being pessimistic.
     *
     * Places where we look for the typeinfo.
     * <ul>
     * <li> If the PV is not a field PV
     * <ul>
     * <li>ArchivePVRequests for full PV name</li>
     * <li>Alias for full PV name + ArchivePVRequests for full PV name</li>
     * </ul>
     * </li><li>If the PV is a field PV
     * <ul>
     * <li>ArchivePVRequests for full PVName</li>
     * <li>Alias for full PV name + ArchivePVRequests for full PVName</li>
     * </ul>
     * Note that this translates to the fact that regardless of whether the PV is a field or not, we look in the same places.
     * </ul>
     */
    public static boolean determineIfPVInWorkflow(String pvName, ConfigService configService) {
        logger.debug("Looking for archiverequests for PV " + pvName);

        // ArchivePVRequests for full PV name
        {
            if (configService.doesPVHaveArchiveRequestInWorkflow(pvName)) {
                logger.debug("Found PV in archive request workflow " + pvName);
                return true;
            }
        }

        // Alias for full PV name + ArchivePVRequests for full PV name
        {
            String realName = configService.getRealNameForAlias(pvName);
            if (realName != null) {
                if (configService.doesPVHaveArchiveRequestInWorkflow(realName)) {
                    logger.debug("Found aliased PV in archive request workflow " + realName);
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Check to see if the channelName has valid characters.
     * Uses the regex "(?&lt;PVNAME&gt;[a-zA-Z0-9_\-+:\[\]&lt;&gt;;/,#{}^]+)\.?(?&lt;FIELDNAME&gt;[a-zA-Z0-9_\-+:]+)?\.?(?&lt;FIELDMODIFIER&gt;\{.+})?"
     * For certain characters, EPICS will not throw exceptions but generate spurious traffic which is hard to detect.
     * From the <a href="https://docs.epics-controls.org/en/latest/appdevguide/databaseDefinition.html#definitions-8">App dev Guide</a>
     * Valid characters are a-z A-Z 0-9 _ - + : [ ] &lt; &gt; ;
     * And we add the '/' character because some folks at FACET use this.
     * And we add the ',' character because some folks at LBL use this.
     * And we add the '#' character because some folks at FRIB use this.
     * And we add the '{' and the '}' character because some folks at BNL use this.
     * And we add the '^' character because some folks at LNL use this.
     * <p>
     * For field names using the filter support:
     * Filter support documentation: <a href="https://epics.anl.gov/base/R3-15/1-docs/filters.html">filters</a>
     * @param channelName The name of PV.
     * @return boolean True or False
     */
    public static boolean isValidChannelName(String channelName) {
        if (StringUtils.isEmpty(channelName)) return false;
        if (channelName.endsWith(".")) {
            logger.error("Channel " + channelName + " is invalid. Ends with '.'");
            return false;
        }
        logger.info(channelNamePattern.pattern());

        Matcher matcher = channelNamePattern.matcher(channelName);
        if (!matcher.find()) {
            return false;
        }

        if (matcher.group(GROUP_PV_NAME).isEmpty()) {
            logger.error("Channel " + channelName + " is invalid. Supported regex is " + channelNamePattern);
            return false;
        }

        if (matcher.end() != channelName.length() || matcher.start() != 0) {
            logger.error("PV Name " + channelName + " is invalid." + " Supported regex is " + channelNamePattern);
            return false;
        }
        return true;
    }

    /**
     * What type of name is the pv.
     * return EPICSVersion.V3 if starts with ca://
     * return EPICSVersion.V4 if starts with pva://
     * returns EPICSVersion.DEFAULT otherwise.
     *
     * @param pvName  The name of PV.
     * @return EPICSVersion
     */
    public static EPICSVersion pvNameVersion(String pvName) {
        if (StringUtils.isEmpty(pvName)) return EPICSVersion.DEFAULT;
        if (pvName.startsWith(V4_PREFIX)) return EPICSVersion.V4;
        if (pvName.startsWith(V3_PREFIX)) return EPICSVersion.V3;
        return EPICSVersion.DEFAULT;
    }

    /**
     * Remove the pva:// or ca:// prefix from the PV name if present.
     * @param pvName The name of PV.
     * @return String  &emsp;
     */
    public static String stripPrefixFromName(String pvName) {
        if (StringUtils.isEmpty(pvName)) return pvName;
        if (pvName.startsWith(V4_PREFIX)) {
            return pvName.replace(V4_PREFIX, "");
        }
        if (pvName.startsWith(V3_PREFIX)) {
            return pvName.replace(V3_PREFIX, "");
        }

        return pvName;
    }

    public enum EPICSVersion {
        V3,
        V4,
        DEFAULT
    }
}
