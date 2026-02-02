package org.epics.archiverappliance.common.reports;

import org.epics.archiverappliance.config.ConfigService;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public interface Details {

    /**
     * Metric detail.
     * @param source Source
     * @param name Name
     * @param value Value
     * @return Metric detail map
     */
    static Map<String, String> metricDetail(String source, String name, String value) {
        Map<String, String> obj = new LinkedHashMap<String, String>();
        obj.put("name", name);
        obj.put("value", value);
        obj.put("source", source);
        return obj;
    }

    /**
     * Source WAR file.
     * @return Source
     */
    ConfigService.WAR_FILE source();

    /**
     * Metric detail with implicit source.
     * @param name Name
     * @param value Value
     * @return Metric detail map
     */
    default Map<String, String> metricDetail(String name, String value) {
        return metricDetail(source().toString(), name, value);
    }

    /**
     * Details.
     * @param configService Config Service
     * @return List of details
     */
    LinkedList<Map<String, String>> details(ConfigService configService);
}
