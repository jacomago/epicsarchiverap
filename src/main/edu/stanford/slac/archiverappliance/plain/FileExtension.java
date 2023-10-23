package edu.stanford.slac.archiverappliance.plain;

/**
 * Lists possible file extensions for this plugin.
 */
public enum FileExtension {
    PB("pb"),
    PARQUET("parquet"),
    ;

    private final String suffix;

    FileExtension(String suffix) {
        this.suffix = suffix;
    }
    public String getSuffix() {
        return this.suffix;
    }

    public String getExtensionString() {
        return "." + this.suffix;
    }
}
