package edu.stanford.slac.archiverappliance.plain.parquet;

import org.epics.archiverappliance.etl.ETLBulkStream;

import java.nio.file.Path;

public interface ETLParquetFilesStream extends ETLBulkStream {

    /**
     * Get parquet file paths
     *
     * @return List of paths to parquet files
     */
    Path getPath();
}
