package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.plain.FileExtension;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.junit.jupiter.params.provider.Arguments;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public record ETLTestPlugins(PlainStoragePlugin src, PlainStoragePlugin dest) {
    public static List<ETLTestPlugins> generatePlugins() {

        return generateFileExtensions().stream()
                .map(fPair -> new ETLTestPlugins(new PlainStoragePlugin(fPair[0]), new PlainStoragePlugin(fPair[1])))
                .toList();
    }

    public static List<FileExtension[]> generateFileExtensions() {
        List<FileExtension[]> fileExtensions = new ArrayList<>();
        fileExtensions.add(new FileExtension[] {FileExtension.PB, FileExtension.PB});
        fileExtensions.add(new FileExtension[] {FileExtension.PB, FileExtension.PARQUET});
        fileExtensions.add(new FileExtension[] {FileExtension.PARQUET, FileExtension.PB});
        fileExtensions.add(new FileExtension[] {FileExtension.PARQUET, FileExtension.PARQUET});

        return fileExtensions;
    }

    public static Stream<Arguments> provideFileExtensionArguments() {
        return ETLTestPlugins.generateFileExtensions().stream().map(fPair -> Arguments.of(fPair[0], fPair[1]));
    }

    public String pvNamePrefix() {
        return String.format("DEST%sSRC%s", this.dest.getFileExtension(), this.src.getFileExtension());
    }
}
