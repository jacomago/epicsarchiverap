package edu.stanford.slac.archiverappliance.plain.parquet;

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.proto.ProtoWriteSupport;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.util.HashMap;
import java.util.Map;

import static edu.stanford.slac.archiverappliance.plain.parquet.ParquetInfo.PV_NAME;
import static edu.stanford.slac.archiverappliance.plain.parquet.ParquetInfo.TYPE;
import static edu.stanford.slac.archiverappliance.plain.parquet.ParquetInfo.YEAR;

public class EpicsWriteSupport<T extends Message> extends WriteSupport<T> {

    String pvName;
    short year;
    ArchDBRTypes archDBRTypes;
    ProtoWriteSupport<T> protoWriteSupport;

    EpicsWriteSupport(Class<? extends Message> messageClass, String pvName, short year, ArchDBRTypes archDBRTypes) {
        this.pvName = pvName;
        this.year = year;
        this.archDBRTypes = archDBRTypes;
        this.protoWriteSupport = new ProtoWriteSupport<T>(messageClass);
    }
    /**
     *
     * @param configuration the job's configuration
     * @return
     */
    @Override
    public WriteContext init(Configuration configuration) {
        WriteContext writeContext = this.protoWriteSupport.init(configuration);

        Map<String, String> extraMetaData = new HashMap<>(writeContext.getExtraMetaData());
        extraMetaData.put(PV_NAME, this.pvName);
        extraMetaData.put(YEAR, String.valueOf(this.year));
        extraMetaData.put(TYPE, String.valueOf(this.archDBRTypes));
        return new WriteContext(writeContext.getSchema(), extraMetaData);
    }

    /**
     *
     * @param recordConsumer the recordConsumer to write to
     */
    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.protoWriteSupport.prepareForWrite(recordConsumer);
    }

    /**
     *
     * @param record one record to write to the previously provided record consumer
     */
    @Override
    public void write(T record) {
        this.protoWriteSupport.write(record);
    }
}
