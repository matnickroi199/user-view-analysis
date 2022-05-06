import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BuildParquet {

    private static final Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "resources/schema.avsc";
    private static final Path OUT_PATH = new Path("sample.parquet");

    static {
        try {
            SCHEMA = new Schema.Parser().parse(new File(SCHEMA_LOCATION));
        } catch (IOException e) {
            throw new RuntimeException("Can't read SCHEMA file from" + SCHEMA_LOCATION, e);
        }
    }

    public static void main(String[] args) throws IOException {
        List<GenericData.Record> sampleData = new ArrayList<>();

        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("c1", 1);
        record.put("c2", "someString");
        sampleData.add(record);

        record = new GenericData.Record(SCHEMA);
        record.put("c1", 2);
        record.put("c2", "otherString");
        sampleData.add(record);

        BuildParquet writerReader = new BuildParquet();
        writerReader.writeToParquet(sampleData, OUT_PATH);
    }

    public void writeToParquet(List<GenericData.Record> recordsToWrite, Path fileToWrite) throws IOException {
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
        }
    }

}