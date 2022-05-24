import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BuildParquet {

    private static final Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "resources/schema.avsc";
    private static final Path OUT_PATH = new Path("resources/sample.parquet");

    static {
        try {
            SCHEMA = new Schema.Parser().parse(new File(SCHEMA_LOCATION));
        } catch (IOException e) {
            throw new RuntimeException("Can't read SCHEMA file from" + SCHEMA_LOCATION, e);
        }
    }

    public static void main(String[] args) throws IOException {
        BuildParquet writerReader = new BuildParquet();
        writerReader.writeToParquet(OUT_PATH);
    }

    public void writeToParquet(Path fileToWrite) throws IOException {
        List<GenericData.Record> recordsToWrite = parseRawToSchema();
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

    public List<GenericData.Record> parseRawToSchema() {
        List<GenericData.Record> parquet = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("resources/pt-v-1650007779932.dat"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] s = line.split("\t");
                GenericData.Record record = new GenericData.Record(SCHEMA);
                record.put("time", s[0]);
                record.put("browser", Utils.IntStr(s[2]));
                record.put("os", Utils.IntStr(s[4]));
                record.put("loc", Utils.IntStr(s[7]));
                record.put("domain", s[8]);
                record.put("path", s[11]);
                record.put("guid", Utils.LongStr(s[13].replace("]", "")));
                record.put("category", s[23]);
                parquet.add(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return parquet;
    }
}