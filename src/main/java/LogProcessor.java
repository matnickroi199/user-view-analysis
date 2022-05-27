import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import utils.Common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

public class LogProcessor {

    private static final Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "resources/schema.avsc";
    private static final String PC_INPUT_PATH ="/ads_log/ad-pt-v1/";
    private static final String PC_OUTPUT_PATH = "/data/pageview_pc/";
    private static final String MB_INPUT_PATH = "/ads_log/ad-pt-mobile/";
    private static final String MB_OUTPUT_PATH = "/data/pageview_mb/";
    private static final Configuration HADOOP_CONFIG;

    static {
        try {
            SCHEMA = new Schema.Parser().parse(new File(SCHEMA_LOCATION));
        } catch (IOException e) {
            throw new RuntimeException("Can't read SCHEMA file from" + SCHEMA_LOCATION, e);
        }
        HADOOP_CONFIG = new Configuration();
        HADOOP_CONFIG.set("fs.defaultFS", "hdfs://10.5.92.76:9000");
        HADOOP_CONFIG.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    public static void main(String[] args) {
        String date;
        if (args.length > 0) {
            date = args[0];
        } else {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date current = new Date();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(current);
            calendar.add(Calendar.DATE, -1);
            date = simpleDateFormat.format(calendar.getTime());
        }
        System.out.println("Handle log: " + date);
        LogProcessor processor = new LogProcessor();
        processor.handle(date, true);
        processor.handle(date, false);
    }

    public void handle(String date, boolean isPC) {
        String rawLogPath = (isPC ? PC_INPUT_PATH : MB_INPUT_PATH) + date;
        String outputPath = (isPC ? PC_OUTPUT_PATH : MB_OUTPUT_PATH) + date;
        List<String> filesInput = new ArrayList<>();
        try (Stream<java.nio.file.Path> paths = Files.walk(Paths.get(rawLogPath))) {
            paths.filter(Files::isRegularFile).forEach(path -> filesInput.add(path.toString()));
            buildParquet(outputPath, filesInput);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void buildParquet(String outputPath, List<String> filesInput) throws IOException {
        List<GenericData.Record> recordsToWrite = new ArrayList<>();
        int numFileRead = 0;
        int numFileWrite = 0;
        for(String file : filesInput) {
            recordsToWrite.addAll(parseRawToSchema(file));
            if(++numFileRead == 100) {
                numFileRead = 0;
                numFileWrite++;
                writeParquetFile(recordsToWrite, outputPath+"/"+numFileWrite+".parquet");
                recordsToWrite.clear();
            }
        }
        if (!recordsToWrite.isEmpty()) {
            numFileWrite++;
            writeParquetFile(recordsToWrite, outputPath+"/"+numFileWrite+".parquet");
        }
    }

    public void writeParquetFile(List<GenericData.Record> recordsToWrite, String path) {
        Path fileToWrite = new Path(path);
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(SCHEMA)
                .withConf(HADOOP_CONFIG)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public List<GenericData.Record> parseRawToSchema(String path) {
        List<GenericData.Record> parquet = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] s = line.split("\t");
                if (s.length >= 24) {
                    GenericData.Record record = new GenericData.Record(SCHEMA);
                    record.put("time", s[0]);
                    record.put("browser", Common.IntStr(s[2]));
                    record.put("os", Common.IntStr(s[4]));
                    record.put("loc", Common.IntStr(s[7]));
                    record.put("domain", s[8]);
                    record.put("path", s[11]);
                    record.put("guid", Common.LongStr(s[13].replace("]", "")));
                    record.put("category", s[23]);
                    parquet.add(record);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return parquet;
    }
}