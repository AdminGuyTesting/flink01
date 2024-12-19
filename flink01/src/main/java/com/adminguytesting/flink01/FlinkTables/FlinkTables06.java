package com.adminguytesting.flink01.FlinkTables;
// Parquet file writing
import com.adminguytesting.flink01.Avro.ParquetAvroHandler;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.function.Function;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

//import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
// Generic Avro dependencies
import org.apache.avro.Schema;

// Hadoop stuff
//import org.apache.hadoop.fs.Path;

// Generic Parquet dependencies
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetWriter;

// Avro->Parquet dependencies
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroParquetWriter;
import scala.collection.generic.BitOperations;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

@JsonPropertyOrder({"cab_id","cab_plate","cab_make","cab_driver","active_trip",
        "pickup_location","target_location","num_pass"})
class CabsPojo {
    public Integer cab_id;
    public String cab_plate;
    public String cab_make;
    public String cab_driver;
    public String active_trip;
    public String pickup_location;
    public String target_location;
    public String num_pass;
}

class UserRank {
    public Integer identifier;
    public Integer ranking;

    public UserRank(Integer id, Integer rank) {
        identifier=id;
        ranking=rank;
    }
    public MessageType getClassSchema() {

        MessageType parquetSchema = new MessageType("input_schema",
                Types.optional(INT32).named("identifier"),
                Types.optional(INT32).named("ranking")
        );

        return parquetSchema;
    }
}
/* /tmp/user.asvc
{
        "type": "record",
        "name": "UserRank",
        "fields": [
        {"name": "identifier", "type": "int"},
        {"name": "ranking", "type": "int"}
        ]
        }

 */
public class FlinkTables06 {

    public void runTest02() throws Exception{
        Schema schema = new Schema.Parser().parse(new File("/tmp/user.avsc"));
        GenericRecord user = new GenericData.Record(schema);
        user.put("identifier", 1);
        user.put("ranking", 100);


        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(user, encoder);
        encoder.flush();

    }

     public void runTest01(String fileName) throws Exception {
         //filename with format "file:///tmp/cabs/cabs.csv"
            //ParameterTool params = ParameterTool.fromArgs(args);
            EnvironmentSettings settings = EnvironmentSettings
                    .newInstance()
                    .inBatchMode()
                    .build();

         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

         String filePath="";
         // Hadoop parquet files {,,}
         // Prepare schema with fields to be read from file, ignoring other fields
         MessageType parquetSchema = new MessageType("input_schema",
                 Types.optional(INT32).named("id"),
                 Types.optional(INT32).named("int_col"),
                 Types.optional(DOUBLE).named("double_col"),
        //         Types.optional(BINARY).as(stringType()).named("date_string_col"),
                 Types.optional(BOOLEAN).named("bool_col")
         );

         //Schema avroSchema = UserRank.getClassSchema();
         Schema avroSchema = new Schema.Parser().parse(new File("/tmp/user.avsc"));
         //MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);

         UserRank dataToWrite[] = new UserRank[]{
                 new UserRank(1, 3),
                 new UserRank(2, 0),
                new UserRank(3, 100)
         };


/*
         org.apache.hadoop.fs.Path filePath2 = new org.apache.hadoop.fs.Path("/tmp/example.parquet");
         int blockSize = 1024;
         int pageSize = 65535;
         try(
                 AvroParquetWriter parquetWriter = new AvroParquetWriter(
                         filePath2,
                         avroSchema,
                         CompressionCodecName.SNAPPY,
                         blockSize,
                         pageSize)
         ){
             for(UserRank obj : dataToWrite){
                 parquetWriter.write(obj);
             }
         }catch(java.io.IOException e){
             System.out.println(String.format("Error writing parquet file %s", e.getMessage()));
             e.printStackTrace();
         }
*/
     }

         //final FileSource<String> source =
         //        FileSource.forRecordStreamFormat(new ParquetFileReader(fileName), /* Flink Path */)
         //                .build();

         //MessageType parquetSchema2 = // use parquet libs to provide the parquet schema file and parse it or extract it from the parquet files
         //ParquetRowInputFormat parquetInputFormat = new ParquetRowInputFormat(new Path(filePath),  parquetSchema);
// project only needed fields if suited to reduce the amount of data. Use: parquetSchema#selectFields(projectedFieldNames);
         //DataSet<Row> input = env.createInput(parquetInputFormat);



         //FileSource<CabsPojo> mySource = FileSource.forRecordStreamFormat(new CsvInputFormat<CabsPojo>())
         //DataStreamSource<String> source = env.fromSource(FileSource
         //        .forRecordStreamFormat(new TextLineFormat(),
         //                new Path("file:///Users/user/file.txt")).build(),
          //       WatermarkStrategy.noWatermarks(), "MySourceName")

         //DataStream<CabsPojo> stream = env.fromSource(
         //        mySource,
         //        WatermarkStrategy.noWatermarks(),
          //       "MySourceName");
         //final FileSource<String> source =
         //        FileSource.forRecordStreamFormat(new TextLineInputFormat(), /* Flink Path */)
         //                .build();
         //final DataStream<String> stream =
         //        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");


         //Function<CsvMapper, CsvSchema> schemaGenerator = mapper ->
         //        mapper.schemaFor(CabsPojo.class).withoutQuoteChar().withColumnSeparator('|');

         //CsvReaderFormat<CabsPojo> csvFormat =
         //        CsvReaderFormat.forSchema(() -> new CsvMapper(), schemaGenerator, TypeInformation.of(CabsPojo.class));

         //FileSource<CabsPojo> source =
         //        FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(new File(fileName))).build();

         //DataStream<CabsPojo> data1 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csv-source");
         //DataStream<CabsPojo> data2 = env.fromSource<CabsPojo>();
        //tableEnv.createTemporaryTable("Cabs", source);
            //TableEnvironment tableEnv = TableEnvironment.create(settings);
          //  final Schema schema = Schema.newBuilder()
          //          .column("cab_id", DataTypes.INT())
          //          .column("cab_plate", DataTypes.STRING())
          //          .column("cab_make", DataTypes.STRING())
           //         .column("cab_driver", DataTypes.STRING())
          //          .column("active_trip", DataTypes.STRING())
          //          .column("pickup_location", DataTypes.STRING())
          //          .column("target_location", DataTypes.STRING())
          //          .column("num_pass", DataTypes.INT())
          //          .build();


           // tableEnv.createTemporaryTable("cabs",
           //         TableDescriptor
           //                 .forConnector("filesystem")
           //                 .schema(schema)
           //                 .option("path", fileName)
           //                 .format(FormatDescriptor.forFormat("csv").build())
            //                .build());




         //Table sqlResult = tableEnv.sqlQuery("SELECT * FROM cabs");
         //sqlResult.execute().print();
         //tabEnv.toRetractStream(aggTableSql, Row.class).print("aggTableSql");
        // tableEnv.createTemporaryView("myview",sqlResult);
         //Table lowerTable = tableEnv.sqlQuery("SELECT LOWER(cab_driver) FROM myview");
         //Table lowerTable1 = tableEnv.sqlQuery("SELECT cab_driver FROM myview");
         //Table lowerTable2 = tableEnv.sqlQuery("SELECT cab_id FROM myview");
         //Table lowerTable3 = tableEnv.sqlQuery("SELECT * FROM myview");
         //DataStream<Row> lowerStream = tableEnv.toDataStream(lowerTable);
         //DataStream<Row> lowerStream1 = tableEnv.toDataStream(lowerTable1);
         //DataStream<Row> lowerStream2 = tableEnv.toDataStream(lowerTable2);
         //DataStream<Row> lowerStream3 = tableEnv.toDataStream(lowerTable3);
         //lowerStream.print("L");
         //lowerStream.print();
         //lowerStream1.print();
         //lowerStream2.print();
         //lowerStream3.print();
         //env.execute();

         //Ne marche pas a la compilation
         //Table result = tableEnv.from("cabs").select("*");
         //result.execute().print();
         // *******************************
         // Ne marche pas a l execution
         //TableResult table2 = tableEnv.executeSql("select * from ATHLETES");
         //TableResult result = tableEnv.executeSql("select * from cabs");
         //   result.print();
         // *******************************
        //}

}
