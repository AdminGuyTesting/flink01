package com.adminguytesting.flink01.Avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
//import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ParquetAvroHandler
{
    //private static final Schema SCHEMA;
    //final Path OUTPUT_PATH = new Path("result.parquet");
    private static final Logger logger = LoggerFactory.getLogger(ParquetAvroHandler.class);



    public ParquetAvroHandler(){
        //System.out.println("Init done");
    }

    /**
     * Reads an existing Apache Avro-based Parquet file from the
     * specified location and prints it into the system console
     *
     * @param mySchemaFile path to the Schema file
     * @param myDataFile path to the datafile to be read and shown on screen
     * @throws IOException
     **/
    public void read(String mySchemaFile, String myDataFile) throws IOException
    {
        Schema schema=null;
        try {
            schema = new Schema.Parser().parse(new File(mySchemaFile));
        } catch (Exception e) {
            System.out.println("Erreur de Schema");

        }
        // Deserialize users from disk
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        try {
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(myDataFile), datumReader);
            GenericRecord user = null;
            while (dataFileReader.hasNext()) {
                // Reuse user object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        } catch (Exception e) {
            System.out.println("Couldn't read from file " + myDataFile + " with schema: " + mySchemaFile);
            logger.error("Can't read SCHEMA file from {}", mySchemaFile);
            logger.error(e.getLocalizedMessage());
            e.printStackTrace();
            throw new RuntimeException("Can't read SCHEMA file from " + mySchemaFile, e);
        }
    }

    /**
     * Creates a new Apache Avro-based Parquet file or overwrites the existing one
     *
     * @param records set of records to write to the file
     * @param mySchemaFile path to the Schema file  as a string
     * @param dataFile path to the datafile to write the data to as a string
     * @param records List of Records to be saved
     * @throws IOException
     **/
    public void write(String mySchemaFile, String dataFile, List<GenericRecord> records) {
        DatumWriter<GenericRecord> datumWriter=null;
        Schema schema=null;

        try {
            schema = new Schema.Parser().parse(new File(mySchemaFile));
            datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        } catch (Exception e){
            System.out.println("Could nt parse schema file " + mySchemaFile);
            logger.error("Can't read SCHEMA file from {}", mySchemaFile);
            logger.error(e.getLocalizedMessage());
            e.printStackTrace();
            throw new RuntimeException("Can't read SCHEMA file from " + mySchemaFile, e);
        }

        File file = new File(dataFile);

        try {
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            dataFileWriter.create(schema, file);
            for(GenericRecord record:records){
                dataFileWriter.append(record);
            }
            dataFileWriter.close();
        } catch (Exception e){
            System.out.println("Couldn't write to file " + dataFile + e.getMessage());
            logger.error("Can't use the Schema file {}", mySchemaFile);
            logger.error(e.getLocalizedMessage());
            e.printStackTrace();
            throw new RuntimeException("Can't read SCHEMA file from " + mySchemaFile, e);
        }

    }

    public List<GenericRecord> getSampleData(String mySchemaFile){
        GenericRecord user1=null,user2=null,user3=null;
        Schema schema;
        List<GenericRecord> thelist = new ArrayList<GenericRecord>();
        try {
            schema = new Schema.Parser().parse(new File(mySchemaFile));

            //Using this schema, let's create some users.
            user1 = new GenericData.Record(schema);
            user1.put("Name", "John");
            user1.put("Id", 1);
            user1.put("PhoneNumber", "555-555-5551");
            user1.put("ZipCode", 88888);
            user1.put("isAlive", true);



            user2 = new GenericData.Record(schema);
            user2.put("Name", "Jane");
            user2.put("Id", 2);
            user2.put("PhoneNumber", "555-555-5552");
            user2.put("ZipCode", 99999);
            user2.put("isAlive", false);


            user3 = new GenericData.Record(schema);
            user3.put("Name", "Daniel");
            user3.put("Id", 3);
            user3.put("PhoneNumber", "066060606");
            user3.put("ZipCode", 67200);
            user3.put("isAlive", true);

            thelist.add(user1);
            thelist.add(user2);
            thelist.add(user3);
        } catch (Exception e){
            System.out.println("Could nt parse schema file " + mySchemaFile +" -- "+ e.getMessage());
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        return thelist;
    }


    public void runTest01() throws Exception
    {
        String dataFile = "saved_users2.avro";
        String schemaFile = "schema.avsc";

        write(schemaFile,dataFile,getSampleData(schemaFile));
        read(schemaFile,dataFile);
      }
}