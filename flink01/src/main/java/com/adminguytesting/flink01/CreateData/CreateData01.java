package com.adminguytesting.flink01.CreateData;

import org.apache.flink.table.api.DataTypes;

import java.io.FileWriter;
import java.io.PrintWriter;

public class CreateData01 {
    public void createFileInProvidedFullPath(String fileName) throws Exception{
        String sep= ",";
        FileWriter fileWriter = new FileWriter(fileName);
        PrintWriter printWriter = new PrintWriter(fileWriter);

        //----------------------------------------------------------
        //printWriter.print("Some String");
        //printWriter.printf("Product name is %s and its price is %d $", "iPhone", 1000);
        printWriter.println(
                "cab_id"+sep+
                "cab_plate"+sep+
                "cab_make"+sep+
                "cab_driver"+sep+
                "active_trip"+ sep+
                "pickup_location"+sep+
                "target_location"+sep+
                "num_pass"
        );

        printWriter.println(
                "DataTypes.INT"+sep+
                "DataTypes.STRING"+sep+
                "DataTypes.STRING"+sep+
                "DataTypes.STRING"+sep+
                "DataTypes.STRING"+sep+
                "DataTypes.STRING"+sep+
                "DataTypes.STRING"+sep+
                "DataTypes.INT"
        );
        printWriter.println(
                "100"+sep+
                "497PL"+sep+
                "citroen"+sep+
                "Jose"+sep+
                "trip to job"+ sep+
                "Schilti"+sep+
                "Illkiki"+sep+
                "111100"
        );
        //----------------------------------------------------------
        printWriter.close();

    }
}
