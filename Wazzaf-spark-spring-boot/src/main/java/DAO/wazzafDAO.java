/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DAO;

import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Mahmoud Eidarous
 */
public class wazzafDAO {
    private SparkSession sparkSession;
    public Dataset<Row> readData(String path)
    {
        Dataset<Row> data;
        data = sparkSession.read().option("header", "true").csv(path);
        
        return data;
    }
    
}
