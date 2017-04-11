/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.flinkconsumer;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.net.ntp.TimeStamp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
/**
 *
 * @author jafernandez
 */
public class FlinkConsumer {
    private static String hbaseZookeeperQuorum="lug041.zylk.net,lug040.zylk.net";
    private static String hbaseZookeeperClientPort="2181";
    private static TableName tableName = TableName.valueOf("kafkaMessages");

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // create execution environment
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	// parse user parameters
	ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // create datastream with the data coming from Kafka
        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer09<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));
        messageStream.rebalance().map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String message) throws Exception {
                writeIntoHBase(message);
                System.out.println("Message written into HBase.");
                return "Kafka dice: " + message;
            }
        }).print();

        try {
            env.execute();
        } catch (Exception ex) {
            Logger.getLogger(FlinkConsumer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void writeIntoHBase(String m) throws IOException{
        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
        config.set("hbase.zookeeper.property.clientPort", hbaseZookeeperClientPort);
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        Connection c = ConnectionFactory.createConnection(config);
        // Check if table exists
        Admin admin = c.getAdmin();
        if (!admin.tableExists(tableName)) {
            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor("messageJava")));
        }
        // Instantiating Table class
        // (deprecated) HTable hTable = new HTable(config, tableName);
        Table t = c.getTable(tableName);
        // Instantiating Put class. Accepts a row key.
        TimeStamp ts = new TimeStamp(new Date());        
        Date d = ts.getDate();        
        Put p = new Put(Bytes.toBytes(d.toString()));
        // adding values using addColumn() method. Accepts column family name, qualifier/row name ,value.
        // (deprecated) p.addColumn(Bytes.toBytes("messageJava"),Bytes.toBytes("java"),Bytes.toBytes(m));
        p.addColumn(Bytes.toBytes("messageJava"),Bytes.toBytes("java"),Bytes.toBytes(m));
        // Saving the put Instance to the Table.
        t.put(p);
        // closing Table & Connection
        t.close();
        c.close();
    }
}