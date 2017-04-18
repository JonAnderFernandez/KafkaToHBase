/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import com.bigdata.parameters.Parameters;
/**
 *
 * @author jafernandez
 */
public class HBaseConexion {
    private static HBaseConexion hbaseConexion = new HBaseConexion();
    
    private static String hbaseZookeeperQuorum = Parameters.HBASE_ZOOKEEPER_QUORUM;
    private static String hbaseZookeeperClientPort = Parameters.HBASE_ZOOKEEPER_CLIENT_PORT;
    
    private HBaseConexion(){ }
    
    public static HBaseConexion getInstance(){
        return hbaseConexion;
    }
    
    public static Connection getConnection() throws IOException{
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
        config.set("hbase.zookeeper.property.clientPort", hbaseZookeeperClientPort);
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        Connection c = ConnectionFactory.createConnection(config);
        return c;
    }
}