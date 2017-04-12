/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.hbase;

import java.util.Date;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.commons.pool2.ObjectPool;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
/**
 *
 * @author jafernandez
 */
public class HBase{        
    private ObjectPool<Connection> pool;
    
    private TableName tableName = TableName.valueOf("kafkaMessages");            
    
    public HBase(ObjectPool<Connection> pool){
        this.pool = pool;
    }   
    
    public void writeIntoHBase(String colFamily, String colQualifier, String value) 
            throws Exception{
        Connection c = null;  
        c = pool.borrowObject();
        Admin admin = c.getAdmin();
        if (!admin.tableExists(tableName)) {
            admin.createTable(new HTableDescriptor(tableName).addFamily(
                new HColumnDescriptor("messageJava")));
        }
        // Instantiating Table class
        Table t = c.getTable(tableName);
        // Instantiating Put class. Accepts a row key.
        TimeStamp ts = new TimeStamp(new Date());        
        Date d = ts.getDate();        
        Put p = new Put(Bytes.toBytes(d.toString()));
        // adding values using addColumn() method. 
        // Accepts column family name, qualifier/row name ,value.                
        p.addColumn(
            Bytes.toBytes(colFamily),
            Bytes.toBytes(colQualifier),
            Bytes.toBytes(value));
        // Saving the put Instance to the Table.
        t.put(p);
        // closing Table & Connection
        t.close();
        pool.returnObject(c);   
    }    
}