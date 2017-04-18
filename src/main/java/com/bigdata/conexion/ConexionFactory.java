/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.conexion;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.hbase.client.Connection;
import com.bigdata.hbase.HBaseConexion;
/**
 *
 * @author jafernandez
 */
public class ConexionFactory extends BasePooledObjectFactory<Connection>{    
    // the override method are used internaly by the implementation of ObjectPool
    @Override
    public Connection create() throws Exception {
        return HBaseConexion.getInstance().getConnection();
    }

    @Override
    public PooledObject<Connection> wrap(Connection c) {
        return new DefaultPooledObject<Connection>(c);
    }
}