package com.cloudera.fce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
//import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
//import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
//import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.IOException;

public class CreateTable  extends Configured implements Tool {

    public int run(String[] args) throws IOException {
		
	if (args.length == 0) {
	    System.out.println("CreateTable {tableName} {columnFamilyName} {numregions} {salts per region}  {optional RegionSize}");
	    return 1;
	}
		
	String tableName = args[0];
	String columnFamilyName = args[1];
	String regionCount = args[2];
	String saltsPerRegion = args[3];
		
#	long regionMaxSize = 107374182400l;
	long regionMaxSize = 15032385536l;
		
	if (args.length > 4) {
	    regionMaxSize = Long.parseLong(args[4]);
	}

	setConf(HBaseConfiguration.create(getConf()));

	/** Connection to the cluster. A single connection shared by all application threads. */
	Connection connection = null;
	
	/** A lightweight handle to a specific table. Used from a single thread. */
	Table table = null;
	Admin admin=null;
	try {
	    // establish the connection to the cluster.
	    connection = ConnectionFactory.createConnection(getConf());
	    // retrieve a handle to the target table.
	    admin = connection.getAdmin();
	    System.out.println("got hbasadmin");

	    HTableDescriptor tableDescriptor = new HTableDescriptor(); 
	    tableDescriptor.setName(Bytes.toBytes(tableName));
	    System.out.println("got HTableDescriptor");

	    HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamilyName);
		
	    columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
	    columnDescriptor.setBlocksize(64 * 1024);
	    columnDescriptor.setBloomFilterType(BloomType.ROW);
		
	    tableDescriptor.addFamily(columnDescriptor);
		
	    tableDescriptor.setMaxFileSize(regionMaxSize);
	    tableDescriptor.setValue(tableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
	    
	    System.out.println(regionCount +" regions");
	    System.out.println(saltsPerRegion +" salts per region");
	    int numRegions = Integer.parseInt(regionCount);
	    int numSaltsPerRegion  = Integer.parseInt(saltsPerRegion);

	    int padLength = Integer.toString(numSaltsPerRegion * numRegions - 1).length();
	    
	    System.out.println(padLength +" padlength");

	    byte[][] splitKeys = new byte[numRegions][];
	    int counter = 0;
			
	    for( counter = 0; counter < numRegions; counter++) {
		String key = StringUtils.leftPad(Integer.toString(counter * numSaltsPerRegion), padLength, '0');
		splitKeys[counter] = Bytes.toBytes(key); 
		System.out.println(key +" adding key for " + splitKeys[counter].toString() );
	    }

	    admin.createTable(tableDescriptor, splitKeys);
	    
	} finally {
	    // close everything down
	    
	    if (table != null) table.close();
	    if (admin != null) admin.close();
	    if (connection != null) connection.close();
	}
	
	System.out.println("Done");
	return 0;
    }

    public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(new CreateTable(), argv);
    System.exit(ret);
  }
	
}

