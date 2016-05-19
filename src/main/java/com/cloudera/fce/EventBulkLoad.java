package com.cloudera.fce;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.Arrays;
    
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.CharBuffer;
import java.nio.FloatBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.commons.codec.binary.Base64;

import org.apache.commons.lang.StringUtils;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class EventBulkLoad extends Configured implements Tool {

    public static byte[]  CQ_RDFNAME    = Bytes.toBytes("F");
    public static byte[]  CQ_SN         = Bytes.toBytes("SN");
    public static byte[]  CQ_OPTIME     = Bytes.toBytes("OT");
    public static byte[]  CQ_EID        = Bytes.toBytes("EI");
    public static byte[]  CQ_EVENTNAME  = Bytes.toBytes("EN");
    public static byte[]  CQ_TS         = Bytes.toBytes("T");
    public static byte[]  CQ_DUR        = Bytes.toBytes("D");
    public static byte[]  CQ_EXCEEDANCE = Bytes.toBytes("X");
    public static byte[]  CQ_PILOTINIT  = Bytes.toBytes("PI");
    public static byte[]  CQ_HASLDS     = Bytes.toBytes("HL");
    public static byte[]  CQ_EVENTTYPE  = Bytes.toBytes("ET");
    public static byte[]  CQ_EVENTFUNC  = Bytes.toBytes("EF");
    public static byte[]  CQ_INSERIDX   = Bytes.toBytes("II");
    public static byte[]  CQ_LDSDATA    = Bytes.toBytes("LD");

    public static int DEFAULT_NUM_OF_SALT = 100;
    public static String  DEFAULT_PADDING_STRING="%02d";
      


    public static int  RDFNAME_INX     = 0;
    public static int  SN_INX          = 1;
    public static int  OPTIME_INX      = 2;
    public static int  EID_INX         = 3;
    public static int  EVENTNAME_INX   = 4;
    public static int  TS_INX          = 5;
    public static int  DUR_INX         = 6;
    public static int  EXCEEDANCE_INX  = 7;
    public static int  PILOTINIT_INX   = 8;
    public static int  HASLDS_INX      = 9;
    public static int  EVENTTYPE_INX   = 10;
    public static int  EVENTFUNC_INX   = 11;
    public static int  INSERIDX_INX    = 12;
    public static int  LDSDATA_INX     = 13;

    public static String TABLE_NAME = "custom.table.name";
    public static String COLUMN_FAMILY = "custom.column.family";
    public static String NUM_SALTS = "custom.numSalts";
    public static String FLEET_ID = "custom.fleetId";
	
    public int run(String[] args) throws Exception {
		
	if (args.length == 0) {
	    System.out.println("EventBulkLoad {inputPath} {outputPath} {tableName} {columnFamily} {fleet id} {numSalts}");
	    return 1;
	}
		
	String inputPath = args[0];
	String outputPath = args[1];
	String tableName = args[2];
	String columnFamily = args[3];
	String fleetId = args[4];
	String numSalts = args[5];

	// Create job
	Job job = Job.getInstance();

	job.setJarByClass(EventBulkLoad.class);
	job.setJobName("EventBulkLoad: " + numSalts);
		
	job.getConfiguration().set(TABLE_NAME, tableName);
	job.getConfiguration().set(COLUMN_FAMILY, columnFamily);
	job.getConfiguration().set(NUM_SALTS, numSalts);
	job.getConfiguration().set(FLEET_ID, fleetId);
		
	// Define input format and path
	job.setInputFormatClass(TextInputFormat.class);
	TextInputFormat.addInputPath(job, new Path(inputPath));

	Configuration config = HBaseConfiguration.create();

	HTable hTable = new HTable(config, tableName);

	// Auto configure partitioner and reducer
	HFileOutputFormat.configureIncrementalLoad(job, hTable);
	FileOutputFormat.setOutputPath(job, new Path(outputPath));

	// Define the mapper and reducer
	job.setMapperClass(CustomMapper.class);

	// Define the key (will be the rowkey) and value (will be keyvalue/CQ)
	job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	job.setMapOutputValueClass(KeyValue.class);

	// Exit
	job.waitForCompletion(true);
	FileSystem hdfs = FileSystem.get(config);

	// Must all HBase to have write access to HFiles

	System.out.println("About to chmod " );
	
	HFileUtils.changePermissionR(outputPath, hdfs);
	System.out.println("DONE WITH chmod jprosser " );
	LoadIncrementalHFiles load = new LoadIncrementalHFiles(config);
	load.doBulkLoad(new Path(outputPath), hTable);
	System.out.println("DONE WITH BULKLOAD " );
	return 1;
    }

    public static class CustomMapper extends Mapper<Writable, Text, ImmutableBytesWritable, KeyValue> {
	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;

	byte[] columnFamily;
		
	int taskId;
	int numSalts;
	int padLength=3;
	DateTimeFormatter formatter = null;
	String fleetId = null;
	@Override
	    public void setup(Context context) {
	    columnFamily = Bytes.toBytes(context.getConfiguration().get(COLUMN_FAMILY));
	    numSalts =  Integer.parseInt(context.getConfiguration().get(NUM_SALTS));
	    taskId = context.getTaskAttemptID().getTaskID().getId();
	    fleetId =   context.getConfiguration().get(FLEET_ID);
	    //padLength = Integer.toString(numSalts-1).length();
	    formatter  = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
	}

	long counter = 0;

	@Override
	    public void map(Writable key, Text value, Context context)
	    throws IOException, InterruptedException {

	    String[] columnDetail = value.toString().split("\t", -1);

	    String rdf        =columnDetail[RDFNAME_INX   ];
	    String sn         =columnDetail[SN_INX        ];
	    String optime     =columnDetail[OPTIME_INX    ];
	    String eid        =columnDetail[EID_INX       ];
	    String eventname  =columnDetail[EVENTNAME_INX ];
	    String ts         =columnDetail[TS_INX        ];
	    String dur        =columnDetail[DUR_INX       ];
	    String exceedance =columnDetail[EXCEEDANCE_INX];
	    String pilot      =columnDetail[PILOTINIT_INX ];
	    String haslds     =columnDetail[HASLDS_INX    ];
	    String eventtype  =columnDetail[EVENTTYPE_INX ];
	    String eventfunc  =columnDetail[EVENTFUNC_INX ];
	    String inseridx   =columnDetail[INSERIDX_INX  ];
	    String ldsdata    =columnDetail[LDSDATA_INX   ];

	    if("OPTIME".equals(optime)){
		System.out.println("Skipping header " );
		return;
	    }
	    
	    // TODO move this to its own private method
	    Long opTimeMillis=formatter.parseDateTime(optime).getMillis();
	    Long convOptime = Long.MAX_VALUE - opTimeMillis;

	    String hashInput= fleetId + sn;
	    short hashVal = (short)Math.abs(hashInput.hashCode() % numSalts);
	    byte[] hashValBytes = ByteBuffer.allocate(2).putShort(hashVal).array();

	    Long   tsd =  formatter.parseDateTime(ts).getMillis() - opTimeMillis;
	    String logicalKey =  fleetId + "|" + sn + "|" + Long.toString(convOptime) + "|" + eid+ "|" + Long.toString(tsd);
	    String rowKey = StringUtils.leftPad(Integer.toString(Math.abs(sn.hashCode() % numSalts)), padLength, '0') + "|" + logicalKey ;

	    byte[] logicalKeyBytes = Bytes.toBytes(logicalKey);
	    
	    byte[] rowKeyBytes = new byte[hashValBytes.length + logicalKeyBytes.length];

	    System.arraycopy(hashValBytes, 0, rowKeyBytes, 0, hashValBytes.length);
	    System.arraycopy(logicalKeyBytes, 0, rowKeyBytes, hashValBytes.length, logicalKeyBytes.length);

	    hKey.set(rowKeyBytes);

	    //create new kvs and and add them
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_RDFNAME   ,Bytes.toBytes(rdf       ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_SN        ,Bytes.toBytes(sn        ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_OPTIME    ,Bytes.toBytes(optime    ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_EID       ,Bytes.toBytes(eid       ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_EVENTNAME ,Bytes.toBytes(eventname ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_TS        ,Bytes.toBytes(ts        ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_DUR       ,Bytes.toBytes(dur       ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_EXCEEDANCE,Bytes.toBytes(exceedance));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_PILOTINIT     ,Bytes.toBytes(pilot     ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_HASLDS    ,Bytes.toBytes(haslds    ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_EVENTTYPE ,Bytes.toBytes(eventtype ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_EVENTFUNC ,Bytes.toBytes(eventfunc ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_INSERIDX  ,Bytes.toBytes(inseridx  ));
	    context.write(hKey, kv);
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_LDSDATA   ,Bytes.toBytes(ldsdata   ));
	    context.write(hKey, kv);

	}
    }


    public static void main(String[] argv) throws Exception {
	int ret = ToolRunner.run(new EventBulkLoad(), argv);
	System.exit(ret);
    }


}
