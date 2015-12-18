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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
import java.util.zip.ZipException;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class ParamBulkLoad extends Configured implements Tool {

    public static byte[]  CQ_SN        = Bytes.toBytes("S");
    public static byte[]  CQ_OPTIME    = Bytes.toBytes("O");
    public static byte[]  CQ_ID        = Bytes.toBytes("I");
    public static byte[]  CQ_RDFNAME   = Bytes.toBytes("F");
    public static byte[]  CQ_PARAMID   = Bytes.toBytes("R");
    public static byte[]  CQ_PARAMTYPE = Bytes.toBytes("Y");
    public static byte[]  CQ_VALID     = Bytes.toBytes("D");
    public static byte[]  CQ_TS        = Bytes.toBytes("T");
    public static byte[]  CQ_VAL       = Bytes.toBytes("V");
    //    public static byte[]  CQ_RDFCOUNT  = Bytes.toBytes("X");

    public static int  RDFNAME_INX   = 0;

    public static int DEFAULT_NUM_OF_SALT = 100;
    public static String  DEFAULT_PADDING_STRING="%02d";


    // rowkey triplet + first element from TS array
    public static int  SN_INX        = 1;
    public static int  OPTIME_INX    = 2;
    public static int  ID_INX        = 3;

    public static int  PARAMID_INX   = 4;
    public static int  PARAMTYPE_INX = 5;
    public static int  VALID_INX     = 6;
    public static int  TS_INX        = 7;
    public static int  VAL_INX       = 8;


    public static String TABLE_NAME = "custom.table.name";
    public static String COLUMN_FAMILY = "custom.column.family";
    public static String NUM_SALTS = "custom.numSalts";
    public static String FLEET_ID = "custom.fleetId";

    public int run(String[] args) throws Exception {

	if (args.length == 0) {
	    System.out.println("ParamBulkLoad {inputPath} {outputPath} {tableName} {columnFamily} {fleet id} {numSalts}");
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

	job.setJarByClass(ParamBulkLoad.class);
	job.setJobName("ParamBulkLoad: " + numSalts);

	// make it so reducers don't start until mappers are finished.
	job.getConfiguration().setDouble("mapred.slowstart.completed.maps",1.0);

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

	// System.out.println("About to chmod jprosser " );

	HFileUtils.changePermissionR(outputPath, hdfs);
	// System.out.println("DONE WITH chmod jprosser " );
	LoadIncrementalHFiles load = new LoadIncrementalHFiles(config);
	load.doBulkLoad(new Path(outputPath), hTable);
	// System.out.println("DONE WITH BULKLOAD " );
	return 1;
    }

    public static class CustomMapper extends Mapper<Writable, Text, ImmutableBytesWritable, KeyValue> {
	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;

	byte[] columnFamily;

	int taskId;
	short numSalts;
	int padLength=3;
	DateTimeFormatter formatter = null;
	String fleetId = null;
	@Override
	    public void setup(Context context) {

	    columnFamily = Bytes.toBytes(context.getConfiguration().get(COLUMN_FAMILY));
	    numSalts =  Short.parseShort(context.getConfiguration().get(NUM_SALTS));
	    fleetId =   context.getConfiguration().get(FLEET_ID);
	    taskId = context.getTaskAttemptID().getTaskID().getId();

	    //padLength = Integer.toString(numSalts-1).length();
	    //formatter  = ISODateTimeFormat.dateTime().withZone(DateTimeZone.getDefault());
	    formatter  = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
	}

	long counter = 0;

	@Override
	    public void map(Writable key, Text value, Context context)
	    throws IOException, InterruptedException {


	    String[] columnDetail = value.toString().split("\t", -1);

	    String rdfname   =columnDetail[RDFNAME_INX  ];
	    String sn        =columnDetail[SN_INX       ];
	    String optime    =columnDetail[OPTIME_INX   ];
	    String id        =columnDetail[ID_INX       ];
	    String paramid   =columnDetail[PARAMID_INX  ];
	    String paramtype =columnDetail[PARAMTYPE_INX];
	    String validZ64  =columnDetail[VALID_INX    ];
	    String tsZ64     =columnDetail[TS_INX       ];
	    String valZ64    =columnDetail[VAL_INX      ];

	    if("OPTIME".equals(optime)){
		//System.out.println("Skipping header " );
		return;
	    }

	    byte[]  valid=null;
	    byte[]  ts = null;
	    byte[]  val = null;
	    int[]   tsIntArray=null;
	    int[]   valIntArray=null;
	    float[] valFloatArray=null;
	    char[]  validCharArray=null;

	    int tsLength=0;

		byte[]  validZ = Base64.decodeBase64(validZ64);
		byte[]  tsZ    = Base64.decodeBase64(tsZ64);
		byte[]  valZ   = Base64.decodeBase64(valZ64);


//		System.out.println("validZ64 length "+ Integer.toString(validZ64.length()) );
//		System.out.println("validZ64 "+ validZ64 );
//		System.out.println("validZ length " + Integer.toString(validZ.length) );


		// Decompress the bytes  	    // TODO move this to its own private method
		//		ByteArrayOutputStream validBos = new ByteArrayOutputStream();

		int validLength=0;
		int valLength=0;
		try {
		ByteArrayOutputStream validBos=new ByteArrayOutputStream(validZ.length * 10 );
		OutputStream validOut=new InflaterOutputStream(validBos);
		validOut.write(validZ);
		valid = validBos.toByteArray();
		validLength=valid.length;

		ByteArrayOutputStream tsBos=new ByteArrayOutputStream(tsZ.length * 10 );
		OutputStream tsOut=new InflaterOutputStream(tsBos);
		tsOut.write(tsZ);
		ts = tsBos.toByteArray();
		tsLength=ts.length;


		ByteArrayOutputStream valBos=new ByteArrayOutputStream(valZ.length * 10 );
		OutputStream valOut=new InflaterOutputStream(valBos);
		valOut.write(valZ);
		val = valBos.toByteArray();
		valLength=val.length;
		}catch(java.util.zip.ZipException ex){

		    String fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
		    System.out.println("Processing file:: " + fileName);
		    System.out.println("got Exception:: " + ex);


		    return;
		}
		int tsd = 0;

		if(tsLength > 0) {
		    tsd  = ByteBuffer.wrap(ts,0,4).order(ByteOrder.LITTLE_ENDIAN).getInt(0);

		    // IntBuffer intBuf = ByteBuffer.wrap(ts).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();

		    // tsIntArray = new int[intBuf.remaining()];
		    // intBuf.get(tsIntArray);

		    //System.out.println(" first ts was  " + Integer.toString(tsd) );
		    //System.out.println(Arrays.toString(tsIntArray));

		}else{
		    //System.out.println("tsLength was 0 " );
		}

		// if(validLength > 0) {

		//     int valid1 = (int)  ByteBuffer.wrap(valid,0,1).order(ByteOrder.LITTLE_ENDIAN).get(0);
		//     //System.out.println("first valid was " + Integer.toString(valid1) );

		//     CharBuffer charBuf = ByteBuffer.wrap(valid).order(ByteOrder.LITTLE_ENDIAN).asCharBuffer();

		//     validCharArray = new char[charBuf.remaining()];
		//     charBuf.get(validCharArray);

		//     //System.out.println(Arrays.toString(validCharArray));

		// }else{
		//     //System.out.println(" valid length was 0 " );
		// }

		// if(valLength > 0) {
		//     if( "1".equals(paramid)) {
		// 	FloatBuffer floatBuf = ByteBuffer.wrap(val).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();

		// 	valFloatArray = new float[floatBuf.remaining()];
		// 	floatBuf.get(valFloatArray);

		// 	//System.out.println(Arrays.toString(valFloatArray));
		//     }else{

		// 	IntBuffer intBuf = ByteBuffer.wrap(val).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();

		// 	valIntArray = new int[intBuf.remaining()];
		// 	intBuf.get(valIntArray);

		// 	//System.out.println(Arrays.toString(valIntArray));
		//     }

		// }else{
		//     //System.out.println("val length was 0 " );
		// }


	    // TODO move this to its own private method
	    Long convOptime = Long.MAX_VALUE - formatter.parseDateTime(optime).getMillis();
	    String hashInput= fleetId + sn;
	    short hashVal = (short)Math.abs(hashInput.hashCode() % numSalts);

	    byte[] hashValBytes = ByteBuffer.allocate(2).putShort(hashVal).array();

	    // tsd does not need to be padded as ordering is not necessary. All the ts + val arrays are merged, zipped, sorteda and unzipped later.
	    String logicalKey =  fleetId + "|" + sn + "|" + Long.toString(convOptime) + "|" + id+ "|" + Integer.toString(tsd);
	    byte[] logicalKeyBytes = Bytes.toBytes(logicalKey);
	    
	    byte[] rowKeyBytes = new byte[hashValBytes.length + logicalKeyBytes.length];

	    System.arraycopy(hashValBytes, 0, rowKeyBytes, 0, hashValBytes.length);
	    System.arraycopy(logicalKeyBytes, 0, rowKeyBytes, hashValBytes.length, logicalKeyBytes.length);


	    //String rowKey =  + "|" + logicalKey ;
	    //	    String rowKey = StringUtils.leftPad(Integer.toString(Math.abs(hashInput.hashCode() % numSalts)), padLength, '0') + "|" + logicalKey ;
	    //System.out.println("rowkey is " + rowKey);

	    hKey.set(rowKeyBytes);

	    //create new kvs and and add them
	    kv = new KeyValue(hKey.get(), columnFamily, CQ_RDFNAME,  Bytes.toBytes(rdfname));
	    context.write(hKey, kv);

	    kv = new KeyValue(hKey.get(), columnFamily, CQ_PARAMID,  Bytes.toBytes(paramid));
	    context.write(hKey, kv);

	    kv = new KeyValue(hKey.get(), columnFamily, CQ_PARAMTYPE,  Bytes.toBytes(paramtype));
	    context.write(hKey, kv);

	    kv = new KeyValue(hKey.get(), columnFamily, CQ_VALID,  valid);
	    context.write(hKey, kv);

	    kv = new KeyValue(hKey.get(), columnFamily, CQ_TS,  ts   );
	    context.write(hKey, kv);

	    kv = new KeyValue(hKey.get(), columnFamily, CQ_VAL,  val  );
	    context.write(hKey, kv);
	}
    }


    public static void main(String[] argv) throws Exception {
	int ret = ToolRunner.run(new ParamBulkLoad(), argv);
	System.exit(ret);
    }


}

