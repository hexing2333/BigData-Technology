package org.zkpk.hbase.inputSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class Main {
	static final Log LOG = LogFactory.getLog(Main.class);
	
	//JobName
	public static final String NAME = "Member Test1";
	
	//\u8f93\u51fa\u76ee\u5f55
	public static final String TEMP_INDEX_PATH = "hdfs://master:9000/tmp/member_user";
	
	//HBase\u4f5c\u4e3a\u8f93\u5165\u6e90\u7684HBase\u4e2d\u7684\u8868 2018211582lzy
	public static String inputTable = "2018211582lzy";
	
	
	public static void main(String[] args)throws Exception{
		//1.\u83b7\u5f97HBase\u7684\u914d\u7f6e\u4fe1\u606f
		Configuration conf = HBaseConfiguration.create();
		
		//2.\u521b\u5efa\u5168\u8868\u626b\u63cf\u5668scan\u5bf9\u8c61
		Scan scan = new Scan();
		scan.setBatch(0);
		scan.setCaching(10000);
		scan.setMaxVersions();
		scan.setTimeRange(System.currentTimeMillis() - 3*24*3600*1000L, System.currentTimeMillis());
		
		//3.\u914d\u7f6escan\uff0c\u6dfb\u52a0\u626b\u63cf\u7684\u6761\u4ef6\uff0c\u5217\u65cf\u548c\u5217\u65cf\u540d\uff08\u8fd9\u91cc\u5217\u65cf\u540d\u4e3acf1\uff0c\u5217\u540dname\uff09
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("gender"));
		scan.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("height"));
		scan.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("weight"));
		
		//4.\u8bbe\u7f6ehadoop\u7684\u63a8\u6d4b\u6267\u884c\u4e3afalse
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		//5.\u8bbe\u7f6eHDFS\u7684\u5b58\u50a8HBase\u8868\u4e2d\u6570\u636e\u7684\u8def\u5f84
		
		Path tmpIndexPath = new Path(TEMP_INDEX_PATH);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(tmpIndexPath)){
			fs.delete(tmpIndexPath,true);
		}
		
		//6.\u521b\u5efajob\u5bf9\u8c61
		Job job = new Job(conf,NAME);
		
		//\u8bbe\u7f6e\u6267\u884cJOB\u7684\u7c7b
		job.setJarByClass(Main.class);
		
		TableMapReduceUtil.initTableMapperJob(inputTable, scan, MemberMapper.class, Text.class, Text.class, job);
		job.setNumReduceTasks(0);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job,tmpIndexPath);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
