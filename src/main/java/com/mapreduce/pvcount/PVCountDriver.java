/**
 * 
 */
package com.mapreduce.pvcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lyl
 *
 */
public class PVCountDriver {

	static Logger logger = LoggerFactory.getLogger(PVCountDriver.class);

	/*
	 * 常用命令: 上传文件:hadoop fs -put pvcount.txt /pvcount/input 
	 * 执行命令: hadoop jar pvcount.jar com.mapreduce.pvcount.PVCountDriver /pvcount/input /pvcount/output
	 * 结果查看: hadoop fs -cat /pvcount/output/part-r-00000 
	 * 删除结果:hadoop dfs -rm -R -f /pvcount/output/
	 * jar包可以在任何一台hadoop上执行
	 */
	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		/* 本地调试 */
		/*
		 * args = new String[] { "hdfs://hadoopslave1:9000/pvcount/input",
		 * "hdfs://hadoopslave1:9000/pvcount/output" };
		 */

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(PVCountDriver.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(PVCountMapper.class);
		job.setReducerClass(PVCountReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}

}