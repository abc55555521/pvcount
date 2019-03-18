/**
 * 
 */
package com.mapreduce.pvcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author lyl
 *
 */
public class PVCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private IntWritable outputKey = new IntWritable();
	private IntWritable outputValue = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String line=value.toString();
		String[] fields=line.split("\t");
		
		if(fields.length<30) {
			context.getCounter("PV_COUNT","LENGTH_LESS_30").increment(1);
			return;
		}
		
		String url=fields[1];
		
		if(StringUtils.isBlank(url)) {
			context.getCounter("PV_COUNT","URL_NULL").increment(1);
			return;
		}
		
		String provinceId=fields[23];
		
		Integer provinceTest=Integer.MIN_VALUE;
		
		try {
			provinceTest=Integer.valueOf(provinceId);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			context.getCounter("PV_COUNT", "PROVINCE_ID_ERROR").increment(1);
			return;
		}
		
		outputKey.set(provinceTest);
		context.write(outputKey, outputValue);
		
	}
}