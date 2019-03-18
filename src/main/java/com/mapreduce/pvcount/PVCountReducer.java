/**
 * 
 */
package com.mapreduce.pvcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author lyl
 *
 */
public class PVCountReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	
	private IntWritable outputValue = new IntWritable();
	
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		outputValue.set(count);
		context.write(key, outputValue);
	}
}