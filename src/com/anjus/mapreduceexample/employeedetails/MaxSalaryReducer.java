package com.anjus.mapreduceexample.employeedetails;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxSalaryReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void reduce(IntWritable key3, Iterable<IntWritable> values3, Context context)
			throws IOException, InterruptedException {
		int max = 0;
		// 1, find the sum of departmental wages
		for (IntWritable count : values3) {
			max = Math.max(max, count.get());
		}
		// 2, write out the department number and the sum of department employees’
		// salaries
		context.write(key3, new IntWritable(max));

	}
}
