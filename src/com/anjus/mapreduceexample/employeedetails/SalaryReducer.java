package com.anjus.mapreduceexample.employeedetails;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 
 * @author Anju Prasannan
 * @date 2021/05/02
 *
 */

public class SalaryReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void reduce(IntWritable key3, Iterable<IntWritable> values3, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		// 1, find the sum of departmental wages
		for (IntWritable count : values3) {
			sum += count.get();
		}
		// 2, write out the department number and the sum of department employees’
		// salaries
		context.write(key3, new IntWritable(sum));

	}
}