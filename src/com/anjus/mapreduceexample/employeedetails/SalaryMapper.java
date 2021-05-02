package com.anjus.mapreduceexample.employeedetails;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 
 * @author Anju Prasannan
 * @date 2021/05/02
 *
 */

public class SalaryMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	@Override
	protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
		// 1, get data
		String line = value1.toString();
		// 2, split data
		String[] data = line.split(",");
		// 3, write out the data
		context.write(new IntWritable(Integer.parseInt(data[1])), new IntWritable(Integer.parseInt(data[2])));
	}
}