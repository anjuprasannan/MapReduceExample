package com.anjus.mapreduceexample.employeedetails;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author Anju Prasannan
 * @date 2021/05/02
 *
 */

public class MaxSalaryJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop\\hadoop-3.2.1");
		Configuration conf = new Configuration();
		String inputFileName = "C:\\Users\\kbipi\\eclipse-workspace\\MapReduceExample\\src\\main\\resources\\employee_details_input\\employee_dept.csv";
		String outputFolder = "C:\\Users\\kbipi\\eclipse-workspace\\MapReduceExample\\src\\main\\resources\\employee_salary_output";
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "employee details");
		job.setJarByClass(MaxSalaryJob.class);
		job.setMapperClass(MaxSalaryMapper.class);
		job.setCombinerClass(MaxSalaryReducer.class);
		job.setReducerClass(MaxSalaryReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputFileName));
		FileOutputFormat.setOutputPath(job, new Path(outputFolder));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}