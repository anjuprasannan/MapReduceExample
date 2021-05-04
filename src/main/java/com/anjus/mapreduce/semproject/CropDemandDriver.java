package com.anjus.mapreduce.semproject;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Anju Prasannan
 * @date 2021/05/02
 *
 */

public class CropDemandDriver {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CropDemandDriver.class);

	public static void main(String[] args) {
		try {
			LOG.info("Inside driver");
			System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop\\hadoop-3.2.1");
			Configuration conf = new Configuration();
			String inputFileName = "C:\\Users\\kbipi\\eclipse-workspace\\MapReduceExample\\src\\main\\resources\\semproject_input\\apy.csv";
			String outputFolder = "C:\\Users\\kbipi\\eclipse-workspace\\MapReduceExample\\src\\main\\resources\\semproject_output";

			Path outputPath = new Path(outputFolder);

			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "Crop & Demand");
			FileSystem fs = FileSystem.get(outputPath.toUri(), job.getConfiguration());
			fs.delete(outputPath, true);
			job.setJarByClass(CropDemandDriver.class);
			job.setMapperClass(CropDemandMapper.class);
			job.setCombinerClass(CropDemandReducer.class);
			job.setReducerClass(CropDemandReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputFileName));
			FileOutputFormat.setOutputPath(job, new Path(outputFolder));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException | InterruptedException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		}
	}

}