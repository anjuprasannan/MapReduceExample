package com.anjus.mapreduce.semproject;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;

public class CropDemandReducer extends Reducer<Text, Text, Text, Text> {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CropDemandReducer.class);
	private DoubleWritable result = new DoubleWritable();
	DecimalFormat f = new DecimalFormat("##.00");
	double max = 0;
	List<String> valueList = new ArrayList<String>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) {
		try {
			double sum = 0;
			LOG.info("Inside reducer");
			// 1, find the max of crop production
			for (Text count : values) {
				double countValue = Double.parseDouble(count.toString());
				max = Math.max(max, countValue);
				double normValue = (countValue / max) * 10.0;
				if (countValue > 0.0) {
					valueList.add(String.valueOf(f.format(normValue)));
				}
			}
			result.set(max);

			if (valueList.size() > 0) {
				for (String value : valueList) {
					sum += Double.parseDouble(value);
				}
				// 2, write out the crop and the max of production
				// context.write(new Text(key.toString() + "#" + valueList), new
				// Text(String.valueOf(result)));
				context.write(key, new Text(String.valueOf(f.format(sum))));
			}
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		}

	}
}
