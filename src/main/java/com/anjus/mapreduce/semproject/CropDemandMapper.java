package com.anjus.mapreduce.semproject;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.LoggerFactory;

public class CropDemandMapper extends Mapper<Object, Text, Text, Text> {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CropDemandMapper.class);

	@Override
	protected void map(Object key, Text value, Context context) {
		try {
			LOG.info("Inside mapper");
			String crop = "";
			String demand = "";
			// 1, get data
			String line = value.toString();
			// 2, split data
			String[] data = line.split(",");
			if ((data.length > 6) && (StringUtils.isNotEmpty(data[6].trim()))) {
				crop = data[4].trim();
				demand = data[6].trim();
				// 3, write out the data
				context.write(new Text(crop), new Text(demand));
			} else {
				LOG.error(line);
			}

		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		}
	}
}
