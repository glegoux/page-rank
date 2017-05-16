/***
 * Class Job2Mapper
 * Job2 Mapper class
 * @author sgarouachi
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	/**
	 * Job2 Map method Generates 3 outputs: Mark existing page: (pageI, !) Used
	 * to calculate the new rank (rank pageI depends on the rank of the inLink):
	 * (pageI, inLink \t rank \t totalLink) Original links of the page for the
	 * reduce output: (pageI, |pageJ,pageK...)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO if needed
		throw new UnsupportedOperationException("Job2Mapper: map: Not implemented yet");
	}
}
