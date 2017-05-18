/***
 * Class Job2Mapper Job2 Mapper class
 * 
 * @author glegoux
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Joiner;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {

  /**
   * Job2 Map method Generates 3 outputs: Mark existing page: (pageI, !) Used to calculate the new
   * rank (rank pageI depends on the rank of the inLink): (pageI, inLink \t rank \t totalLink)
   * Original links of the page for the reduce output: (pageI, |pageJ,pageK...)
   */
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
    // Parse input value
    String[] values = value.toString().split("\t");
    String sourcePage = values[0];
    String pageRank = values[1];
    String originalLinks = values.length == 3 ? values[2] : "";
    // Mark existing page
    context.write(new Text(sourcePage), new Text("!"));
    // Mark page rank
    if (originalLinks.isEmpty()) {
      return;
    }
    String[] outLinks = originalLinks.split(",");
    int numberOfLinks = outLinks.length;
    for (String page : outLinks) {
      String valueOut = Joiner.on("\t").join(sourcePage, pageRank, numberOfLinks);
      context.write(new Text(page), new Text(valueOut));
    }
    // Original link
    context.write(new Text(sourcePage), new Text("|" + originalLinks));
  }
}
