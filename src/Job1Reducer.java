/***
 * Class Job1Reducer Job1 Reducer class
 * 
 * @author glegoux
 */

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.base.Joiner;

public class Job1Reducer extends Reducer<Text, Text, Text, Text> {

  /**
   * Job1 Reduce method (page, 1.0 \t outLinks) 
   * Remove redundant links & sort them Asc
   */
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
      InterruptedException {
    // Remove redundant outLinks and sort by ascending order
    Set<String> outLinks = new TreeSet<>();
    for (Text value : values) {
      outLinks.add(value.toString());
    }
    // Append default page rank + outLinks
    String links = Joiner.on(',').join(outLinks);
    String result = links.isEmpty() ? "1.0" : Joiner.on('\t').join("1.0", links);
    context.write(key, new Text(result));
  }
}
