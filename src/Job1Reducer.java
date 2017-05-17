/***
 * Class Job1Reducer Job1 Reducer class
 * 
 * @author sgarouachi
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.base.Joiner;

public class Job1Reducer extends Reducer<Text, Text, Text, Text> {

  /**
   * Job1 Reduce method (page, 1.0 \t outLinks) Remove redundant links & sort them Asc
   */
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
      InterruptedException {
    // Remove redundant outLinks
    List<Text> outLinks = new ArrayList<>();
    for (Text value : values) {
      if (!outLinks.contains(value))
        outLinks.add(new Text(value));
    }
    // Sort outLinks by ascending order
    Collections.sort(outLinks);
    System.out.println(outLinks);
    // Append default page rank + outLinks
    String links = Joiner.on(',').join(outLinks);
    Text result = new Text(Joiner.on('\t').join("1.0", links));
    context.write(key, result);
  }
}
