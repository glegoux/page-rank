/***
 * Class Job2Reducer Job2 Reducer class
 * 
 * @author glegoux
 */

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.base.Joiner;

public class Job2Reducer extends Reducer<Text, Text, Text, Text> {
  // Initialize dumping factor to 0.85
  private static final float damping = 0.85F;

  /**
   * Job2 Reduce method Calculate the new page rank
   */
  @Override
  public void reduce(Text page, Iterable<Text> values, Context context) throws IOException,
      InterruptedException {

    boolean existingPage = false;
    String[] split;
    float sumShareOtherPageRanks = 0;
    String links = "";
    String input;

    for (Text value : values) {
      input = value.toString();

      if (input.equals("!")) {
        existingPage = true;
        continue;
      }

      if (input.startsWith("|")) {
        links = input.substring(1);
        continue;
      }

      split = input.split("\t");
      float pageRank = Float.parseFloat(split[1]);
      int countOutLinks = Integer.parseInt(split[2]);

      // Add the share to sumShareOtherPageRanks
      sumShareOtherPageRanks += (pageRank / countOutLinks);
    }

    if (!existingPage) {
      return;
    }


    // Write to output (page, rank \t outLinks)

    float pageRank = (1 - damping) + damping * sumShareOtherPageRanks;
    String formattedPageRank = String.format(java.util.Locale.US, "%.4f", pageRank);

    String result = links.isEmpty() ? formattedPageRank : Joiner.on("\t").join(formattedPageRank, links);
    context.write(new Text(page), new Text(result));

  }

}
