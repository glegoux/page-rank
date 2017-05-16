/***
 * Class Job1Reducer Job1 Reducer class
 * 
 * @author sgarouachi
 */

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<Text, Text, Text, Text> {
  
  

  /**
   * Job1 Reduce method (page, 1.0 \t outLinks) Remove redundant links & sort them Asc
   */
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
      InterruptedException {
    // TODO if needed
    // Remove redundant outLinks
    // Sort outLinks by Asc
    // Append default page rank + outLinks
    //Text result = new Text();  
    //result.set(new Text("PageA\t1.0\tPageB,PageC"));
    //context.write(key, result);
    System.out.println("END");
    // throw new UnsupportedOperationException("Job1Reducer: reduce: Not implemented yet");
  }
}
