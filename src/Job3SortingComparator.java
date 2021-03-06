/***
 * Class Job3SortingComparator Job3 Sorting Comparator class
 * 
 * @author glegoux
 */

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Job3SortingComparator extends WritableComparator {

  protected Job3SortingComparator() {
    super(FloatWritable.class, true);
  }

  /**
   * Compare two float DESC
   * 
   * @return Int -1 0 1
   */
  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    return -super.compare(w1, w2);
  }
}
