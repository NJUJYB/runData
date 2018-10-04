package org.apache.spark.runhdfs;

/**
 * Created by jyb on 10/3/18.
 */

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An InputStream covering a range of compressed data. The start and end
 * offsets requested by a client may be modified by the codec to fit block
 * boundaries or other algorithm-dependent requirements.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class runSplitCompressionInputStream
		extends runCompressionInputStream {

  private long start;
  private long end;

  public runSplitCompressionInputStream(InputStream in, long start, long end)
		  throws IOException {
	super(in);
	this.start = start;
	this.end = end;
  }

  protected void setStart(long start) {
	this.start = start;
  }

  protected void setEnd(long end) {
	this.end = end;
  }

  /**
   * After calling createInputStream, the values of start or end
   * might change.  So this method can be used to get the new value of start.
   * @return The changed value of start
   */
  public long getAdjustedStart() {
	return start;
  }

  /**
   * After calling createInputStream, the values of start or end
   * might change.  So this method can be used to get the new value of end.
   * @return The changed value of end
   */
  public long getAdjustedEnd() {
	return end;
  }
}