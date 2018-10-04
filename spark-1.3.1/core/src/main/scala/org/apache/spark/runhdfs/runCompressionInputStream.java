package org.apache.spark.runhdfs;

/**
 * Created by jyb on 10/3/18.
 */


import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
/**
 * A compression input stream.
 *
 * <p>Implementations are assumed to be buffered.  This permits clients to
 * reposition the underlying input stream then call {@link #resetState()},
 * without having to also synchronize client buffers.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class runCompressionInputStream extends InputStream implements Seekable {
  /**
   * The input stream to be compressed.
   */
  protected final InputStream in;
  protected long maxAvailableData = 0L;

  /**
   * Create a compression input stream that reads
   * the decompressed bytes from the given stream.
   *
   * @param in The input stream to be compressed.
   * @throws IOException
   */
  protected runCompressionInputStream(InputStream in) throws IOException {
	if (!(in instanceof Seekable) || !(in instanceof PositionedReadable)) {
	  this.maxAvailableData = in.available();
	}
	this.in = in;
  }

  @Override
  public void close() throws IOException {
	in.close();
  }

  /**
   * Read bytes from the stream.
   * Made abstract to prevent leakage to underlying stream.
   */
  @Override
  public abstract int read(byte[] b, int off, int len) throws IOException;

  /**
   * Reset the decompressor to its initial state and discard any buffered data,
   * as the underlying stream may have been repositioned.
   */
  public abstract void resetState() throws IOException;

  /**
   * This method returns the current position in the stream.
   *
   * @return Current position in stream as a long
   */
  @Override
  public long getPos() throws IOException {
	if (!(in instanceof Seekable) || !(in instanceof PositionedReadable)){
	  //This way of getting the current position will not work for file
	  //size which can be fit in an int and hence can not be returned by
	  //available method.
	  return (this.maxAvailableData - this.in.available());
	}
	else{
	  return ((Seekable)this.in).getPos();
	}

  }

  /**
   * This method is current not supported.
   *
   * @throws UnsupportedOperationException
   */

  @Override
  public void seek(long pos) throws UnsupportedOperationException {
	throw new UnsupportedOperationException();
  }

  /**
   * This method is current not supported.
   *
   * @throws UnsupportedOperationException
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws UnsupportedOperationException {
	throw new UnsupportedOperationException();
  }
}
