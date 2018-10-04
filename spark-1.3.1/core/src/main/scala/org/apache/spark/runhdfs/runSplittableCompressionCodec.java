package org.apache.spark.runhdfs;

/**
 * Created by jyb on 10/3/18.
 */

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;


/**
 * This interface is meant to be implemented by those compression codecs
 * which are capable to compress / de-compress a stream starting at any
 * arbitrary position.
 *
 * Especially the process of de-compressing a stream starting at some arbitrary
 * position is challenging.  Most of the codecs are only able to successfully
 * de-compress a stream, if they start from the very beginning till the end.
 * One of the reasons is the stored state at the beginning of the stream which
 * is crucial for de-compression.
 *
 * Yet there are few codecs which do not save the whole state at the beginning
 * of the stream and hence can be used to de-compress stream starting at any
 * arbitrary points.  This interface is meant to be used by such codecs.  Such
 * codecs are highly valuable, especially in the context of Hadoop, because
 * an input compressed file can be split and hence can be worked on by multiple
 * machines in parallel.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface runSplittableCompressionCodec extends CompressionCodec {

  /**
   * During decompression, data can be read off from the decompressor in two
   * modes, namely continuous and blocked.  Few codecs (e.g. BZip2) are capable
   * of compressing data in blocks and then decompressing the blocks.  In
   * Blocked reading mode codecs inform 'end of block' events to its caller.
   * While in continuous mode, the caller of codecs is unaware about the blocks
   * and uncompressed data is spilled out like a continuous stream.
   */
  public enum READ_MODE {CONTINUOUS, BYBLOCK};

  /**
   * Create a stream as dictated by the readMode.  This method is used when
   * the codecs wants the ability to work with the underlying stream positions.
   *
   * @param seekableIn  The seekable input stream (seeks in compressed data)
   * @param start The start offset into the compressed stream. May be changed
   *              by the underlying codec.
   * @param end The end offset into the compressed stream. May be changed by
   *            the underlying codec.
   * @param readMode Controls whether stream position is reported continuously
   *                 from the compressed stream only only at block boundaries.
   * @return  a stream to read uncompressed bytes from
   */
  runSplitCompressionInputStream createInputStream(InputStream seekableIn,
	Decompressor decompressor, long start, long end, READ_MODE readMode) throws IOException;
}