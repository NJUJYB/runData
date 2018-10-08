package org.apache.spark.runhdfs;

import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Created by jyb on 10/3/18.
 */

/**
 * Treats keys as offset in file and value as line.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Unstable
public class runLineRecordReader implements RecordReader<LongWritable, Text> {
  private static final
  Log LOG = LogFactory.getLog(runLineRecordReader.class.getName());
  private static String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private runLineReader in;
  private FSDataInputStream fileIn;
  private final Seekable filePosition;
  int maxLineLength;
  private CompressionCodec codec;
  private Decompressor decompressor;

  public runLineRecordReader(Configuration job,
	FileSplit split) throws IOException {
	this(job, split, null);
  }

  public runLineRecordReader(Configuration job, FileSplit split,
	byte[] recordDelimiter) throws IOException {
	this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
	start = split.getStart();
	end = start + split.getLength();
	final Path file = split.getPath();
	compressionCodecs = new CompressionCodecFactory(job);
	codec = compressionCodecs.getCodec(file);

	// open the file and seek to the start of the split
	final FileSystem fs = file.getFileSystem(job);
	fileIn = fs.open(file);
	if (isCompressedInput()) {
	  decompressor = CodecPool.getDecompressor(codec);
	  if (codec instanceof runSplittableCompressionCodec) {
		final runSplitCompressionInputStream cIn =
				((runSplittableCompressionCodec)codec).createInputStream(
						fileIn, decompressor, start, end, runSplittableCompressionCodec.READ_MODE.BYBLOCK);
		// runData
		in = new runLineReader(cIn, job, recordDelimiter, file, start, end);
		start = cIn.getAdjustedStart();
		end = cIn.getAdjustedEnd();
		filePosition = cIn; // take pos from compressed stream
	  } else {
		// runData
		in = new runLineReader(codec.createInputStream(fileIn, decompressor), job, recordDelimiter,
				file, start, end);
		filePosition = fileIn;
	  }
	} else {
	  fileIn.seek(start);
	  // runData
	  in = new runLineReader(fileIn, job, recordDelimiter, file, start, end);
	  filePosition = fileIn;
	}
	// If this is not the first split, we always throw away first record
	// because we always (except the last split) read one extra line in
	// next() method.
	if (start != 0) {
	  start += in.readLine(new Text(), 0, maxBytesToConsume(start));
	}
	this.pos = start;
  }

  public runLineRecordReader(InputStream in, long offset, long endOffset,
	int maxLineLength) {
	this(in, offset, endOffset, maxLineLength, null);
  }

  public runLineRecordReader(InputStream in, long offset, long endOffset,
							 int maxLineLength, byte[] recordDelimiter) {
	this.maxLineLength = maxLineLength;
	this.in = new runLineReader(in, recordDelimiter);
	this.start = offset;
	this.pos = offset;
	this.end = endOffset;
	filePosition = null;
  }

  public runLineRecordReader(InputStream in, long offset, long endOffset,
	Configuration job) throws IOException{
	this(in, offset, endOffset, job, null);
  }

  public runLineRecordReader(InputStream in, long offset, long endOffset,
	Configuration job, byte[] recordDelimiter) throws IOException{
	this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
	this.in = new runLineReader(in, job, recordDelimiter);
	this.start = offset;
	this.pos = offset;
	this.end = endOffset;
	filePosition = null;
  }

  public LongWritable createKey() {
	return new LongWritable();
  }

  public Text createValue() {
	return new Text();
  }

  private boolean isCompressedInput() {
	return (codec != null);
  }

  private int maxBytesToConsume(long pos) {
	return isCompressedInput()
			? Integer.MAX_VALUE
			: (int) Math.min(Integer.MAX_VALUE, end - pos);
  }

  private long getFilePosition() throws IOException {
	long retVal;
	if (isCompressedInput() && null != filePosition) {
	  retVal = filePosition.getPos();
	} else {
	  retVal = pos;
	}
	return retVal;
  }

  /** Read a line. */
  public synchronized boolean next(LongWritable key, Text value)
		  throws IOException {

	// We always read one extra line, which lies outside the upper
	// split limit i.e. (end - 1)
	while (getFilePosition() <= end) {
	  key.set(pos);

	  int newSize = in.readLine(value, maxLineLength,
			  Math.max(maxBytesToConsume(pos), maxLineLength));
	  if (newSize == 0) {
		return false;
	  }
	  pos += newSize;
	  if (newSize < maxLineLength) {
		return true;
	  }

	  // line too long. try again
	  LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
	}

	return false;
  }

  /**
   * Get the progress within the split
   */
  public synchronized float getProgress() throws IOException {
	if (start == end) {
	  return 0.0f;
	} else {
	  return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
	}
  }

  public  synchronized long getPos() throws IOException {
	return pos;
  }

  public synchronized void close() throws IOException {
	try {
	  if (in != null) {
		in.close();

		// runData
		if(in.fileOrigPath != null){
		  ArrayList<String> outsideEvents = new ArrayList<String>();
		  ArrayList<Long> outsideTimes = new ArrayList<Long>();
		  String targetFilePath = proActiveRunData.targetDirPath + in.fileName;
		  outsideEvents.add(in.fileName);outsideTimes.add(System.nanoTime());
		  outsideEvents.add(targetFilePath);outsideTimes.add(System.nanoTime());
		  proActiveRunData.checkDirExist();
		  File tfile = new File(targetFilePath);
		  try{
			if(!tfile.exists()){
			  outsideEvents.add("Create File");outsideTimes.add(System.nanoTime());
			  tfile.createNewFile();
			  FileOutputStream fos = new FileOutputStream(tfile, true);
			  fos.write(in.splitBuf);
			  fos.flush();
			  fos.close();
			  in.splitBuf = null;
			  outsideEvents.add("End File Write");outsideTimes.add(System.nanoTime());
			}else {
			  outsideEvents.add("File Exists");outsideTimes.add(System.nanoTime());
			  tfile.delete();
			}
		  } catch (IOException e){
			e.printStackTrace();
		  }
		  if(in.expLogs.size() > 0){
			runDataLogs.writeLogs("FillBuffer", in.expLogs, in.times);
		  }
		  String localFilePath = targetFilePath;
		  String hdfsFilePath = in.fileOrigPath.split("master:9000")[1];
		  String fileName = in.fileName;
		  validCachedFile.validFile(localFilePath, hdfsFilePath, fileName);
		}
	  }
	} finally {
	  if (decompressor != null) {
		CodecPool.returnDecompressor(decompressor);
	  }
	}
  }
}
