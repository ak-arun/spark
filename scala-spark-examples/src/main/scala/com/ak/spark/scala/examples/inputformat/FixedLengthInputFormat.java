package com.ak.spark.scala.examples.inputformat;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthRecordReader;

/**
 * FixedLengthInputFormat is an input format used to read input files
 * which contain fixed length records.  The content of a record need not be
 * text.  It can be arbitrary binary data.  Users must configure the record
 * length property by calling:
 * FixedLengthInputFormat.setRecordLength(conf, recordLength);<br><br> or
 * conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, recordLength);
 * <br><br>
 * @see FixedLengthRecordReader
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FixedLengthInputFormat
    extends FileInputFormat<LongWritable, BytesWritable> {

  public static final String FIXED_RECORD_LENGTH =
      "fixedlengthinputformat.record.length"; 

  /**
   * Set the length of each record
   * @param conf configuration
   * @param recordLength the length of a record
   */
  public static void setRecordLength(Configuration conf, int recordLength) {
    conf.setInt(FIXED_RECORD_LENGTH, recordLength);
  }

  /**
   * Get record length value
   * @param conf configuration
   * @return the record length, zero means none was set
   */
  public static int getRecordLength(Configuration conf) {
	  return conf.get(FIXED_RECORD_LENGTH)==null?0:Integer.parseInt(conf.get(FIXED_RECORD_LENGTH));
  }

  @Override
  public RecordReader<LongWritable, BytesWritable>
      createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    int recordLength = getRecordLength(context.getConfiguration());
    if (recordLength <= 0) {
      throw new IOException("Fixed record length " + recordLength
          + " is invalid.  It should be set to a value greater than zero");
    }
    return new FixedLengthRecordReader(recordLength);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec = 
        new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    return (null == codec);
  } 

}
