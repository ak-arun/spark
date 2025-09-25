package com.ak.spark.utf16le;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class UTF16LETextInputFormat extends FileInputFormat<LongWritable, Text> {
    
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new UTF16LERecordReader();
    }
    
    public static class UTF16LERecordReader extends RecordReader<LongWritable, Text> {
        private FSDataInputStream inputStream;
        private long start;
        private long end;
        private long pos;
        private LongWritable key = new LongWritable();
        private Text value = new Text();
        private byte[] buffer = new byte[8192];
        private int bufferPos = 0;
        private int bufferSize = 0;
        
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            FileSplit fileSplit = (FileSplit) split;
            Configuration conf = context.getConfiguration();
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            
            inputStream = fs.open(file);
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            pos = start;
            
            if (start == 0) {
                // Skip BOM if at start of file
                byte[] bom = new byte[2];
                inputStream.read(bom);
                if (bom[0] == (byte)0xFF && bom[1] == (byte)0xFE) {
                    pos += 2;
                } else {
                    inputStream.seek(start);
                    pos = start;
                }
            } else {
                // Align to UTF-16 character boundary (even byte position)
                if (start % 2 != 0) {
                    start++;
                    pos = start;
                }
                inputStream.seek(start);
                
                // Skip to next record boundary to avoid reading partial records
                skipToNextRecord();
            }
        }
        
        private void skipToNextRecord() throws IOException {
            // Read until we find a complete newline (0A 00) to align with record boundary
            while (pos < end) {
                if (bufferPos + 1 >= bufferSize) {
                    bufferSize = inputStream.read(buffer);
                    bufferPos = 0;
                    if (bufferSize <= 0) break;
                }
                
                if (bufferPos + 1 < bufferSize) {
                    byte b1 = buffer[bufferPos++];
                    byte b2 = buffer[bufferPos++];
                    pos += 2;
                    
                    // Found record boundary - stop here
                    if (b1 == 0x0A && b2 == 0x00) {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        
        @Override
        public boolean nextKeyValue() throws IOException {
            if (pos >= end) return false;
            
            StringBuilder line = new StringBuilder();
            boolean foundRecord = false;
            
            while (pos < end) {
                if (bufferPos + 1 >= bufferSize) {
                    bufferSize = inputStream.read(buffer);
                    bufferPos = 0;
                    if (bufferSize <= 0) break;
                }
                
                // Read UTF-16 LE character (2 bytes)
                if (bufferPos + 1 < bufferSize) {
                    byte b1 = buffer[bufferPos++];  // Low byte
                    byte b2 = buffer[bufferPos++];  // High byte
                    pos += 2;
                    
                    // Check for newline (0A 00)
                    if (b1 == 0x0A && b2 == 0x00) {
                        foundRecord = true;
                        break;
                    }
                    
                    // Convert to char and append (Little Endian)
                    char c = (char)((b2 << 8) | (b1 & 0xFF));
                    line.append(c);
                } else {
                    // Incomplete character at buffer boundary - refill buffer
                    break;
                }
            }
            
            // Handle case where we reach end of split but have data
            if (!foundRecord && line.length() > 0 && pos >= end) {
                // Read beyond split boundary to complete the record
                while (true) {
                    if (bufferPos + 1 >= bufferSize) {
                        bufferSize = inputStream.read(buffer);
                        bufferPos = 0;
                        if (bufferSize <= 0) break;
                    }
                    
                    if (bufferPos + 1 < bufferSize) {
                        byte b1 = buffer[bufferPos++];
                        byte b2 = buffer[bufferPos++];
                        pos += 2;
                        
                        if (b1 == 0x0A && b2 == 0x00) {
                            foundRecord = true;
                            break;
                        }
                        
                        char c = (char)((b2 << 8) | (b1 & 0xFF));
                        line.append(c);
                    } else {
                        break;
                    }
                }
            }
            
            if (foundRecord || line.length() > 0) {
                key.set(pos);
                value.set(line.toString());
                return true;
            }
            
            return false;
        }
        
        @Override
        public LongWritable getCurrentKey() { 
            return key; 
        }
        
        @Override
        public Text getCurrentValue() { 
            return value; 
        }
        
        @Override
        public float getProgress() {
            if (start == end) {
                return 0.0f;
            }
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
        
        @Override
        public void close() throws IOException {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }
}

/*package com.ak.spark.utf16le;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class UTF16LETextInputFormat extends FileInputFormat<LongWritable, Text> {
    
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new UTF16LERecordReader();
    }
    
    public static class UTF16LERecordReader extends RecordReader<LongWritable, Text> {
        private FSDataInputStream inputStream;
        private long start;
        private long end;
        private long pos;
        private LongWritable key = new LongWritable();
        private Text value = new Text();
        private byte[] buffer = new byte[8192];
        private int bufferPos = 0;
        private int bufferSize = 0;
        
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            FileSplit fileSplit = (FileSplit) split;
            Configuration conf = context.getConfiguration();
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            
            inputStream = fs.open(file);
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            pos = start;
            
            // Skip BOM if at start of file
            if (start == 0) {
                byte[] bom = new byte[2];
                inputStream.read(bom);
                if (bom[0] == (byte)0xFF && bom[1] == (byte)0xFE) {
                    pos += 2;
                } else {
                    inputStream.seek(start);
                }
            } else {
                inputStream.seek(start);
            }
        }
        
        @Override
        public boolean nextKeyValue() throws IOException {
            if (pos >= end) return false;
            
            StringBuilder line = new StringBuilder();
            boolean foundRecord = false;
            
            while (pos < end) {
                if (bufferPos >= bufferSize) {
                    bufferSize = inputStream.read(buffer);
                    bufferPos = 0;
                    if (bufferSize <= 0) break;
                }
                
                // Read UTF-16 LE character (2 bytes)
                if (bufferPos + 1 < bufferSize) {
                    byte b1 = buffer[bufferPos++];
                    byte b2 = buffer[bufferPos++];
                    pos += 2;
                    
                    // Check for newline (0A 00)
                    if (b1 == 0x0A && b2 == 0x00) {
                        foundRecord = true;
                        break;
                    }
                    
                    // Convert to char and append
                    char c = (char)((b2 << 8) | (b1 & 0xFF));
                    line.append(c);
                } else {
                    break;
                }
            }
            
            if (foundRecord || line.length() > 0) {
                key.set(pos);
                value.set(line.toString());
                return true;
            }
            
            return false;
        }
        
        @Override
        public LongWritable getCurrentKey() { return key; }
        
        @Override
        public Text getCurrentValue() { return value; }
        
        @Override
        public float getProgress() {
            return (pos - start) / (float)(end - start);
        }
        
        @Override
        public void close() throws IOException {
            if (inputStream != null) inputStream.close();
        }
    }
}
*/
