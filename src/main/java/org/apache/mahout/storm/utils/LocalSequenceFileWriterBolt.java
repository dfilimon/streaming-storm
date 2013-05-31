package org.apache.mahout.storm.utils;

import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.streaming.mapreduce.CentroidWritable;
import org.apache.mahout.math.Centroid;

public class LocalSequenceFileWriterBolt extends BaseRichBolt {
  public static final String OUTPUT_PATH = "localfile.output";

  private String _outputPath;

  private SequenceFile.Writer _writer;

  private OutputCollector _outputCollector;

  public LocalSequenceFileWriterBolt(String outputPath) {
    _outputPath = outputPath;
  }

  public static SequenceFile.Writer newLocalWriter(Path outputPath, Class<?> keyClass, Class<?> valueClass) {
    Configuration hadoopConf = new Configuration();
    try {
      return SequenceFile.createWriter(FileSystem.getLocal(hadoopConf), hadoopConf, outputPath, keyClass, valueClass);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
    _writer = newLocalWriter(new Path(_outputPath), IntWritable.class, CentroidWritable.class);
    _outputCollector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    if (!tuple.getSourceStreamId().equals("__tick")) {
      Centroid centroid = (Centroid) tuple.getValueByField("vector");
      if (centroid.getIndex() == 0) {
        Closeables.closeQuietly(_writer);
        _writer = newLocalWriter(new Path(_outputPath), IntWritable.class, CentroidWritable.class);
      }
      try {
        _writer.append(new IntWritable(centroid.getIndex()), new CentroidWritable(centroid));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      System.out.printf("Wrote vector %d\n", centroid.getIndex());

      _outputCollector.emit(new Values(centroid));
    } else {
      System.out.printf("Writer ticked %s\n", tuple.toString());
    }
  }

  @Override
  public void cleanup() {
    try {
      _writer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("vector"));
  }
}
