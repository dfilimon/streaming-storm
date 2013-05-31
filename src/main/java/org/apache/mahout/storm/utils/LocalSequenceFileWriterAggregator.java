package org.apache.mahout.storm.utils;

import java.io.IOException;

import com.google.common.io.Closeables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.streaming.mapreduce.CentroidWritable;
import org.apache.mahout.math.Centroid;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class LocalSequenceFileWriterAggregator extends BaseAggregator<SequenceFile.Writer> {

  private String _path;

  private int _numVectors = 0;

  public LocalSequenceFileWriterAggregator(String path) {
    super();
    _path = path;
  }

  @Override
  public SequenceFile.Writer init(Object o, TridentCollector tridentCollector) {
    return LocalSequenceFileWriterBolt.newLocalWriter(new Path(_path), IntWritable.class, CentroidWritable.class);
  }

  @Override
  public void aggregate(SequenceFile.Writer writer, TridentTuple objects, TridentCollector tridentCollector) {
    try {
      writer.append(new IntWritable(_numVectors++), new CentroidWritable((Centroid) objects.getValueByField("vector")));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    tridentCollector.emit(objects);
  }

  @Override
  public void complete(SequenceFile.Writer writer, TridentCollector tridentCollector) {
    Closeables.closeQuietly(writer);
  }
}
