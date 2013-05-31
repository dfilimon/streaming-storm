package org.apache.mahout.storm.spout;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.Vector;
import org.apache.mahout.storm.utils.HypercubeSampler;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

public class HypercubeBatchSpout implements IBatchSpout {

  private Iterator<Vector> _vectors;

  private int _numPoints;

  private int _emittedPoints = 0;

  @Override
  public void open(Map map, TopologyContext topologyContext) {
    _numPoints = ((Number) map.get(HypercubeSpout.NUM_POINTS)).intValue();
    _vectors = new HypercubeSampler(((Number) map.get(HypercubeSpout.NUM_DIMENSION)).intValue(),
        ((Number) map.get(HypercubeSpout.NUM_CLUSTERS)).intValue(), _numPoints, (Double) map.get(HypercubeSpout.RADIUS),
        (Double) map.get(HypercubeSpout.UNIFORM_FRACTION)).infiniteSampleVectors();
  }

  @Override
  public void emitBatch(long l, TridentCollector tridentCollector) {
    if (_numPoints > 0) {
      int numEmit = RandomUtils.nextInt(_numPoints);
      _numPoints -= numEmit;
      for (int i = 0; i < numEmit; ++i) {
        tridentCollector.emit(new Values(new Centroid(_emittedPoints++, _vectors.next(), 1)));
      }
      System.out.printf("Emitted %d points in batch %d\n", numEmit, l);
    }
  }

  @Override
  public void ack(long l) { }

  @Override
  public void close() { }

  @Override
  public Map getComponentConfiguration() {
    Config conf = new Config();
    conf.setMaxTaskParallelism(1);
    return conf;
  }

  @Override
  public Fields getOutputFields() {
    return new Fields("vector");
  }
}
