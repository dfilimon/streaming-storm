package org.apache.mahout.storm.spout;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.Vector;
import org.apache.mahout.storm.utils.HypercubeSampler;

public class HypercubeSpout extends BaseRichSpout {
  public static final String NUM_POINTS = "hypercube.numPoints";
  public static final String NUM_DIMENSION = "hypercube.numDimension";
  public static final String NUM_CLUSTERS = "hypercube.numClusters";
  public static final String RADIUS = "hypercube.radius";
  public static final String UNIFORM_FRACTION = "hypercube.uniformFraction";

  public static final String OUTPUT_PATH = "hypercube.output";

  private SpoutOutputCollector _spoutOutputCollector;

  private Iterator<Vector> _vectors;

  private int _numPoints;

  private int _emittedPoints = 0;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("vector"));
  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    _spoutOutputCollector = spoutOutputCollector;
    _numPoints = ((Number) map.get(NUM_POINTS)).intValue();
    _vectors = new HypercubeSampler(((Number) map.get(NUM_DIMENSION)).intValue(),
        ((Number) map.get(NUM_CLUSTERS)).intValue(), _numPoints, (Double) map.get(RADIUS),
        (Double) map.get(UNIFORM_FRACTION)).infiniteSampleVectors();
  }

  @Override
  public void nextTuple() {
    if (_numPoints > 0) {
      Centroid next = new Centroid(_emittedPoints, _vectors.next(), 1);
      _spoutOutputCollector.emit(new Values(next));
      --_numPoints;

      System.out.printf("Emitted vector %d\n", _emittedPoints++);
    }
  }
}
