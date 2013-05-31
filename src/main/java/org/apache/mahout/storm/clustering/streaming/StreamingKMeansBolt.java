package org.apache.mahout.storm.clustering.streaming;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.mahout.clustering.streaming.cluster.StreamingKMeans;
import org.apache.mahout.clustering.streaming.mapreduce.StreamingKMeansDriver;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.neighborhood.BruteSearch;
import org.apache.mahout.math.neighborhood.FastProjectionSearch;
import org.apache.mahout.math.neighborhood.LocalitySensitiveHashSearch;
import org.apache.mahout.math.neighborhood.ProjectionSearch;
import org.apache.mahout.math.neighborhood.UpdatableSearcher;

public class StreamingKMeansBolt extends BaseRichBolt {
  private StreamingKMeans _clusterer;

  private int _numPoints = 0;

  private OutputCollector _outputCollector;

  @Override
  public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
    UpdatableSearcher searcher = searcherFromConfiguration(conf);
    Integer numClusters = ((Number) conf.get(StreamingKMeansDriver.ESTIMATED_NUM_MAP_CLUSTERS)).intValue();
    Double estimatedDistanceCutoff = (Double) conf.get(StreamingKMeansDriver.ESTIMATED_DISTANCE_CUTOFF);
    _clusterer = new StreamingKMeans(searcher, numClusters, estimatedDistanceCutoff);
    _outputCollector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    if (tuple.getSourceStreamId().equals("__tick")) {
      _clusterer.reindexCentroids();
      for (Centroid centroid : _clusterer) {
        _outputCollector.emit(new Values(centroid));
      }
    } else {
      Vector vector = (Vector) tuple.getValueByField("vector");
      _clusterer.cluster(new Centroid(_numPoints++, vector, 1));
      System.out.printf("Clustered %d points in %d clusters\n", _numPoints, _clusterer.getNumClusters());
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("vector"));
  }

  /**
   * Instantiates a searcher from a given configuration.
   * @param conf the configuration
   * @return the instantiated searcher
   * @throws RuntimeException if the distance measure class cannot be instantiated
   * @throws IllegalStateException if an unknown searcher class was requested
   */
  public static UpdatableSearcher searcherFromConfiguration(Map conf) {
    DistanceMeasure distanceMeasure;
    String distanceMeasureClass = (String) conf.get(DefaultOptionCreator.DISTANCE_MEASURE_OPTION);
    try {
      distanceMeasure = (DistanceMeasure)Class.forName(distanceMeasureClass).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate distanceMeasure", e);
    }

    Integer numProjections = ((Number) conf.get(StreamingKMeansDriver.NUM_PROJECTIONS_OPTION)).intValue();
    Integer searchSize =  ((Number) conf.get(StreamingKMeansDriver.SEARCH_SIZE_OPTION)).intValue();

    String searcherClass = (String) conf.get(StreamingKMeansDriver.SEARCHER_CLASS_OPTION);

    if (searcherClass.equals(BruteSearch.class.getName())) {
      return ClassUtils.instantiateAs(searcherClass, UpdatableSearcher.class,
          new Class[]{DistanceMeasure.class}, new Object[]{distanceMeasure});
    } else if (searcherClass.equals(FastProjectionSearch.class.getName()) ||
        searcherClass.equals(ProjectionSearch.class.getName())) {
      return ClassUtils.instantiateAs(searcherClass, UpdatableSearcher.class,
          new Class[]{DistanceMeasure.class, int.class, int.class},
          new Object[]{distanceMeasure, numProjections, searchSize});
    } else if (searcherClass.equals(LocalitySensitiveHashSearch.class.getName())) {
      return ClassUtils.instantiateAs(searcherClass, LocalitySensitiveHashSearch.class,
          new Class[]{DistanceMeasure.class, int.class},
          new Object[]{distanceMeasure, searchSize});
    } else {
      throw new IllegalStateException("Unknown class instantiation requested");
    }
  }
}
