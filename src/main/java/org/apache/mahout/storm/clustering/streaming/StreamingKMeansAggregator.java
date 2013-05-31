package org.apache.mahout.storm.clustering.streaming;

import java.util.Map;

import backtype.storm.tuple.Values;
import org.apache.mahout.clustering.streaming.cluster.StreamingKMeans;
import org.apache.mahout.clustering.streaming.mapreduce.StreamingKMeansDriver;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.neighborhood.UpdatableSearcher;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class StreamingKMeansAggregator extends BaseAggregator<StreamingKMeans> {
  private Map _conf;

  public StreamingKMeansAggregator(Map conf) {
    _conf = conf;
  }

  @Override
  public StreamingKMeans init(Object id, TridentCollector tridentCollector) {
    UpdatableSearcher searcher = StreamingKMeansBolt.searcherFromConfiguration(_conf);
    int numClusters = ((Number) _conf.get(StreamingKMeansDriver.ESTIMATED_NUM_MAP_CLUSTERS)).intValue();
    double estimatedDistanceCutoff = (Double) _conf.get(StreamingKMeansDriver.ESTIMATED_DISTANCE_CUTOFF);
    return new StreamingKMeans(searcher, numClusters, estimatedDistanceCutoff);
  }

  @Override
  public void aggregate(StreamingKMeans centroids, TridentTuple objects, TridentCollector tridentCollector) {
    Centroid centroid = (Centroid) objects.getValueByField("vector");
    centroids.cluster(centroid);
  }

  @Override
  public void complete(StreamingKMeans centroids, TridentCollector tridentCollector) {
    centroids.reindexCentroids();
    for (Centroid centroid : centroids) {
      tridentCollector.emit(new Values(centroid));
    }
  }
}
