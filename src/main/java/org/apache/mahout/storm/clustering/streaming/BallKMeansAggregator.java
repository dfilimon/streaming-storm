package org.apache.mahout.storm.clustering.streaming;

import java.util.List;

import backtype.storm.Config;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import org.apache.mahout.clustering.streaming.cluster.BallKMeans;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.Centroid;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class BallKMeansAggregator extends BaseAggregator<List<Centroid>> {
  Config _conf;

  public BallKMeansAggregator(Config conf) {
    _conf = conf;
  }

  @Override
  public List<Centroid> init(Object o, TridentCollector tridentCollector) {
    return Lists.newArrayList();
  }

  @Override
  public void aggregate(List<Centroid> list, TridentTuple objects, TridentCollector tridentCollector) {
    list.add((Centroid) objects.getValueByField("vector"));
  }

  @Override
  public void complete(List<Centroid> list, TridentCollector tridentCollector) {
    BallKMeans clusterer = new BallKMeans(StreamingKMeansBolt.searcherFromConfiguration(_conf),
        ((Number) _conf.get(DefaultOptionCreator.NUM_CLUSTERS_OPTION)).intValue(), 100);
    clusterer.cluster(list);

    for (Centroid centroid : clusterer) {
      tridentCollector.emit(new Values(centroid));
    }
  }
}
