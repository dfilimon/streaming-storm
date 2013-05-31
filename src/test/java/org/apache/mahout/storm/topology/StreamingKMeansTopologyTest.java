package org.apache.mahout.storm.topology;

import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.ClusteringUtils;
import org.apache.mahout.clustering.streaming.cluster.BallKMeans;
import org.apache.mahout.clustering.streaming.mapreduce.CentroidWritable;
import org.apache.mahout.clustering.streaming.mapreduce.StreamingKMeansDriver;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterable;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.neighborhood.BruteSearch;
import org.apache.mahout.math.neighborhood.FastProjectionSearch;
import org.apache.mahout.math.neighborhood.Searcher;
import org.apache.mahout.math.stats.OnlineSummarizer;
import org.apache.mahout.storm.clustering.streaming.BallKMeansAggregator;
import org.apache.mahout.storm.clustering.streaming.StreamingKMeansAggregator;
import org.apache.mahout.storm.clustering.streaming.StreamingKMeansBolt;
import org.apache.mahout.storm.spout.HypercubeBatchSpout;
import org.apache.mahout.storm.spout.HypercubeSpout;
import org.apache.mahout.storm.utils.CentroidSerializer;
import org.apache.mahout.storm.utils.LocalSequenceFileWriterAggregator;
import org.apache.mahout.storm.utils.LocalSequenceFileWriterBolt;
import org.apache.mahout.storm.utils.VectorSerializer;
import org.junit.Before;
import org.junit.Test;
import storm.trident.TridentTopology;

public class StreamingKMeansTopologyTest {
  private static final String INPUT_PATH = "test.vectors.in";
  private static final String OUTPUT_PATH = "test.centroids.out";

  private static final int NUM_CLUSTERS = 20;
  private static final int ESTIMATED_NUM_MAP_CLUSTERS = 60;

  Config _conf;

  @Before
  public void setupConfig() {
    _conf = new Config();
    _conf.setNumWorkers(2);

    _conf.registerSerialization(Path.class, FieldSerializer.class);
    _conf.registerSerialization(SequenceFile.Writer.class, FieldSerializer.class);
    _conf.registerSerialization(DenseVector.class, VectorSerializer.class);
    _conf.registerSerialization(Centroid.class, CentroidSerializer.class);

    _conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);

    _conf.put(HypercubeSpout.NUM_POINTS, 2000);
    _conf.put(HypercubeSpout.NUM_DIMENSION, 500);
    _conf.put(HypercubeSpout.NUM_CLUSTERS, NUM_CLUSTERS);
    _conf.put(HypercubeSpout.RADIUS, 0.0001);
    _conf.put(HypercubeSpout.UNIFORM_FRACTION, 0.0);

    _conf.put(DefaultOptionCreator.DISTANCE_MEASURE_OPTION, SquaredEuclideanDistanceMeasure.class.getName());
    _conf.put(StreamingKMeansDriver.NUM_PROJECTIONS_OPTION, 3);
    _conf.put(StreamingKMeansDriver.SEARCH_SIZE_OPTION, 2);
    _conf.put(StreamingKMeansDriver.SEARCHER_CLASS_OPTION, FastProjectionSearch.class.getName());
    _conf.put(StreamingKMeansDriver.ESTIMATED_NUM_MAP_CLUSTERS, ESTIMATED_NUM_MAP_CLUSTERS);
    _conf.put(StreamingKMeansDriver.ESTIMATED_DISTANCE_CUTOFF, 1e-7);

    _conf.put(HypercubeSpout.OUTPUT_PATH, INPUT_PATH);
    _conf.put(LocalSequenceFileWriterBolt.OUTPUT_PATH, OUTPUT_PATH);
  }

  @Test
  public void testBasic() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new HypercubeSpout());
    builder.setBolt("input", new LocalSequenceFileWriterBolt(INPUT_PATH)).shuffleGrouping("spout");
    builder.setBolt("cluster", new StreamingKMeansBolt()).shuffleGrouping("input");
    builder.setBolt("output", new LocalSequenceFileWriterBolt(OUTPUT_PATH)).shuffleGrouping("cluster");

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", _conf, builder.createTopology());
    Utils.sleep(20 * 1000);
    cluster.killTopology("test");
    cluster.shutdown();

    List<Centroid> datapoints = getCentroidsFromFile(INPUT_PATH);
    List<Centroid> centroids = getCentroidsFromFile(OUTPUT_PATH);

    System.out.printf("Total weight datapoints %f\n", ClusteringUtils.totalWeight(datapoints));
    System.out.printf("Total weight pre-collapse %f\n", ClusteringUtils.totalWeight(centroids));

    BallKMeans collapser = new BallKMeans(StreamingKMeansBolt.searcherFromConfiguration(_conf), NUM_CLUSTERS, 100);
    collapser.cluster(centroids);
    List<Centroid> finalCentroids = Lists.newArrayList(collapser);

    List<OnlineSummarizer> summarizers = summarizeClusterDistances(datapoints, finalCentroids,
        new SquaredEuclideanDistanceMeasure());

    System.out.printf("Total weight post-collapse %f\n", ClusteringUtils.totalWeight(finalCentroids));
    for (int i = 0; i < finalCentroids.size(); ++i) {
      OnlineSummarizer summarizer = summarizers.get(i);
      System.out.printf("Cluster %d [%d]: min %f; avg %f; max %f\n",
          i, summarizer.getCount(), summarizer.getMin(), summarizer.getMean(), summarizer.getMax());
    }
  }

  @Test
  public void testTransactional() {
    TridentTopology topology = new TridentTopology();
    topology.newStream("spout", new HypercubeBatchSpout())
        .aggregate(new Fields("vector"), new LocalSequenceFileWriterAggregator(INPUT_PATH), new Fields("vector"))
        .partitionAggregate(new Fields("vector"), new StreamingKMeansAggregator(_conf), new Fields("vector"))
        .aggregate(new Fields("vector"), new BallKMeansAggregator(_conf), new Fields("vector"))
        .aggregate(new Fields("vector"), new LocalSequenceFileWriterAggregator(OUTPUT_PATH), new Fields("vector"));

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", _conf, topology.build());
    Utils.sleep(20 * 1000);
    cluster.killTopology("test");
    cluster.shutdown();

    List<Centroid> datapoints = getCentroidsFromFile(INPUT_PATH);
    List<Centroid> centroids = getCentroidsFromFile(OUTPUT_PATH);
    List<OnlineSummarizer> summarizers = summarizeClusterDistances(datapoints, centroids,
        new SquaredEuclideanDistanceMeasure());
    System.out.printf("Total weight post-collapse %f\n", ClusteringUtils.totalWeight(centroids));
    for (int i = 0; i < centroids.size(); ++i) {
      OnlineSummarizer summarizer = summarizers.get(i);
      System.out.printf("Cluster %d [%d]: min %f; avg %f; max %f\n",
          i, summarizer.getCount(), summarizer.getMin(), summarizer.getMean(), summarizer.getMax());
    }
  }

  public static List<Centroid> getCentroidsFromFile(String path) {
    List<Centroid> centroids = Lists.newArrayList();
    for (CentroidWritable centroidWritable :
        new SequenceFileValueIterable<CentroidWritable>(new Path(path), true, new Configuration())) {
      Centroid centroid = centroidWritable.getCentroid().clone();
      centroids.add(centroid);
    }
    return centroids;
  }

  public static List<OnlineSummarizer> summarizeClusterDistances(Iterable<? extends Vector> datapoints,
                                                                 Iterable<? extends Vector> centroids,
                                                                 DistanceMeasure distanceMeasure) {
    Searcher searcher = new BruteSearch(distanceMeasure);
    searcher.addAll(centroids);
    List<OnlineSummarizer> summarizers = Lists.newArrayList();
    if (searcher.size() == 0) {
      return summarizers;
    }
    for (int i = 0; i < searcher.size(); ++i) {
      summarizers.add(new OnlineSummarizer());
    }
    for (Vector vector : datapoints) {
      Centroid closest = (Centroid) searcher.search(vector, 1).get(0).getValue();
      OnlineSummarizer summarizer = summarizers.get(closest.getIndex());
      summarizer.add(distanceMeasure.distance(vector, closest));
    }
    return summarizers;
  }

}
