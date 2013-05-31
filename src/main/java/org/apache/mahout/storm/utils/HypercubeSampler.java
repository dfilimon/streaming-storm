package org.apache.mahout.storm.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.jet.random.Uniform;
import org.apache.mahout.math.random.MultiNormal;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class HypercubeSampler {
  @Option(name="-d")
  private int numDimensions;

  @Option(name="-k")
  private int numClusters;

  @Option(name="-n")
  private int numPoints;

  @Option(name="-r")
  private double radius;

  @Option(name="-uf")
  private double uniformFraction = 0.1;

  @Option(name="-o")
  private String output;

  private Random random = RandomUtils.getRandom();

  public HypercubeSampler() { }

  public HypercubeSampler(long numDimensions, long numClusters, long numPoints, double radius, double uniformFraction) {
    this.numDimensions = (int) numDimensions;
    this.numClusters = (int) numClusters;
    this.numPoints = (int) numPoints;
    this.radius = radius;
    this.uniformFraction = uniformFraction;
    random = RandomUtils.getRandom();
  }

  public List<MultiNormal> getDistributions() {
    List<MultiNormal> distributions = Lists.newArrayList();
    for (int i = 0; i < numClusters; ++i) {
      Vector centroid = new DenseVector(numDimensions);
      for (int j = 0; j < numDimensions; ++j) {
        centroid.setQuick(j, random.nextInt(2));
      }
      distributions.add(new MultiNormal(radius, centroid));
    }
    return distributions;
  }

  public Iterable<Vector> sampleVectors() {
    final List<MultiNormal> distributions = getDistributions();
    return new Iterable<Vector>() {
      @Override
      public Iterator<Vector> iterator() {
        return new AbstractIterator<Vector>() {
          private int numUniformPoints = (int) (uniformFraction * numPoints);
          private int numVertexPoints = numPoints - numUniformPoints;

          @Override
          protected Vector computeNext() {
            if (numUniformPoints > 0 || numVertexPoints > 0) {
              if (numUniformPoints > 0 && random.nextDouble() < uniformFraction) {
                --numUniformPoints;
                return new DenseVector(numDimensions).assign(new Uniform(random));
              } else {
                --numVertexPoints;
                return distributions.get(random.nextInt(numClusters)).sample();
              }
            } else {
              return endOfData();
            }
          }
        };
      }
    };
  }

  public Iterator<Vector> infiniteSampleVectors() {
    final List<MultiNormal> distributions = getDistributions();
    return new AbstractIterator<Vector>() {

      @Override
      protected Vector computeNext() {
        if (random.nextDouble() < uniformFraction) {
          return new DenseVector(numDimensions).assign(new Uniform(random));
        } else {
          return distributions.get(random.nextInt(numClusters)).sample();
        }
      }
    };
  }

  public void writeVectors() throws IOException {
    Configuration conf = new Configuration();
    SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(conf), conf, new Path(output),
        IntWritable.class, VectorWritable.class, SequenceFile.CompressionType.RECORD, new DefaultCodec());

    IntWritable intWritable = new IntWritable();
    VectorWritable vectorWritable = new VectorWritable();

    int numVectors = 0;
    for (Vector vector : sampleVectors()) {
      if (numVectors % 1000 == 0) {
        System.out.printf("Wrote %d vectors\n", numVectors);
      }
      intWritable.set(numVectors++);
      vectorWritable.set(vector);
      writer.append(intWritable, vectorWritable);
    }

    writer.close();
  }

  public void doMain(String[] args) {
    CmdLineParser parser = new CmdLineParser(this);
    try {
      parser.parseArgument(args);
      writeVectors();
    } catch (CmdLineException e) {
      parser.printUsage(System.err);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    new HypercubeSampler().doMain(args);
  }
}
