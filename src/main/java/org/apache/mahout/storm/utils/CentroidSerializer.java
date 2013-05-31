package org.apache.mahout.storm.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.Vector;

public class CentroidSerializer extends Serializer<Centroid> {
  @Override
  public void write(Kryo kryo, Output output, Centroid centroid) {
    output.writeInt(centroid.getIndex());
    output.writeDouble(centroid.getWeight());
    VectorSerializer.writeVector(output, centroid.getVector());
  }

  @Override
  public Centroid read(Kryo kryo, Input input, Class<Centroid> centroidClass) {
    int index = input.readInt();
    double weight = input.readDouble();
    Vector vector = VectorSerializer.readVector(input);
    return new Centroid(index, vector, weight);
  }
}
