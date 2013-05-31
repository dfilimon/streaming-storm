package org.apache.mahout.storm.utils;

import java.util.Iterator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

public final class VectorSerializer extends Serializer<Vector> {

  public static final int FLAG_DENSE = 0x01;
  public static final int FLAG_SEQUENTIAL = 0x02;
  public static final int FLAG_NAMED = 0x04;
  public static final int NUM_FLAGS = 3;

  public static void writeVector(Output output, Vector vector) {
    boolean dense = vector.isDense();
    boolean sequential = vector.isSequentialAccess();
    boolean named = vector instanceof NamedVector;

    output.writeByte((dense ? FLAG_DENSE : 0)
        | (sequential ? FLAG_SEQUENTIAL : 0)
        | (named ? FLAG_NAMED : 0));

    Varint.writeUnsignedVarInt(vector.size(), output);
    if (dense) {
      for (Vector.Element element : vector.all()) {
        output.writeDouble(element.get());
      }
    } else {
      Varint.writeUnsignedVarInt(vector.getNumNondefaultElements(), output);
      Iterator<Vector.Element> iter = vector.nonZeroes().iterator();
      if (sequential) {
        int lastIndex = 0;
        while (iter.hasNext()) {
          Vector.Element element = iter.next();
          int thisIndex = element.index();
          // Delta-code indices:
          Varint.writeUnsignedVarInt(thisIndex - lastIndex, output);
          lastIndex = thisIndex;
          output.writeDouble(element.get());
        }
      } else {
        while (iter.hasNext()) {
          Vector.Element element = iter.next();
          Varint.writeUnsignedVarInt(element.index(), output);
          output.writeDouble(element.get());
        }
      }
    }
    if (named) {
      String name = ((NamedVector) vector).getName();
      output.writeString(name == null ? "" : name);
    }
  }

  public static Vector readVector(Input input) {
    int flags = input.readByte();
    Preconditions.checkArgument(flags >> NUM_FLAGS == 0, "Unknown flags set: %d", Integer.toString(flags, 2));
    boolean dense = (flags & FLAG_DENSE) != 0;
    boolean sequential = (flags & FLAG_SEQUENTIAL) != 0;
    boolean named = (flags & FLAG_NAMED) != 0;

    int size = Varint.readUnsignedVarInt(input);
    Vector v;
    if (dense) {
      double[] values = new double[size];
      for (int i = 0; i < size; i++) {
        values[i] = input.readDouble();
      }
      v = new DenseVector(values);
    } else {
      int numNonDefaultElements = Varint.readUnsignedVarInt(input);
      v = sequential
          ? new SequentialAccessSparseVector(size, numNonDefaultElements)
          : new RandomAccessSparseVector(size, numNonDefaultElements);
      if (sequential) {
        int lastIndex = 0;
        for (int i = 0; i < numNonDefaultElements; i++) {
          int delta = Varint.readUnsignedVarInt(input);
          int index = lastIndex + delta;
          lastIndex = index;
          double value = input.readDouble();
          v.setQuick(index, value);
        }
      } else {
        for (int i = 0; i < numNonDefaultElements; i++) {
          int index = Varint.readUnsignedVarInt(input);
          double value = input.readDouble();
          v.setQuick(index, value);
        }
      }
    }
    if (named) {
      String name = input.readString();
      v = new NamedVector(v, name);
    }
    return v;
  }

  /**
   * Writes the bytes for the object to the output.
   * <p/>
   * This method should not be called directly, instead this serializer can be passed to {@link com.esotericsoftware.kryo.Kryo} write methods that accept a
   * serialier.
   *
   * @param vector May be null if {@link #getAcceptsNull()} is true.
   */
  @Override
  public void write(Kryo kryo, Output output, Vector vector) {
    writeVector(output, vector);
  }

  /**
   * Reads bytes and returns a new object of the specified concrete type.
   * <p/>
   * Before Kryo can be used to read child objects, {@link com.esotericsoftware.kryo.Kryo#reference(Object)} must be called with the parent object to
   * ensure it can be referenced by the child objects. Any serializer that uses {@link com.esotericsoftware.kryo.Kryo} to read a child object may need to
   * be reentrant.
   * <p/>
   * This method should not be called directly, instead this serializer can be passed to {@link com.esotericsoftware.kryo.Kryo} read methods that accept a
   * serialier.
   *
   * @return May be null if {@link #getAcceptsNull()} is true.
   */
  @Override
  public Vector read(Kryo kryo, Input input, Class<Vector> type) {
    return readVector(input);
  }
}

