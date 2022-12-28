/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.plain;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;

import com.github.jinahya.bit.io.BitOutput;
import com.github.jinahya.bit.io.ByteOutput;
import com.github.jinahya.bit.io.DefaultBitOutput;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.bytes.LittleEndianDataOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plain encoding except for booleans
 */
public class PlainValuesWriter extends ValuesWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PlainValuesWriter.class);

  public static final Charset CHARSET = Charset.forName("UTF-8");

  private CapacityByteArrayOutputStream arrayOut;
  private LittleEndianDataOutputStream out;
  private int[] bitCounter = new int[64];
  private int totalValues;
  private long prevValue;
  private byte[] tmpByte = {0};
  ByteBufferAllocator allocator;
  CapacityByteArrayOutputStream outputBuffer;
  int outputIndex = 0;
  int byteIndex = 0;
  public PlainValuesWriter(int initialSize, int pageSize, ByteBufferAllocator allocator) {
    arrayOut = new CapacityByteArrayOutputStream(initialSize, pageSize, allocator);
    outputBuffer = new CapacityByteArrayOutputStream(initialSize, pageSize, allocator);
    out = new LittleEndianDataOutputStream(arrayOut);
    prevValue = 0L;
    totalValues = 0;
  }

  @Override
  public final void writeBytes(Binary v) {
    try {
      out.writeInt(v.length());
      v.writeTo(out);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write bytes", e);
    }
  }

  @Override
  public final void writeInteger(int v) {
    try {
      out.writeInt(v);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write int", e);
    }
  }

  @Override
  public final void writeLong(long v) {
    try {
      out.writeLong(v);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write long", e);
    }
  }

  @Override
  public final void writeFloat(float v) {
    try {
      out.writeFloat(v);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write float", e);
    }
  }

  @Override
  public final void writeDouble(double v) {
    try {
      // out.writeDouble(v);
      long l = Double.doubleToLongBits(v);
      long diff = l-prevValue;
      diff = (diff >> 63) ^ (diff << 1); // zigzag
      int leadingZeros = Long.numberOfLeadingZeros(diff);
      if((-1L >>> leadingZeros) == diff || diff == 0) {
        leadingZeros = Math.max(leadingZeros-1, 0);
      }
      bitCounter[leadingZeros]++;
      out.writeLong(diff);
      prevValue = l;
      totalValues += 1;
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write double", e);
    }
  }

  @Override
  public void writeByte(int value) {
    try {
      out.write(value);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write byte", e);
    }
  }

  @Override
  public long getBufferedSize() {
    return arrayOut.size();
  }

  private void addLongBits(long v, int _remainingBits) throws IOException {
    int remainingBits = _remainingBits;
    while(remainingBits > 0) {
      if (byteIndex == 0 && remainingBits >= 8) {
        remainingBits -= 8;
        tmpByte[0] = (byte) (0xffL & (v >>> (remainingBits)));
        outputBuffer.write(tmpByte);
        tmpByte[0] = 0;
        outputIndex++;
      } else if (remainingBits >= 8 - byteIndex) {
        remainingBits -= (8 - byteIndex);
        tmpByte[0] = (byte) (tmpByte[0] | (int) ((-1L >>> 56+byteIndex) & (v >>> remainingBits)));
        outputBuffer.write(tmpByte);
        tmpByte[0] = (byte) 0;
        byteIndex = 0;
        outputIndex++;
      } else {
        tmpByte[0] = (byte) ((long) tmpByte[0] |  (((-1L >>> 64-remainingBits) & v) << 8-byteIndex-remainingBits));
        byteIndex += remainingBits;
        if(byteIndex == 8) {
          // this probably will never be true (covered by the previous)
          byteIndex = 0;
          outputIndex++;
        }
        remainingBits = 0;
      }
      outputIndex++;
    }
  }
  private CapacityByteArrayOutputStream doubleDeltaEncoder(byte[] input) throws IOException {
    // determine final output size
    int currentCount = 0;
    long finalSize = input.length * 8 + 8;
    int redundantBits = 0;
      for(int i = 0; i < 64; i++) {
      if(bitCounter[i] != 0) {
        currentCount += bitCounter[i];
        long newSize =  (totalValues)*(64-i) + (currentCount)*64 + 8;
        if(newSize < finalSize) {
          finalSize = newSize;
          redundantBits = i;
        }
      }
    }
    int bits = 64-redundantBits;
    ByteBuffer  inputBuffer = ByteBuffer.wrap(input).order(ByteOrder.LITTLE_ENDIAN);

    long max = -1L >>> redundantBits;
    long leadingOnes = -1L << bits;
//    output = new byte[(int)Math.ceil(finalSize/8.0)+1024];
    tmpByte[0] = (byte) bits;
    outputBuffer.write(tmpByte);
    outputIndex++;

    for(int i = 0; i < totalValues; i++) {
      long v = inputBuffer.getLong(i << 3);
      if (bits < 64 && ((leadingOnes & v) != 0 || v == max)) {
        addLongBits(max, bits);
        addLongBits(v, 64);
      } else {
        addLongBits(v, bits);
      }
    }
    if(tmpByte[0] != 0) {
      outputBuffer.write(tmpByte);
      outputIndex++;
    }
    int paddingSize = 8 - (int) (outputBuffer.size() % 8);
    outputBuffer.write(new byte[paddingSize]);

    System.out.println("Bits: " + bits + ", Output size: " + outputBuffer.size() + " Input size: " + input.length + " Bit counter: " + Arrays.toString(bitCounter));
    return outputBuffer;
  }
  @Override
  public BytesInput getBytes() {
    try {
      out.flush();
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write page", e);
    }
    if (LOG.isDebugEnabled()) LOG.debug("writing a buffer of size {}", arrayOut.size());
    if(totalValues > 0) {
      try {
        return BytesInput.from(doubleDeltaEncoder(BytesInput.from(arrayOut).toByteArray()));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return BytesInput.from(arrayOut);
  }

  @Override
  public void reset() {
    arrayOut.reset();
    outputBuffer.reset();
    totalValues = 0;
    prevValue = 0;
    bitCounter = new int[64];
    outputIndex = 0;
    byteIndex = 0;
    tmpByte[0] = 0;
  }

  @Override
  public void close() {
    arrayOut.close();
    out.close();
  }

  @Override
  public long getAllocatedSize() {
    return arrayOut.getCapacity();
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.PLAIN;
  }

  @Override
  public String memUsageString(String prefix) {
    return arrayOut.memUsageString(prefix + " PLAIN");
  }

}
