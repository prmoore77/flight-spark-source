/*
 * Copyright (C) 2019 The flight-spark-source Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.flight.spark;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Test.None;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class TestConnector {
  private static final String USERNAME_1 = "flight1";
  private static final String USERNAME_2 = "flight2";
  private static final String NO_USERNAME = "";
  private static final String PASSWORD_1 = "woohoo1";
  private static final String PASSWORD_2 = "woohoo2";

  private static final String TEST_FULL_COMMAND = "{\"command\": \"test_command\"}";
  private static final String TEST_PROJECTION_COMMAND = "{\"columns\":[\"bid\",\"ask\",\"symbol\"],\"command\":\"test_command\"}";

  private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private static Location location;
  private static FlightServer server;
  private static SparkSession spark;
  private static FlightSparkContext csc;

  public static CallHeaderAuthenticator.AuthResult validate(String username, String password) {
    if (Strings.isNullOrEmpty(username)) {
      throw CallStatus.UNAUTHENTICATED.withDescription("Credentials not supplied.").toRuntimeException();
    }
    final String identity;
    if (USERNAME_1.equals(username) && PASSWORD_1.equals(password)) {
      identity = USERNAME_1;
    } else if (USERNAME_2.equals(username) && PASSWORD_2.equals(password)) {
      identity = USERNAME_2;
    } else {
      throw CallStatus.UNAUTHENTICATED.withDescription("Username or password is invalid.").toRuntimeException();
    }
    return () -> identity;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    FlightServer.Builder builder = FlightServer.builder(allocator,
      Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, /*port*/ 0),
      new TestProducer());
    builder.headerAuthenticator(
      new GeneratedBearerTokenAuthenticator(
        new BasicCallHeaderAuthenticator(TestConnector::validate)
      )
    );
    server = builder.build();
    server.start();
    location = server.getLocation();
    spark = SparkSession.builder()
      .appName("flightTest")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.allowMultipleContexts", "true")
      .config("spark.flight.endpoint.host", location.getUri().getHost())
      .config("spark.flight.endpoint.port", Integer.toString(location.getUri().getPort()))
      .config("spark.flight.auth.username", USERNAME_1)
      .config("spark.flight.auth.password", PASSWORD_1)
      .getOrCreate();
    csc = new FlightSparkContext(spark);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    AutoCloseables.close(server, allocator, spark);
  }

  private class DummyObjectOutputStream extends ObjectOutputStream {
    public DummyObjectOutputStream() throws IOException {
      super(new ByteArrayOutputStream());
    }
  }

  @Test(expected = None.class)
  public void testFlightPartitionReaderFactorySerialization() throws IOException {
    List<FlightClientMiddlewareFactory> middleware = new ArrayList<>();
    FlightClientOptions clientOptions = new FlightClientOptions("xxx", "yyy", "FooBar", "FooBar", "FooBar", middleware);
    FlightPartitionReaderFactory readerFactory = new FlightPartitionReaderFactory(JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(clientOptions));

    try (ObjectOutputStream oos = new DummyObjectOutputStream()) {
      oos.writeObject(readerFactory);
    }
  }

  @Test(expected = None.class)
  public void testFlightPartitionSerialization() throws IOException {
    Ticket ticket = new Ticket("FooBar".getBytes());
    FlightEndpoint endpoint = new FlightEndpoint(ticket, location);
    FlightPartition partition = new FlightPartition(new FlightEndpointWrapper(endpoint));
    try (ObjectOutputStream oos = new DummyObjectOutputStream()) {
      oos.writeObject(partition);
    }
  }

  @Test
  public void testConnect() {
    csc.read(TEST_FULL_COMMAND);
  }

  @Test
  public void testRead() {
    long count = csc.read(TEST_FULL_COMMAND).count();
    Assert.assertEquals(20, count);
  }

  @Test
  public void testSql() {
    long count = csc.readSql(TEST_FULL_COMMAND).count();
    Assert.assertEquals(20, count);
  }

  @Test
  public void testFilter() {
    Dataset<Row> df = csc.readSql(TEST_FULL_COMMAND);
    long count = df.filter(df.col("symbol").equalTo("USDCAD")).count();
    long countOriginal = csc.readSql(TEST_FULL_COMMAND).count();
    Assert.assertTrue(count < countOriginal);
  }

  private static class SizeConsumer implements Consumer<Row> {
    private int length = 0;
    private int width = 0;

    @Override
    public void accept(Row row) {
      length += 1;
      width = row.length();
    }
  }

  @Test
  public void testProject() {
    Dataset<Row> df = csc.readSql(TEST_FULL_COMMAND);
    SizeConsumer c = new SizeConsumer();
    df.select("bid", "ask", "symbol").toLocalIterator().forEachRemaining(c);
    long count = c.width;
    long countOriginal = csc.readSql(TEST_FULL_COMMAND).columns().length;
    Assert.assertTrue(count < countOriginal);
  }

  private static class TestProducer extends NoOpFlightProducer {
    private boolean parallel = false;

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
      parallel = true;
      listener.onNext(new Result("ok".getBytes()));
      listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
      Schema schema;
      List<FlightEndpoint> endpoints;
      if (parallel) {
        endpoints = ImmutableList.of(new FlightEndpoint(new Ticket(descriptor.getCommand()), location),
          new FlightEndpoint(new Ticket(descriptor.getCommand()), location));
      } else {
        endpoints = ImmutableList.of(new FlightEndpoint(new Ticket(descriptor.getCommand()), location));
      }
      String joe = new String(descriptor.getCommand());
      System.out.println("getFlightInfo - Command: " + joe);
      if (new String(descriptor.getCommand()).equals(TEST_PROJECTION_COMMAND)) {
        schema = new Schema(ImmutableList.of(
          Field.nullable("bid", Types.MinorType.FLOAT8.getType()),
          Field.nullable("ask", Types.MinorType.FLOAT8.getType()),
          Field.nullable("symbol", Types.MinorType.VARCHAR.getType()))
        );

      } else {
        schema = new Schema(ImmutableList.of(
          Field.nullable("bid", Types.MinorType.FLOAT8.getType()),
          Field.nullable("ask", Types.MinorType.FLOAT8.getType()),
          Field.nullable("symbol", Types.MinorType.VARCHAR.getType()),
          Field.nullable("bidsize", Types.MinorType.BIGINT.getType()),
          Field.nullable("asksize", Types.MinorType.BIGINT.getType()))
        );
      }
      return new FlightInfo(schema, descriptor, endpoints, 1000000, 10);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      String ted = new String(ticket.getBytes());
      System.out.println("getStream - ticket: " + ted);

      final int size = (new String(ticket.getBytes()).contains("USDCAD")) ? 5 : 10;

      if (new String(ticket.getBytes()).equals(TEST_PROJECTION_COMMAND)) {
        Float8Vector b = new Float8Vector("bid", allocator);
        Float8Vector a = new Float8Vector("ask", allocator);
        VarCharVector s = new VarCharVector("symbol", allocator);

        VectorSchemaRoot root = VectorSchemaRoot.of(b, a, s);
        listener.start(root);

        //batch 1
        root.allocateNew();
        for (int i = 0; i < size; i++) {
          b.set(i, (double) i);
          a.set(i, (double) i);
          s.set(i, (i % 2 == 0) ? new Text("USDCAD") : new Text("EURUSD"));
        }
        b.setValueCount(size);
        a.setValueCount(size);
        s.setValueCount(size);
        root.setRowCount(size);
        listener.putNext();

        // batch 2

        root.allocateNew();
        for (int i = 0; i < size; i++) {
          b.set(i, (double) i);
          a.set(i, (double) i);
          s.set(i, (i % 2 == 0) ? new Text("USDCAD") : new Text("EURUSD"));
        }
        b.setValueCount(size);
        a.setValueCount(size);
        s.setValueCount(size);
        root.setRowCount(size);
        listener.putNext();
        root.clear();
        listener.completed();
      } else {
        BigIntVector bs = new BigIntVector("bidsize", allocator);
        BigIntVector as = new BigIntVector("asksize", allocator);
        Float8Vector b = new Float8Vector("bid", allocator);
        Float8Vector a = new Float8Vector("ask", allocator);
        VarCharVector s = new VarCharVector("symbol", allocator);

        VectorSchemaRoot root = VectorSchemaRoot.of(b, a, s, bs, as);
        listener.start(root);

        //batch 1
        root.allocateNew();
        for (int i = 0; i < size; i++) {
          bs.set(i, (long) i);
          as.set(i, (long) i);
          b.set(i, (double) i);
          a.set(i, (double) i);
          s.set(i, (i % 2 == 0) ? new Text("USDCAD") : new Text("EURUSD"));
        }
        bs.setValueCount(size);
        as.setValueCount(size);
        b.setValueCount(size);
        a.setValueCount(size);
        s.setValueCount(size);
        root.setRowCount(size);
        listener.putNext();

        // batch 2

        root.allocateNew();
        for (int i = 0; i < size; i++) {
          bs.set(i, (long) i);
          as.set(i, (long) i);
          b.set(i, (double) i);
          a.set(i, (double) i);
          s.set(i, (i % 2 == 0) ? new Text("USDCAD") : new Text("EURUSD"));
        }
        bs.setValueCount(size);
        as.setValueCount(size);
        b.setValueCount(size);
        a.setValueCount(size);
        s.setValueCount(size);
        root.setRowCount(size);
        listener.putNext();
        root.clear();
        listener.completed();
      }
    }


  }
}
