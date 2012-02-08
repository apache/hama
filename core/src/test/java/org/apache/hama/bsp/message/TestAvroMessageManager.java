package org.apache.hama.bsp.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BooleanMessage;
import org.apache.hama.bsp.DoubleMessage;
import org.apache.hama.bsp.IntegerMessage;

public class TestAvroMessageManager {

  private static NettyServer server;
  private static Server hadoopServer;
  private static long start;

  public static final class MessageSender implements Sender {

    @Override
    public Void transfer(AvroBSPMessageBundle messagebundle)
        throws AvroRemoteException {
      try {
        BSPMessageBundle msg = deserializeMessage(messagebundle.data);
        System.out.println("Received message in "
            + (System.currentTimeMillis() - start) + "ms");
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

  }

  private static final BSPMessageBundle deserializeMessage(ByteBuffer buffer)
      throws IOException {
    BSPMessageBundle msg = new BSPMessageBundle();

    ByteArrayInputStream inArray = new ByteArrayInputStream(buffer.array());
    DataInputStream in = new DataInputStream(inArray);
    msg.readFields(in);

    return msg;
  }

  private static final ByteBuffer serializeMessage(BSPMessageBundle msg)
      throws IOException {
    ByteArrayOutputStream outArray = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(outArray);
    msg.write(out);
    out.close();
    System.out.println("serialized " + outArray.size() + " bytes");
    return ByteBuffer.wrap(outArray.toByteArray());
  }

  public static final BSPMessageBundle getRandomBundle() {
    BSPMessageBundle bundle = new BSPMessageBundle();

    for (int i = 0; i < 500000; i++) {
      bundle.addMessage(new IntegerMessage("test", i));
    }

    for (int i = 0; i < 10000; i++) {
      bundle.addMessage(new BooleanMessage("test123", i % 2 == 0));
    }

    Random r = new Random();
    for (int i = 0; i < 400000; i++) {
      bundle.addMessage(new DoubleMessage("123123asd", r.nextDouble()));
    }

    return bundle;
  }

  public static final void main(String[] args) throws IOException {
    BSPMessageBundle randomBundle = getRandomBundle();
    testAvro(randomBundle);
    testHadoop(randomBundle);
  }

  private static final void testAvro(BSPMessageBundle bundle)
      throws IOException, AvroRemoteException {

    server = new NettyServer(new SpecificResponder(Sender.class,
        new MessageSender()), new InetSocketAddress(13530));

    NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(server
        .getPort()));
    Sender proxy = (Sender) SpecificRequestor.getClient(Sender.class, client);

    AvroBSPMessageBundle msg = new AvroBSPMessageBundle();

    msg.setData(serializeMessage(bundle));

    start = System.currentTimeMillis();
    proxy.transfer(msg);

    server.close();
    client.close();
  }

  private static interface RPCTestInterface extends VersionedProtocol {

    public void transfer(BSPMessageBundle bundle);

  }

  private static class HadoopRPCInstance implements RPCTestInterface {

    @Override
    public long getProtocolVersion(String arg0, long arg1) throws IOException {
      return 0;
    }

    @Override
    public void transfer(BSPMessageBundle bundle) {
      System.out.println("Received message in "
          + (System.currentTimeMillis() - start) + "ms");
    }

  }

  private static final void testHadoop(BSPMessageBundle bundle)
      throws IOException {
    Configuration conf = new Configuration();
    HadoopRPCInstance hadoopRPCInstance = new HadoopRPCInstance();
    hadoopServer = new Server(hadoopRPCInstance, conf, new InetSocketAddress(
        13612).getHostName(), 13612);
    hadoopServer.start();
    RPCTestInterface proxy = (RPCTestInterface) RPC.getProxy(
        RPCTestInterface.class, 0, new InetSocketAddress(13612), conf);
    start = System.currentTimeMillis();
    proxy.transfer(bundle);
    hadoopServer.stop();
  }

}
