package org.apache.hadoop.hive.cassandra;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * A proxy client connects to cassandra backend server.
 *
 */
public class CassandraProxyClient implements java.lang.reflect.InvocationHandler {

  private static final Logger logger = Logger.getLogger(CassandraProxyClient.class);

  /**
   * The initial host to create the proxy client.
   */
  private final String host;
  private final int port;
  private final boolean framed;

  /**
   * The last successfully connected server.
   */
  private String lastUsedHost;
  /**
   * Last time the ring was checked.
   */
  private long lastPoolCheck;

  /**
   * Cassandra thrift client.
   */
  private ClientHolder clientHolder;

  /**
   * The key space to get the ring information from.
   */
  private String ringKs;

  /**
   * Option to choose the next server from the ring. Default is RoundRobin unless
   * specified by the client to choose randomizer.
   */
  private RingConnOption nextServerGen;

  /**
   * Maximum number of attempts when connection is lost.
   */
  private final int maxAttempts = 10;

  /**
   * Construct a proxy connection.
   *
   * @param host
   *          cassandra host
   * @param port
   *          cassandra port
   * @param framed
   *          true to used framed connection
   * @param randomizeConnections
   *          true if randomly choosing a server when connection fails; false to use round-robin
   *          mechanism
   * @return a Brisk Client Interface
   * @throws IOException
   */
  public static ClientHolder newProxyConnection(String host, int port, boolean framed,
      boolean randomizeConnections)
      throws CassandraException {
    return (ClientHolder) java.lang.reflect.Proxy.newProxyInstance(ClientHolder.class
        .getClassLoader(),
              CassandraClientHolder.class.getInterfaces(), new CassandraProxyClient(host, port, framed,
                      randomizeConnections));
  }


  /**
   * Create connection to a given host.
   *
   * @param host
   *          cassandra host
   * @return cassandra thrift client
   * @throws CassandraException
   *           error
   */
  private CassandraClientHolder createConnection(String host) throws CassandraException {
    TSocket socket = new TSocket(host, port);
    TTransport trans = framed ? new TFramedTransport(socket) : socket;

    CassandraClientHolder ch = new CassandraClientHolder(trans);

    return ch;
  }

  private CassandraProxyClient(String host, int port, boolean framed, boolean randomizeConnections)
      throws CassandraException {
    this.host = host;
    this.port = port;
    this.framed = framed;
    this.lastUsedHost = host;
    this.lastPoolCheck = 0;

    // If randomized to choose a connection, initialize the random generator.
    if (randomizeConnections) {
      nextServerGen = new RandomizerOption();
    } else {
      nextServerGen = new RoundRobinOption();
    }

    initializeConnection();
  }

  /**
   * Initialize the cassandra connection with the initial given cassandra host.
   * Create a temporary keyspace if no one exists. Otherwise, choose the first non-system
   * keyspace to describe the ring.
   * Initialize the ring.
   *
   * @throws IOException
   */
  private void initializeConnection() throws CassandraException {
    clientHolder = createConnection(host);

    if (logger.isDebugEnabled()) {
      logger.debug("Connected to cassandra at " + host + ":" + port);
    }

    assert clientHolder.isOpen();

    // Find the first keyspace that's not system and assign it to the lastly used keyspace.
    try {
      List<KsDef> allKs = clientHolder.getClient().describe_keyspaces();

      if (allKs.isEmpty() || (allKs.size() == 1 && allKs.get(0).name.equalsIgnoreCase("system"))) {
        allKs.add(createTmpKs());
      }

      for (KsDef ks : allKs) {
        if (!ks.name.equalsIgnoreCase("system")) {
          ringKs = ks.name;
          break;
        }
      }

      // Set the ring keyspace for initialization purpose. This value
      // should be overwritten later by set_keyspace
      clientHolder.getClient(ringKs);
    } catch (InvalidRequestException e) {
      throw new CassandraException(e);
    } catch (TException e) {
      throw new CassandraException(e);
    } catch (SchemaDisagreementException e){
      throw new CassandraException(e);
    }

    checkRing();
  }

  /**
   * Create a temporary keyspace. This will only be called when there is no keyspace except system
   * defined on (new cluster).
   * However we need a keyspace to call describe_ring to get all servers from the ring.
   *
   * @return the temporary keyspace
   * @throws InvalidRequestException
   *           error
   * @throws TException
   *           error
   * @throws SchemaDisagreementException
   * @throws InterruptedException
   *           error
   */
  private KsDef createTmpKs() throws InvalidRequestException, TException, SchemaDisagreementException {

    Map<String, String> stratOpts = new HashMap<String, String>();
    stratOpts.put("replication_factor", "1");

    KsDef tmpKs = new KsDef("proxy_client_ks", "org.apache.cassandra.locator.SimpleStrategy",
        Arrays.asList(new CfDef[] {})).setStrategy_options(stratOpts);

    clientHolder.getClient().system_add_keyspace(tmpKs);

    return tmpKs;
  }

  /**
   * Refresh the server in the ring.
   *
   * @throws TException
   * @throws InvalidRequestException
   *
   * @throws IOException
   */
  private void checkRing() throws CassandraException {
    assert clientHolder != null;

    long now = System.currentTimeMillis();

    if ((now - lastPoolCheck) > 60 * 1000) {
      List<TokenRange> ring;
      try {
        ring = clientHolder.getClient().describe_ring(ringKs);
      } catch (InvalidRequestException e) {
        throw new CassandraException(e);
      } catch (TException e) {
        throw new CassandraException(e);
      }
      lastPoolCheck = now;
      nextServerGen.resetRing(ring);
    }

  }

  /**
   * Attempt to connect to the next available server.
   * If there is no server in the ring, an exception will be thrown out.
   * If there is no server in the ring that is different from the server used last time,
   * we should try the same server again in case that it recovers.
   * Otherwise, try to connect to a different server.
   *
   * @throws error
   *           when there is no server to connect from the ring.
   */
  private void attemptReconnect() throws CassandraException {
    String endpoint = nextServerGen.getNextServer(lastUsedHost);

    if (endpoint != null) {
      clientHolder = createConnection(endpoint);
      lastUsedHost = endpoint; // Assign the last successfully connected server.
      checkRing(); // Refresh the servers in the ring.
      logger.info("Connected to cassandra at " + endpoint + ":" + port);
    } else {
      clientHolder = createConnection(lastUsedHost);
    }
  }

  public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
    Object result = null;

    int tries = 0;

    while (result == null && tries++ < maxAttempts) {
      try {
        if (clientHolder == null) {
          // Let's try to connect to the next server.
          attemptReconnect();
        }

        if (clientHolder != null && clientHolder.isOpen()) {
          result = m.invoke(clientHolder, args);

          if (m.getName().equalsIgnoreCase("getClient") && args != null && args.length == 1) {
            // Keep last known keyspace when set_keyspace is successfully invoked.
            ringKs = (String) args[0];
          }

          return result;
        }
      } catch (CassandraException e) {
        // We are unable to connect to any server in the ring, let's continue trying
        // until we hit the maximum number of attempts.
        if (tries >= maxAttempts) {
          throw e.getCause();
        }
      } catch (InvocationTargetException e) {
        // Error is from cassandra thrift server
        if (e.getTargetException() instanceof UnavailableException ||
                  e.getTargetException() instanceof TimedOutException ||
                  e.getTargetException() instanceof TTransportException) {
          // These errors seem due to not being able to connect the cassandra server.
          // If this is last try quit the program; otherwise keep trying.
          if (tries >= maxAttempts) {
            throw e.getCause();
          }
        } else {
          // The other errors, we should not keep trying.
          throw e.getCause();
        }
      }
    }

    throw new CassandraException("Not able to connect to any server in the ring " + lastUsedHost);
  }

  /**
   * A class to implement the method of getting the next servers from the ring.
   *
   */
  public abstract class RingConnOption {
    protected List<String> servers;

    protected RingConnOption() {

    }

    protected RingConnOption(List<TokenRange> servers) {
      this.servers = getAllServers(servers);
    }

    /**
     * Return the next server from the ring. If there is no server in the ring, throw an exception.
     * If there is only one server in the ring, return the server if it is different from the server
     * tried last time.
     * If there are more than two servers in the ring, return the server that is different from the
     * server tried last time;
     * if there is no server that is different from the server tried last time, return null;
     *
     * @param the
     *          last host used for connection
     * @return next server for connection
     */
    public String getNextServer(String host) throws CassandraException {
      if (!checkServerHealth()) {
        throw new CassandraException("No server is available from the ring.");
      }

      if (servers.size() == 1) {
        if (servers.get(0).equals(host)) {
          return null;
        } else {
          return servers.get(0);
        }
      } else {
        return getServerFromRing(host);
      }
    }

    /**
     * Retrieve the next server from the ring.
     *
     * In the constructor, all servers from the ring are hashed and mapped. Theoretically there
     * should be no duplicated server
     * in the ring.
     *
     * @param host
     *          the last host used for connection
     * @return new server for connection
     */
    protected abstract String getServerFromRing(String host);

    /**
     * Reset the servers in the ring.
     */
    public void resetRing(List<TokenRange> servers) {
      this.servers = getAllServers(servers);
    }

    private List<String> getAllServers(List<TokenRange> input) {
      HashMap<String, Integer> map = new HashMap<String, Integer>(input.size());
      for (TokenRange thisRange : input) {
        List<String> servers = thisRange.endpoints;
        for (String newServer : servers) {
          map.put(newServer, new Integer(1));
        }
      }

      return new ArrayList<String>(map.keySet());
    }

    /**
     * Check the health of the server ring.
     *
     * @return If there is no server in the ring, return false; Otherwise return true.
     */
    private boolean checkServerHealth() {
      if (servers == null || servers.size() == 0) {
        logger.warn("No cassandra ring information found, no node is available to connect to");
        return false;
      }

      return true;
    }
  }

  /**
   * Randomly choose a server from the ring to connect.
   */
  public class RandomizerOption extends RingConnOption {

    private final Random generator;

    public RandomizerOption() {
      super();
      generator = new Random();
    }

    public RandomizerOption(List<TokenRange> rings) {
      super(rings);
      generator = new Random();
    }

    @Override
    protected String getServerFromRing(String thisHost) {
      String endpoint = thisHost;

      while (!endpoint.equals(thisHost)) {
        int index = generator.nextInt(servers.size());
        endpoint = servers.get(index);
      }

      return endpoint;
    }
  }

  /**
   * Choose a server using round-robin mechanism.
   */
  public class RoundRobinOption extends RingConnOption {
    private int lastUsedIndex;

    public RoundRobinOption() {
      super();
    }

    public RoundRobinOption(List<TokenRange> rings) {
      super(rings);
      lastUsedIndex = 0;
    }

    @Override
    protected String getServerFromRing(String thisHost) {
      String endpoint = thisHost;

      while (!endpoint.equals(thisHost)) {
        lastUsedIndex++;
        // Start from beginning if reaches to the last server in the ring.
        if (lastUsedIndex == servers.size()) {
          lastUsedIndex = 0;
        }

        endpoint = servers.get(lastUsedIndex);
      }

      return endpoint;
    }
  }
}
