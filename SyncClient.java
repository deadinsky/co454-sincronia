import org.apache.thrift.transport.TTransport;

//just encapsulates a synchronized client
public class SyncClient {
    public final SincroniaService.Client client;
    public final TTransport transport;
    public final String host;
    public final int port;

    public SyncClient(SincroniaService.Client client, TTransport transport, String host, int port){
        this.client = client;
        this.transport = transport;
        this.host = host;
        this.port = port;
    }
}
