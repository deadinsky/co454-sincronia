import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

public class ClientContainer {
    static Integer idCount = 0;
    public final int id;
    public int port;
    public String host;
    public Semaphore lock = new Semaphore(1,true);
    public CountDownLatch countDownLatch;
    public SincroniaService.AsyncClient client;

    public ClientContainer(SincroniaService.AsyncClient c, String host, int port){
        client = c;
        this.host = host;
        this.port = port;
        synchronized (idCount){
            id = idCount++;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientContainer that = (ClientContainer) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
