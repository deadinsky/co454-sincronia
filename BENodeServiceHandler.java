import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BENodeServiceHandler implements SincroniaService.Iface {
	static Logger log = Logger.getLogger(BENodeServiceHandler.class.getName());

    public int sendJobs(List<Job> schedule) {
		for (Job job : schedule) {
			log.info("Job " + job.index + " processed in " + job.timeUnits);
		}
		return 0;
    }

    @Override
	public void setClients(int numClient) {/*noop*/}
	@Override
	public void initialize(String hostname, int port, int numThreads) {/*noop*/}
	@Override
	public void ping() throws TException {/*noop*/}
}
