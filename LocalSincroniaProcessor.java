import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;

import java.util.ArrayList;
import java.util.LinkedHashSet;


public class LocalSincroniaProcessor {
    public static void main(String [] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: java FENode schedule_file");
			System.exit(-1);
		}
		BasicConfigurator.configure();

		ArrayList<ArrayList<Job>> schedules = CalculateSchedules.parseSchedules(args[0], true);
		ArrayList<Integer> ingresses = new ArrayList<>();
		LinkedHashSet<Integer> ids = new LinkedHashSet<Integer>();
		LinkedHashSet<String> egresses = new LinkedHashSet<String>();
		for (int i = 0; i < schedules.size(); i++) {
			if (schedules.get(i).size() > 0) {
				ingresses.add(schedules.get(i).get(0).ingress);
				for (Job job : schedules.get(i)) {
					ids.add(job.id);
					egresses.add(job.egress);
				}
			}
		}
		ArrayList<ArrayList<Job>> beNodeSchedules = CalculateSchedules.calculateSchedules(schedules,
				ingresses.toArray(new Integer[0]), ids.toArray(new Integer[0]), egresses.toArray(new String[0]));
		CalculateSchedules.printSchedules(beNodeSchedules);
    }
}
