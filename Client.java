import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Client {
    public static void main(String [] args) {
		if (args.length != 3) {
			System.err.println("Usage: java Client FE_host FE_port schedule_file");
			System.exit(-1);
		}
		ArrayList<ArrayList<Job>> schedules = CalculateSchedules.parseSchedules(args[2], true);
		try {
			//this is thrift initialization for a client
			TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			SincroniaService.Client client = new SincroniaService.Client(protocol);
			transport.open();

			client.setClients(schedules.size());
			transport.close();
		} catch (IllegalArgument illegalArgument) {
			illegalArgument.printStackTrace();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		ArrayList<Thread> threads = new ArrayList<>();
		for (int i = 0; i < schedules.size(); i++){
			int finalI = i;
			Runnable r = () -> {
				try {
					//this is thrift initialization for a client
					TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
					TTransport transport = new TFramedTransport(sock);
					TProtocol protocol = new TBinaryProtocol(transport);
					SincroniaService.Client client = new SincroniaService.Client(protocol);
					transport.open();
					if (schedules.get(finalI).size() > 0) {
						client.sendJobs(schedules.get(finalI), schedules.get(finalI).get(0).ingress);
					} else {
						client.sendJobs(schedules.get(finalI), -1);
					}
					transport.close();
				} catch (IllegalArgument illegalArgument) {
					illegalArgument.printStackTrace();
				} catch (TTransportException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
			};
			threads.add(new Thread(r));
		}
		for (Thread thread : threads) {
			thread.start();
		}
	}
}
