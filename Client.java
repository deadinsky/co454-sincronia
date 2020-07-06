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
		ArrayList<List<Job>> schedules = new ArrayList<>();
		File scheduleFile = new File(args[2]);
		Integer numClients = 0;
		try {
			Scanner scheduleReader = new Scanner(scheduleFile);
			String firstLine[] = scheduleReader.nextLine().split(" ");
			numClients = Integer.valueOf(firstLine[1]);
			while (scheduleReader.hasNextLine()) {
				String currentLine[] = scheduleReader.nextLine().split(";");
				List<Job> schedule = new ArrayList<Job>();
				for (int i = 0; i < currentLine.length; i++) {
					String jobSplit[] = currentLine[i].split(",");
					int id = Integer.valueOf(jobSplit[0].substring(1));
					jobSplit = jobSplit[1].split(":");
					String egress = jobSplit[0].substring(1);
					boolean isNegative = false;
					int epsilon = 0;
					int timeUnits = 0;
					if (jobSplit[1].contains("e")) {
						if (jobSplit[1].contains("-")) {
							jobSplit = jobSplit[1].split("-");
							isNegative = true;
						} else if (jobSplit[1].contains("+")) {
							jobSplit = jobSplit[1].split("\\+");
						}
						timeUnits = Integer.valueOf(jobSplit[0].substring(1));
						if (jobSplit[1].charAt(0) == 'e') {
							epsilon = (isNegative ? -1 : 1);
						} else {
							jobSplit = jobSplit[1].split("e");
							epsilon = Integer.valueOf(jobSplit[0]) * (isNegative ? -1 : 1);
						}
					} else {
						timeUnits = Integer.valueOf(jobSplit[1].substring(1));
					}
					schedule.add(new Job(id, egress, timeUnits, epsilon));
				}
				schedules.add(schedule);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		if (numClients != schedules.size()) {
			System.err.println("Incorrect input file format, client size discrepancy.");
			System.exit(-1);
		}
		try {
			//this is thrift initialization for a client
			TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			SincroniaService.Client client = new SincroniaService.Client(protocol);
			transport.open();

			client.setClients(numClients);
			transport.close();
		} catch (IllegalArgument illegalArgument) {
			illegalArgument.printStackTrace();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		ArrayList<Thread> threads = new ArrayList<>();
		for (int i = 0; i < numClients; i++){
			int finalI = i;
			Runnable r = () -> {
				try {
					//this is thrift initialization for a client
					TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
					TTransport transport = new TFramedTransport(sock);
					TProtocol protocol = new TBinaryProtocol(transport);
					SincroniaService.Client client = new SincroniaService.Client(protocol);
					transport.open();

					client.sendJobs(schedules.get(finalI), finalI+1);
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
