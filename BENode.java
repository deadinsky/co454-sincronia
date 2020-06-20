import java.net.InetAddress;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


public class BENode {
    static Logger log;
	final static int MAX_WORKER_THREADS = 64;

    public static void main(String [] args) throws Exception {
	if (args.length != 3) {
	    System.err.println("Usage: java BENode FE_host FE_port BE_port");
	    System.exit(-1);
	}

	// initialize log4j
	BasicConfigurator.configure();
	log = Logger.getLogger(BENode.class.getName());

	String hostFE = args[0];
	int portFE = Integer.parseInt(args[1]);
	int portBE = Integer.parseInt(args[2]);
	log.info("Launching BE node on port " + portBE + " at host " + getHostName());

	// launch Thrift server
	SincroniaService.Processor processor = new SincroniaService.Processor<SincroniaService.Iface>(new BENodeServiceHandler());
	TServerSocket socket = new TServerSocket(portBE);
	TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
	sargs.protocolFactory(new TBinaryProtocol.Factory());
	sargs.transportFactory(new TFramedTransport.Factory());
	sargs.processorFactory(new TProcessorFactory(processor));
	sargs.maxWorkerThreads(MAX_WORKER_THREADS);
	TThreadPoolServer server = new TThreadPoolServer(sargs);

	//initialize front end client, this is expensive so only do once
	TSocket sock = new TSocket(hostFE, portFE);
	TTransport transport = new TFramedTransport(sock);
	TProtocol protocol = new TBinaryProtocol(transport);
	SincroniaService.Client client = new SincroniaService.Client(protocol);

	System.out.println("Initialized client");
	boolean connected = false;
		while(!connected){
					//busy wait one second before trying to get the front end again
					Thread.sleep(1000);
					try {
						transport.open();
						log.info(getHostName());
						client.initialize(getHostName(), portBE,
								Math.min(MAX_WORKER_THREADS, Runtime.getRuntime().availableProcessors()));
						log.info("Connected to Frontend");
						connected = true;
						transport.close();
					} catch(TException e) {
						log.info("Couldn't connect to front end trying in one second");
				}
		}
		server.serve();
    }

    static String getHostName()
    {
	try {
	    return InetAddress.getLocalHost().getHostName();
	} catch (Exception e) {
	    return "localhost";
	}
    }
}
