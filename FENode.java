import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;


public class FENode {
    static Logger log;

    public static void main(String [] args) throws Exception {
	if (args.length != 1) {
	    System.err.println("Usage: java FENode FE_port");
	    System.exit(-1);
	}

	// initialize log4j
	BasicConfigurator.configure();
	log = Logger.getLogger(FENode.class.getName());

	int portFE = Integer.parseInt(args[0]);
	log.info("Launching FE node on port " + portFE);

	// launch Thrift server
	SincroniaService.Processor processor = new SincroniaService.Processor<SincroniaService.Iface>(new FENodeServiceHandler());
	TServerSocket socket = new TServerSocket(portFE);
	TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
	sargs.protocolFactory(new TBinaryProtocol.Factory());
	sargs.transportFactory(new TFramedTransport.Factory());
	sargs.processorFactory(new TProcessorFactory(processor));
	//he said max he will test with is 20, 25 will for sure allow backends to come and go as they please
	// without excessive waiting
	sargs.maxWorkerThreads(25);
	TThreadPoolServer server = new TThreadPoolServer(sargs);
	server.serve();
    }
}
