import org.apache.log4j.Logger;

import java.util.List;
public class SendJobsCallback implements org.apache.thrift.async.AsyncMethodCallback<Integer> {
    static org.apache.log4j.Logger log = Logger.getLogger(SendJobsCallback.class.getName());
    ClientContainer clientContainer;
    CircularLinkedList circularLinkedList;
    SendJobsCallback(ClientContainer clientContainer, CircularLinkedList circularLinkedList){
        this.clientContainer = clientContainer;
        this.circularLinkedList = circularLinkedList;
    }

    @Override
    public void onComplete(Integer returnCode) {
        clientContainer.countDownLatch.countDown();
    }

    @Override
    public void onError(Exception e) {
        log.error(e.getMessage());
        //just remove all references of the backend node
        circularLinkedList.delete(clientContainer);
    }
}
