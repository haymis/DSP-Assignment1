package Manager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class Mapper implements Runnable{
	private AmazonEC2 ec2;
	private AmazonSQSClient sqs;
	private String jobsQueueURL;
	private String clientsQueueURL;
	private ExecutorService mapperExecutor;
	private ConcurrentHashMap<String, Integer> clientsUUIDToURLLeft;
	private AtomicBoolean shouldTerminate;
	private Logger logger;
	private AWSCredentials credentials;
	private int sleepTime = 1000;

	public Mapper(String jobsQueueURL2, String clientsQueueURL2,
			ConcurrentHashMap<String, Integer> clientsUUIDToURLLeft,
			AmazonSQSClient sqs2, AmazonEC2 ec2, AtomicBoolean shouldTerminate2, 
			int numOfThreads, Logger logger, AWSCredentials credentials) {
		this.jobsQueueURL = jobsQueueURL;
		this.clientsQueueURL =clientsQueueURL;
		this.clientsUUIDToURLLeft = clientsUUIDToURLLeft;
		this.sqs = sqs;
		this.ec2 = ec2;
		this.shouldTerminate = shouldTerminate;
		this.mapperExecutor = Executors.newFixedThreadPool(numOfThreads);
		this.logger = logger;
		this.credentials = credentials;
		logger.info("[MAPPER] - Mapper Started");
	}

	public void run() {
		ReceiveMessageResult result ;
        List<Message> messages ;
        
		while(!shouldTerminate.get()){
		    do
	        {
	            result = sqs.receiveMessage(
	            			new ReceiveMessageRequest()
	            			.withQueueUrl(clientsQueueURL)
	            			.withMessageAttributeNames("All")
	            		);
	            messages = result.getMessages();

	            logger.info("[MAPPER] - Waiting for messages, sleeping "+ (this.sleepTime / 1000) +" second");
	            
	            try {
	                Thread.sleep(sleepTime);
	            } catch (InterruptedException e) {
	                e.printStackTrace();
	            }

	        } while (messages.isEmpty());

	        messages = result.getMessages();
	        logger.info("[MAPPER] - New messages arrived from local app! Handling");

	        for (Message message: messages) {
	            String receipt = message.getReceiptHandle();
	            Map<String, MessageAttributeValue> msgAttributes = message.getMessageAttributes();
	            if (msgAttributes.containsKey("Num-of-URLs")){
	                handleNewTask(msgAttributes);
	                sqs.deleteMessage(new DeleteMessageRequest(clientsQueueURL, receipt));
	            }
//	            else if (msgAttributes.containsKey("Terminate")) {
//	                myInstanceID = msgAttributes.get("Manager-ID").getStringValue();
//	                terminateManager();
//	            }
	        }
		}
	}
	
//		msgAttributes.containsKey("uuid")) {
//	clientsUUIDToURLLeft.put(msgAttributes.get("uuid").getStringValue(), 
//			Integer.parseInt(msgAttributes.get("Num-of-URLs").getStringValue()));
	
	private void handleNewTask(Map<String, MessageAttributeValue> msgAtrributes) {

        logger.info("[MAPPER] - Starting to handle new task from local app");
        Runnable taskHandler = new TaskHandler(clientsUUIDToURLLeft, msgAtrributes, logger,
        		sqs, credentials, jobsQueueURL);
        this.mapperExecutor .execute(taskHandler);

        logger.info("Manager :: Finished creating task handler");

    }

}
