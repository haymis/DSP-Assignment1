package Manager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class Reducer implements Runnable{
	
	private String jobDoneAckQueueURL;
	private AmazonSQSClient sqs;
	private ExecutorService reducerExecutor;
	private ConcurrentHashMap<String, Integer> clientsUUIDToURLLeft;
	private AtomicBoolean shouldTerminate;
	private Logger logger;
	private AWSCredentials credentials;
	private int sleepTime = 5*1000;
	private Object talkToTheBossLock;
	private ConcurrentHashMap<String, Integer> clientsUUIDToNumOfWorkers;
	private AtomicBoolean reducerDone;
	private AtomicBoolean mapperDone;
	
	public Reducer(String jobDoneAckQueueURL, ConcurrentHashMap<String, Integer> clientsUUIDToURLLeft,
			AmazonSQSClient sqs, AtomicBoolean shouldTerminate, int numOfThreads, Logger logger, AWSCredentials credentials,
			Object talkToTheBossLock, ConcurrentHashMap<String, Integer> requiredWorkersPerTask, AtomicBoolean mapperDone,
			AtomicBoolean reducerDone) {
		this.jobDoneAckQueueURL = jobDoneAckQueueURL;
		this.clientsUUIDToURLLeft = clientsUUIDToURLLeft;
		this.sqs = sqs;
		this.shouldTerminate = shouldTerminate;
		this.reducerExecutor = Executors.newFixedThreadPool(numOfThreads);
		this.logger = logger;
		this.credentials = credentials;
		this.talkToTheBossLock = talkToTheBossLock;
		this.clientsUUIDToNumOfWorkers = requiredWorkersPerTask;
		this.reducerDone = reducerDone;
		this.mapperDone = mapperDone;
		logger.info("Reducer Started");
	}
	
	public void logInfo(String message){
		logger.info("[REDUCER] - " + message);
	}

	public void run() {
		ReceiveMessageResult result;
        List<Message> messages;
        
		while(!mapperDone.get() || !clientsUUIDToURLLeft.isEmpty()){
		    do
	        {
	            result = sqs.receiveMessage(
	            			new ReceiveMessageRequest()
	            			.withQueueUrl(jobDoneAckQueueURL)
	            			.withMessageAttributeNames("All")
	            			.withMaxNumberOfMessages(10)
	            		);
	            messages = result.getMessages();	            
	            
	            if (messages.isEmpty()) {
	            	logInfo("Waiting for messages, sleeping "+ (this.sleepTime / 1000) +" second");
		            try {
		                Thread.sleep(sleepTime);
		            } catch (InterruptedException e) {
		                e.printStackTrace();
		            }
	            }

	        } while (messages.isEmpty());

	        messages = result.getMessages();
	        logInfo("New messages acks arrived from workers");

	        for (Message message : messages) {
	        	//logInfo("Appending message to file: " + message);
	            String receipt = message.getReceiptHandle();
	            Map<String, MessageAttributeValue> msgAttributes = message.getMessageAttributes();
	            if (msgAttributes.containsKey("UUID")){
	            	String localAppUUID = msgAttributes.get("UUID").getStringValue();
	            	int numOfUrlsLeft = clientsUUIDToURLLeft.get(localAppUUID) - 1;
	            	clientsUUIDToURLLeft.put(localAppUUID, numOfUrlsLeft);
	                sqs.deleteMessage(new DeleteMessageRequest(jobDoneAckQueueURL, receipt));
	                if (numOfUrlsLeft == 0) {
	                	handleNewTask(localAppUUID);
	                	clientsUUIDToURLLeft.remove(localAppUUID);
	                	clientsUUIDToNumOfWorkers.remove(localAppUUID);
	                	synchronized(talkToTheBossLock){
	                		talkToTheBossLock.notify();
	                	}
	                }
	            }
	        }
		}
		terminate();
	}

	private void terminate() {
		reducerExecutor.shutdown();
		try {
			while (!reducerExecutor.awaitTermination(60, TimeUnit.SECONDS))
				logInfo("Awaiting completion of reducer tasks.");
			
		} catch (InterruptedException e) {
			logInfo("Got interrupted while waiting for tasks to complete. shutting down :(");
			e.printStackTrace();
		}
		logInfo("Done waiting. Exiting...");
		reducerDone.set(true);
		synchronized(talkToTheBossLock){
			talkToTheBossLock.notifyAll();
		}
	}

	private void handleNewTask(String localAppUUID) {
        logInfo("Job " + localAppUUID + "is done. Starting new task to concat all responses and upload file");
        Runnable reduceTaskHandler = new ReduceTaskHandler(localAppUUID, logger,
        		sqs, credentials);
        this.reducerExecutor.execute(reduceTaskHandler);
        logInfo("Finished creating task handler");		
	}

}