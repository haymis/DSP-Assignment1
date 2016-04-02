package Manager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class ReduceTaskHandler implements Runnable {
	private static AtomicInteger reduceTaskCounter = new AtomicInteger(0);
	private String localAppUUID;
	private Logger logger;
	private AmazonSQSClient sqs;
	private AWSCredentials credentials;
	private int id;
	private String bucketName = "hayminirhodadi";
	private String responseFileName;
	private int numOfMsgesPerRequest = 1;

	
	public ReduceTaskHandler(String localAppUUID, Logger logger, AmazonSQSClient sqs, AWSCredentials credentials) {
		this.localAppUUID = localAppUUID;
		this.logger = logger;
		this.sqs = sqs;
		this.credentials = credentials;
		this.responseFileName = localAppUUID + "-response.txt";
		this.id = reduceTaskCounter.getAndIncrement();
	}

	public void logInfo(String message){
		logger.info("[REDUCE TASK HANDLER - " + this.id + "] - " + message);
	}
	
	public void run() {
		
        File file = new File(responseFileName);
        try {
			file.createNewFile();
		} catch (IOException e) {
			logInfo("Couldn't create file!");
			e.printStackTrace();
			return;
		}
        String responsesQueueURL = sqs.getQueueUrl("Responses-" + localAppUUID).getQueueUrl();
        
        List<Message> messages;
		ReceiveMessageResult result;
		
		try {
			write("[", file);
		} catch (IOException e) {
			logInfo("Couldn't write to file!");
			e.printStackTrace();
		}
		while(true){
			StringBuilder sb = new StringBuilder();
			
			result = getMsgFromQueue(responsesQueueURL, "All", numOfMsgesPerRequest);
	        messages = result.getMessages();
	        
	        if(messages.isEmpty()){
	        	logInfo("No more messages to add to file.");
	        	break;
	        }
	        
	        Map<String, MessageAttributeValue> msgAttributes;
	        for(Message message : messages){
	        	String receipt = message.getReceiptHandle();
	        	
	        	logInfo("Working on msg:");
	        	msgAttributes = message.getMessageAttributes();
	        	String tweetResult = msgAttributes.get("result").getStringValue();
	        	logInfo("Got result" + tweetResult);
	        	sb.append(tweetResult);
	        	sb.append(",\n");
	    		sqs.deleteMessage(new DeleteMessageRequest(responsesQueueURL, receipt));
	        }   
	        try {
				write(sb.toString(), file);
			} catch (IOException e) {
				logInfo("Couldn't write to file!");
				e.printStackTrace();
			}
		}
		try {
			write("]", file);
		} catch (IOException e) {
			logInfo("Couldn't write to file!");
			e.printStackTrace();
		}
		uploadToS3(file);
		sendMessageToSQS();
	}
	private void sendMessageToSQS() {
		logInfo("Sending message to the Local App's queue that response is ready, and the file path.");
        Map<String,MessageAttributeValue> attributes = new HashMap<String,MessageAttributeValue>();
        attributes.put("BucketName", new MessageAttributeValue().withDataType("String").withStringValue(bucketName));
        attributes.put("OutputFilename", new MessageAttributeValue().withDataType("String").withStringValue(responseFileName));
        
        String requestQueueURL = sqs.getQueueUrl("Request-" + localAppUUID).getQueueUrl();

 
        try{
            sqs.sendMessage(new SendMessageRequest().withQueueUrl(requestQueueURL)
            		.withMessageBody("Your response is ready, sir.")
            		.withMessageAttributes(attributes));
            logInfo("Message to Local App sent!");
        }
        catch (QueueDoesNotExistException e){
            logInfo("This is embarrasing! Couldn't find the local app queue.");
            e.printStackTrace();
        }
    }
	
	private void uploadToS3(File f) {
		logInfo("Uploading file to S3");
		AmazonS3 s3 = new AmazonS3Client(credentials);
        logInfo("Created Amazon S3 Client");
        if (!s3.doesBucketExist(bucketName))
            s3.createBucket(bucketName);
        logInfo("Created bucket.");
        PutObjectRequest por = new PutObjectRequest(bucketName, responseFileName, f);
        por.withCannedAcl(CannedAccessControlList.PublicRead);
        s3.putObject(por);
        logInfo("Done uploading " + responseFileName);        	
	}

	private ReceiveMessageResult getMsgFromQueue(String queueURL,
			String tagName, int maxMessages) {
		return sqs.receiveMessage(
    			new ReceiveMessageRequest()
    			.withQueueUrl(queueURL)
    			.withMessageAttributeNames(tagName)
    			.withMaxNumberOfMessages(maxMessages)
	    	  ); 
	}

	private static void write(String content, File file) throws IOException {
		try (FileWriter writer = new FileWriter(file.getAbsoluteFile(), true)) {
			writer.append(content);
		}
	}
	
}