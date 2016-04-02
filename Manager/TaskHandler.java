package Manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class TaskHandler implements Runnable {
	private static AtomicInteger taskCounter = new AtomicInteger(0);
	private static final int MAX_WORKERS = 20;
	private Map<String, MessageAttributeValue> msgAtrributes;
	private ConcurrentHashMap<String, Integer> clientsUUIDToURLLeft;
	private Logger logger;
	private AmazonSQSClient sqs;
	private AWSCredentials credentials;
	private String jobsQueueURL;
	private AmazonEC2 ec2;
	private int id;
	
	public TaskHandler(ConcurrentHashMap<String, Integer> clientsUUIDToURLLeft,
			Map<String, MessageAttributeValue> msgAtrributes, Logger logger, AmazonSQSClient sqs,
			AWSCredentials credentials, String jobsQueueUrl){
		this.clientsUUIDToURLLeft = clientsUUIDToURLLeft;
		this.msgAtrributes = msgAtrributes;
        this.logger = logger;
        this.sqs = sqs;
        this.credentials = credentials;
        this.jobsQueueURL = jobsQueueUrl;
        this.id = taskCounter.getAndIncrement(); 
        this.ec2 = new AmazonEC2Client(credentials);
		logger.info("===== Handler Log Started =====");
	}
	
	public void logInfo(String message){
		logger.info("[TaskHandler - " + this.id + "] - " + message);
	}
	
	public void run() {
		
		String localAppUUID = this.msgAtrributes.get("UUID").getStringValue();
		logInfo("Started running on uuid " + localAppUUID);
		int numOfMessages = Integer.parseInt(this.msgAtrributes.get("NumOfURLs").getStringValue());
		String inputFilePath = this.msgAtrributes.get("InputFilename").getStringValue();
		String bucketName = this.msgAtrributes.get("BucketName").getStringValue();
		int workersRatio = Integer.parseInt(this.msgAtrributes.get("WorkerPerMessage").getStringValue());
		if(workersRatio == 0)
			workersRatio = 30;  
		int numOfWorkers = numOfMessages / workersRatio;
				
		sqs.createQueue(new CreateQueueRequest("Responses-" + localAppUUID)).getQueueUrl();		
		clientsUUIDToURLLeft.put(localAppUUID, numOfMessages);
		//createWorkers(numOfWorkers);
		try {
			downloadInputFileFromS3(bucketName, inputFilePath, localAppUUID);
		} catch (IOException e) {
			logger.info("[MapperTask] - couldn't read from file.");
			e.printStackTrace();
		}
		
	}

	private void createWorkers(int numOfWorkers) {
		// Calculate num of workers to add.
		
		
        int absoluteToAdd = Integer.min(numOfWorkers - getNumOfCurrentWorkers(), MAX_WORKERS);
        if (absoluteToAdd <= 0) { // have enough workers.
            logger.info("[MAPPER Task] - No more workers needed.");
            return;
        }
        logInfo("Creating " + absoluteToAdd + " new workers");

        RunInstancesRequest runWorkerRequest = new RunInstancesRequest("ami-b66ed3de", absoluteToAdd, absoluteToAdd);
        runWorkerRequest.setKeyName("nirhaymi");
        runWorkerRequest.setInstanceType(InstanceType.T2Micro.toString());

        // Bootstrapping the new Worker instance
//            try {
//                runWorkerRequest.setUserData(getBootStrapScript("/tmp/worker_bootstrap.sh"));
//            } catch (IOException e) {
//            	logger.info("[MAPPER Task] - Cannot find bootstrap file or failed in getBootStrapScript");
//                e.printStackTrace();                
//            }
        List<Instance> instances = ec2.runInstances(runWorkerRequest).getReservation().getInstances();
        if (instances.size() != absoluteToAdd)
        	logInfo("Couldn't create required amount of workers. Created " + instances.size() + " instead of " + absoluteToAdd);
        
        for (int i=0; i < instances.size(); i++) {
            String workerID = instances.get(i).getInstanceId();
            //this.workerInstancesIds.add(workerID);

            // Creating tags for workers instances
            CreateTagsRequest createTagsRequest = new CreateTagsRequest().withResources(workerID)
                    .withTags(new Tag("Type", "Worker"), new Tag("ID", workerID));
            ec2.createTags(createTagsRequest);
        }

        logger.info("[MAPPER Task] - Workers added.");
		
	}
	
	private int getNumOfCurrentWorkers()
    {	
		int numOfCurrentWorkers = 0;
        this.ec2 = new AmazonEC2Client(credentials);
        Filter tagFilter = new Filter("tag:Type", Arrays.asList("Worker"));
        Filter stateFilter = new Filter("instance-state-name", Arrays.asList("stopped", "pending", "running"));

        DescribeInstancesResult result = ec2.describeInstances((new DescribeInstancesRequest())
        		.withFilters(tagFilter, stateFilter));
//
//        if (!result.getReservations().isEmpty()) 
//        	numOfCurrentWorkers = result.getReservations().get(0).getInstances().size();
        for (int i = 0; i < result.getReservations().size(); i++) {
        	numOfCurrentWorkers += result.getReservations().get(i).getInstances().size();
        }
        logInfo("numOfCurrentWorkers - " + numOfCurrentWorkers);
        
        return numOfCurrentWorkers; 
    }

	private void downloadInputFileFromS3(String bucketName, String inputFilePath, String localAppUUID) throws IOException {

        logInfo("Downloading input file");
        AmazonS3 s3 = new AmazonS3Client(credentials);
        S3Object inputFile = s3.getObject(new GetObjectRequest(bucketName, inputFilePath));
        
        InputStream inputFileData = inputFile.getObjectContent();
        logInfo("Done Downloading input file");

        int urlNumber = 0;

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputFileData));
        String url;
        logInfo("Sending jobs to workers.");
        while ((url = reader.readLine()) != null) {
            sendJobToWorkers(url, urlNumber, localAppUUID);
            urlNumber++;
        }
        logInfo("Done sending " + urlNumber + " jobs to workers.");

        inputFileData.close();
    }
	
    private void sendJobToWorkers(String url, int urlNumber, String localAppUUID) {

        //logger.info("Task Handler :: Sending job #"+urlNumber);

        Map<String, MessageAttributeValue> attributes = new HashMap<String, MessageAttributeValue>();

        attributes.put("Url", new MessageAttributeValue().withDataType("String").withStringValue(url));      
        attributes.put("UUID", new MessageAttributeValue().withDataType("String").withStringValue(localAppUUID));

        sqs.sendMessage(new SendMessageRequest().withQueueUrl(jobsQueueURL).withMessageBody("New URL. Please process").withMessageAttributes(attributes));

        //logger.info("Task Handler :: Sent job #"+urlNumber);
    }
	
}

