package Manager;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Manager {
	
	private static AWSCredentials credentials;
	private static AmazonEC2 ec2;
	private static AmazonSQSClient sqs;
//	private static ExecutorService executor;
	private static Logger logger = Logger.getLogger("Manager Logger");
//	private static Vector<String> workerInstanceIds;
	private static ConcurrentHashMap<String, Integer> clientsUUIDToURLLeft;
	private static ConcurrentHashMap<String, Integer> requiredWorkersPerTask;
	private static Object talkToTheBossLock;
	private static AtomicBoolean shouldTerminate;
//	private static String myInstanceID;
	private static String jobDoneAckQueueURL;
	private static String managerWorkersQueue;
	private static String clientsQueueURL;
	private static String jobsQueueURL;
	private static int mapperNumOfThreads = 3;
	private static int reducerNumOfThreads = 3;
	//private static String credentialsFilePath = "/tmp/rootkey.properties";
	private static String credentialsFilePath = "C:\\Users\\Haymi\\Documents\\BGU\\DSP\\rootkey.properties";
	private static String logFilePath = "C:\\Users\\Haymi\\Documents\\BGU\\DSP\\managerLog.txt"; //"/tmp/managerLog.log";
	static Map<String, MessageAttributeValue> fireWorkerMsg;
	
	public static void main(String[] args) {
		shouldTerminate = new AtomicBoolean(false);
		FileHandler fh;
        try {
            fh = new FileHandler(logFilePath);
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
            logger.info("===== Manager started =====");
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        try {
            initEC2Client();
            initSQSQueues();
        } catch (IOException e) {
            logger.info("cannot find access key file!");
        }
        
        fireWorkerMsg = new HashMap<String, MessageAttributeValue>();
        fireWorkerMsg.put("Terminate", new MessageAttributeValue().withDataType("String").withStringValue("Terminate")); 
        
        talkToTheBossLock = new Object();
        clientsUUIDToURLLeft = new ConcurrentHashMap<String, Integer>();
        requiredWorkersPerTask = new ConcurrentHashMap<String, Integer>();
//        jobsQueueURL = sqs.getQueueUrl("JobsQueue").getQueueUrl();
//        informationQueue = sqs.getQueueUrl("InformationQueue").getQueueUrl();
//        jobDoneAckQueue = sqs.createQueue(new CreateQueueRequest("JobDoneAckQueue")).getQueueUrl();
        Runnable mapper = new Mapper(jobsQueueURL, clientsQueueURL, requiredWorkersPerTask,
        		clientsUUIDToURLLeft, sqs, ec2, shouldTerminate, mapperNumOfThreads,
        		logger, credentials, talkToTheBossLock);
        Runnable reducer = new Reducer(jobDoneAckQueueURL, clientsUUIDToURLLeft, sqs,
        		shouldTerminate, reducerNumOfThreads, logger, credentials, talkToTheBossLock, requiredWorkersPerTask);
        
        Thread mapperThread = new Thread(mapper);
        Thread reducerThread = new Thread(reducer);
        
        mapperThread.start();
        reducerThread.start();
        
        while(!shouldTerminate.get()){
        	synchronized(talkToTheBossLock){
            	try {
            		logInfo("SLEEPING ON LOCK!");
					talkToTheBossLock.wait();
					logInfo("MANAGER HAS SOME WORK!!!");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
            	manage();
            }
        }
//        terminateServer();
    }
		
	private static void manage(){
		logInfo("Managing");
		ec2 = new AmazonEC2Client(credentials);
        Filter tagFilter = new Filter("tag:Type", Arrays.asList("Worker"));
        Filter stateFilter = new Filter("instance-state-name", Arrays.asList("pending", "running"));

        DescribeInstancesResult result = ec2.describeInstances((new DescribeInstancesRequest())
        		.withFilters(tagFilter, stateFilter));
        logInfo("checking for workers");
        
        int numOfCurrentWorkers = 0;
        if(result != null && !result.getReservations().isEmpty())
        	numOfCurrentWorkers = getNumOfCurrentWorkers(result);
        
        int numOfRequiredWorkers = getNumOfRequiredWorkers();
        logInfo("currentWorkers = " + numOfCurrentWorkers + ". neededWorkers = " + numOfRequiredWorkers);
        BalanceWorkers(numOfRequiredWorkers - numOfCurrentWorkers);
        logInfo("Done round of management");
	}
	private static void BalanceWorkers(int balance) {
		logInfo("Balancing workers. Got - " + balance);
		if(balance < 0)
			fireWorkers((-1) * balance);
		else if(balance > 0)
			recruitWorkers(balance);
	}

	private static void recruitWorkers(int numOfWorkers) {
		logInfo("Creating new " + numOfWorkers + " Workers");
		int recruited = WorkerInstanceData.getWorkers(numOfWorkers, ec2);
		if(recruited != numOfWorkers){
			logInfo("Couldn't create required amount of workers. Created " + recruited + " instead of " + numOfWorkers);
			manage();
		}
	}

	private static void fireWorkers(int numOfWorkers) {
		logInfo("Firing " + numOfWorkers + " Workers");
		for(int i = 0; i < numOfWorkers; i++)
			sqs.sendMessage(new SendMessageRequest().withQueueUrl(managerWorkersQueue).withMessageBody("You Are Fired!").withMessageAttributes(fireWorkerMsg));
	}

	private static int getNumOfCurrentWorkers(DescribeInstancesResult result) {
		int numOfCurrentWorkers = 0;
		for (int i = 0; i < result.getReservations().size(); i++) {
        	numOfCurrentWorkers += result.getReservations().get(i).getInstances().size();
        }
		return numOfCurrentWorkers;
	}

	private static int getNumOfRequiredWorkers(){
		int numOfRequired = 0;
		for(String uuid : clientsUUIDToURLLeft.keySet()){//TODO: CHECK WHY requiredWorkers.. is null.. 
			numOfRequired = Integer.max(numOfRequired, requiredWorkersPerTask.get(uuid));
		}
		return numOfRequired;
	}
	
	private static void initEC2Client() throws IOException {
        logger.info("[MANAGER] - Starting to init ec2 client ");
        credentials = new PropertiesCredentials(new FileInputStream(credentialsFilePath));        
        ec2 = new AmazonEC2Client(credentials);
        logger.info("[MANAGER] - EC2 Client initialized.");
    }

    private static void initSQSQueues() {
        logger.info("[MANAGER] - Starting to init queues");
        sqs = new AmazonSQSClient(credentials);
        clientsQueueURL = sqs.getQueueUrl("ClientsQueue").getQueueUrl();
        jobsQueueURL = sqs.createQueue(new CreateQueueRequest("JobQueueQueue")).getQueueUrl();
        jobDoneAckQueueURL = sqs.createQueue(new CreateQueueRequest("jobDoneAckQueue")).getQueueUrl();
        managerWorkersQueue = sqs.createQueue(new CreateQueueRequest("ManagerWorkersQueue")).getQueueUrl();
        logger.info("[MANAGER] - Queues initialized");
    }
    
    private static void logInfo(String msg){
    	logger.info("[MANAGER] - " + msg);
    }
}


