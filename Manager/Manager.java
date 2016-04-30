package Manager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import static Util.Const.bucketName;

public class Manager {
    private static UUID uuid = UUID.randomUUID();
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
    private static AtomicBoolean reducerDone;
    private static AtomicBoolean mapperDone;
    //	private static String myInstanceID;
    private static String jobDoneAckQueueURL;
    private static String managerWorkersQueue;
    private static String clientsQueueURL;
    private static String jobsQueueURL;
    private static String workersStatsQueue;
    private static int mapperNumOfThreads = 3;
    private static int reducerNumOfThreads = 3;
    private static String logFilePath = "/tmp/log/managerLog.txt"; //"/tmp/managerLog.log";
    private static String statsFilePath = "/tmp/logWorkersStats.txt";
    private static String logFileName = "/tmp/log/managerLog-" + uuid.toString() + ".txt";
    private static String statsFileName = "/tmp/log/WorkersStats-" + uuid.toString() + ".txt";
    private static FileHandler logFileHandler;
    static Map<String, MessageAttributeValue> fireWorkerMsg;

    public static void main(String[] args) {
        logger.info("===== Manager.Manager started =====");
        shouldTerminate = new AtomicBoolean(false);
        reducerDone = new AtomicBoolean(false);
        mapperDone = new AtomicBoolean(false);
        try {
            logFileHandler = new FileHandler(logFilePath);
            logger.addHandler(logFileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            logFileHandler.setFormatter(formatter);
            logger.info("===== Manager.Manager started =====");
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
                logger, credentials, talkToTheBossLock, mapperDone);
        Runnable reducer = new Reducer(jobDoneAckQueueURL, clientsUUIDToURLLeft, sqs,
                shouldTerminate, reducerNumOfThreads, logger, credentials, talkToTheBossLock,
                requiredWorkersPerTask, mapperDone, reducerDone);

        Thread mapperThread = new Thread(mapper);
        Thread reducerThread = new Thread(reducer);

        mapperThread.start();
        reducerThread.start();

        while(!reducerDone.get() || !mapperDone.get()){
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
        terminate();
    }

    private static void terminate() {
        logInfo("Starting termination...");
        int numOfRequiredWorkers = getNumOfRequiredWorkers();
        if(numOfRequiredWorkers != 0) {
            logInfo("Error in termination - required " + numOfRequiredWorkers + " workers. \n"
                    + "Terminating them now - check that all workers are really terminated!");
            fireWorkers(numOfRequiredWorkers);
        }
        summarizeStats();
        uploadLogAndStats();
    }

    private static void summarizeStats() {
        long totalRuntime = 0;
        double SuccessfulRuntimePerMessage = 0;
        double FailedRuntimePerMessage = 0;
        int successfulTweets = 0;
        int failedTweets = 0;
        int numOfWorkers = 0;
        ReceiveMessageResult result ;
        List<Message> messages ;
        do
        {
            result = sqs.receiveMessage(
                    new ReceiveMessageRequest()
                            .withQueueUrl(workersStatsQueue)
                            .withMessageAttributeNames("All")
            );

            messages = result.getMessages();
            logInfo("Stats messages arrived...");

            for (Message message: messages) {
                String receipt = message.getReceiptHandle();
                Map<String, MessageAttributeValue> statsAttributes = message.getMessageAttributes();
                totalRuntime += Long.parseLong(statsAttributes.get("Runtime").getStringValue());
                successfulTweets += Integer.parseInt(statsAttributes.get("SuccessfulTweets").getStringValue());
                failedTweets += Integer.parseInt(statsAttributes.get("FailedTweets").getStringValue());
                SuccessfulRuntimePerMessage = getNewAverage(numOfWorkers, SuccessfulRuntimePerMessage,
                        statsAttributes.get("SuccessfulRuntimePerMessage").getStringValue());
                FailedRuntimePerMessage = getNewAverage(numOfWorkers, FailedRuntimePerMessage,
                        statsAttributes.get("FailedRuntimePerMessage").getStringValue());
                numOfWorkers++;
                sqs.deleteMessage(new DeleteMessageRequest(workersStatsQueue, receipt));
            }
        } while (!messages.isEmpty());

        logInfo("Done Calculating Stats");
        double avgTotalRuntime = totalRuntime / numOfWorkers;
        writeStatsToFile(avgTotalRuntime, successfulTweets, failedTweets, FailedRuntimePerMessage, SuccessfulRuntimePerMessage);
    }

    private static void writeStatsToFile(double avgTotalRuntime, int successfulTweets,
                                         int failedTweets, double FailedRuntimePerMessage, double SuccessfulRuntimePerMessage) {
        Charset charset = Charset.forName("US-ASCII");
        String stats = "WORKERS STATS:\n--------------\n\n"
                + "Average Runtime Of A Worker - " + avgTotalRuntime + "\n"
                + "Total Successful Tweets - " + successfulTweets + "\n"
                + "Total Failed Tweets - " + failedTweets + "\n"
                + "Average Failed Runtime Per Message - " + FailedRuntimePerMessage + "\n"
                + "Average Successful Runtime Per Message - " + SuccessfulRuntimePerMessage + "\n";


        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(statsFilePath), charset)) {
            writer.write(stats, 0, stats.length());
        } catch (IOException e) {
            logInfo("Error while writin stats - " + e.getMessage());
        }
    }

    private static double getNewAverage(int counter, double currentAvg, String currentAvgRuntimeString) {
        double currentAvgRuntime = Double.parseDouble(currentAvgRuntimeString);
        if(counter == 0)
            return currentAvgRuntime;
        return ((currentAvg * counter) + currentAvgRuntime) / (counter + 1);
    }

    private static void uploadLogAndStats() {
        logInfo("Uploading file to S3");
        AmazonS3 s3 = new AmazonS3Client(credentials);
        logInfo("Created Amazon S3 Client");
        if (!s3.doesBucketExist(bucketName)){
            s3.createBucket(bucketName);
            logInfo("Created bucket.");
        }
        logFileHandler.close();

        // Upload log file
        File f = new File(logFilePath);
        PutObjectRequest por = new PutObjectRequest(bucketName, logFileName, f);
        por.withCannedAcl(CannedAccessControlList.PublicRead);
        s3.putObject(por);

        // Upload stats file
        f = new File(statsFilePath);
        por = new PutObjectRequest(bucketName, statsFileName, f);
        por.withCannedAcl(CannedAccessControlList.PublicRead);
        s3.putObject(por);
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
        for(String uuid : requiredWorkersPerTask.keySet()){//TODO: CHECK WHY requiredWorkers.. is null..
            numOfRequired = Math.max(numOfRequired, requiredWorkersPerTask.get(uuid));
        }
        logInfo("getNumOfRequiredWorkers returned: " + numOfRequired+" and requiredWorkersPerTask is: \n" + requiredWorkersPerTask.toString());
        return numOfRequired;
    }

    private static void initEC2Client() throws IOException {
        logger.info("[MANAGER] - Starting to init ec2 client ");
        credentials = new PropertiesCredentials(new FileInputStream(Util.Const.credentialsFilePath));
        ec2 = new AmazonEC2Client(credentials);
        logger.info("[MANAGER] - EC2 Client initialized.");
    }

    private static void initSQSQueues() {
        logger.info("[MANAGER] - Starting to init queues");
        sqs = new AmazonSQSClient(credentials);
        if (sqs == null) {
            System.out.println("Error initializing sqs. quitting.");
            System.exit(1);
        }
        clientsQueueURL = sqs.getQueueUrl("ClientsQueue").getQueueUrl();
        jobsQueueURL = sqs.createQueue(new CreateQueueRequest("JobsQueue")).getQueueUrl();
        jobDoneAckQueueURL = sqs.createQueue(new CreateQueueRequest("jobDoneAckQueue")).getQueueUrl();
        managerWorkersQueue = sqs.createQueue(new CreateQueueRequest("ManagerWorkersQueue")).getQueueUrl();
        workersStatsQueue = sqs.createQueue(new CreateQueueRequest("WorkersStatsQueue")).getQueueUrl();
        logger.info("[MANAGER] - Queues initialized");
    }

    private static void logInfo(String msg){
        logger.info("[MANAGER] - " + msg);
    }
}