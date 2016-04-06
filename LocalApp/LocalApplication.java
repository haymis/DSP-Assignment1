package ass1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.aspectj.weaver.patterns.PatternParser;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class LocalApplication {

    private static String tweetsFilePath;
    private static String tweetsHtmlPath;
    private static String inputFileName;
    
    private static UUID uuid;
    
    // Number of messages per worker.
    private static int WorkerRatio;
    private static boolean terminate = false;
    private static String managerInstanceId;
    private static AmazonEC2 ec2;
    private static AmazonS3 s3;
    private static String bucketName = "hayminirhodadi";
    private static AWSCredentials credentials;
    private static AmazonSQSClient sqs;
	private static String infoQueueURL;
	private static String mySummaryQueueURL;
	private static String jobsQueueURL;
	private static String s3Path = "https://%s.s3.amazonaws.com/%s";
	private static long sleepTime = 20 * 1000;
	
	public static void main(String[] args){
		WorkerRatio = 100;
		tweetsFilePath = "C:\\Users\\Haymi\\Documents\\BGU\\DSP\\tweetLinks1.txt";
		tweetsHtmlPath = "C:\\Users\\Haymi\\Documents\\BGU\\DSP\\ParsedTweets.html";
		//tweetsHtmlPath = args[1];
		uuid = UUID.randomUUID();
		try {
			initEC2();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		runManagerInstance();
		uploadToS3();
		sendMessageToSQS();
		Message response = waitForResponse();
		handleResponse(response);
	}
	
	private static void handleResponse(Message response) {
		String bucket = response.getMessageAttributes().get("BucketName").getStringValue();
		String filename = response.getMessageAttributes().get("OutputFilename").getStringValue();
		S3Object inputFile = null;
		try {
			inputFile = downloadInputFileFromS3(bucket, filename);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (inputFile == null){
			System.out.println("Error getting result file from S3 (" + filename + ").");
			return;
		}
		try {
			HTMLFileCreator.CreateHTMLFromResponse(inputFile, tweetsHtmlPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static S3Object downloadInputFileFromS3(String bucketName, String inputFilePath) throws IOException {
		System.out.println("Downloading input file");
		AmazonS3 s3 = new AmazonS3Client(credentials);
        S3Object inputFile = s3.getObject(new GetObjectRequest(bucketName, inputFilePath));
        return inputFile;
    }
	
	private static Message waitForResponse(){
		ReceiveMessageResult result;
        List<Message> messages;
        
	    do
        {
            result = sqs.receiveMessage(
            			new ReceiveMessageRequest()
            			.withQueueUrl("Request-" + uuid.toString())
            			.withMessageAttributeNames("All")
            			.withMaxNumberOfMessages(1)
            		);
            messages = result.getMessages();	            
            
            if (messages.isEmpty()) {
            	System.out.println("Waiting for my response, sleeping "+ (sleepTime / 1000) +" second");
	            try {
	                Thread.sleep(sleepTime );
	            } catch (InterruptedException e) {
	                e.printStackTrace();
	            }
            }

        } while (messages.isEmpty());
		System.out.println("Got Response!");
	    return messages.get(0);
	}
	
    private static void sendMessageToSQS() {
        sqs = new AmazonSQSClient(credentials);
        
        
        
        System.out.println("Creating new SQS queues.");

        Map<String,MessageAttributeValue> attributes = new HashMap<String,MessageAttributeValue>();

        attributes.put("BucketName", new MessageAttributeValue().withDataType("String").withStringValue(bucketName));
        attributes.put("InputFilename", new MessageAttributeValue().withDataType("String").withStringValue(inputFileName));
        attributes.put("UUID", new MessageAttributeValue().withDataType("String").withStringValue(String.valueOf(uuid)));
        attributes.put("WorkerPerMessage", new MessageAttributeValue().withDataType("Number").withStringValue(String.valueOf(WorkerRatio)));
        attributes.put("NumOfURLs", new MessageAttributeValue().withDataType("Number").withStringValue(String.valueOf(tweetsAmount())));

        //Creating the Manager-LocalApp Queue
        CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName("ClientsQueue");
        infoQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();

        System.out.println("ClientsQueue created ");

        //Creating the unique LocalApp Queue
        createQueueRequest = new CreateQueueRequest().withQueueName("Request-" + uuid.toString());
        mySummaryQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();

        System.out.println("Personal localapp queue created.");

//        // Creating the Jobs queue
//        Map<String,String> attributes2 = new HashMap<String,String>();
//        attributes2.put("VisibilityTimeout", "160");
//        createQueueRequest = new CreateQueueRequest().withQueueName("JobsQueue").withAttributes(attributes2);
//        jobsQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();

        // Sending the message with the location of the input file to the manager
        try{
            sqs.sendMessage(new SendMessageRequest().withQueueUrl(infoQueueURL).withMessageBody("Please process these URLS").withMessageAttributes(attributes));
        }
        catch (QueueDoesNotExistException e){
            System.out.println("Manager is no longer running. Exiting...");
            System.exit(0);
        }


        System.out.println("SQS queues created.");

    }
	
	private static int tweetsAmount() {
		int result = 0;
	    try {
	    	BufferedReader br = new BufferedReader(new FileReader(tweetsFilePath));
			while ( br.readLine() != null)
		    	result++;
		    br.close();
		}
	    catch (Exception e){ 
			e.printStackTrace();
	    }
		return result;
	}

	private static void uploadToS3() {
		
		s3 = new AmazonS3Client(credentials);
        System.out.println("AmazonS3Client created.");
        if (!s3.doesBucketExist(bucketName))
            s3.createBucket(bucketName);
        System.out.println("Bucket exists.");
        File f;

        // Upload tweets file.
        f = new File(tweetsFilePath);
        inputFileName = uuid.toString() +"-"+ f.getName();
        PutObjectRequest por = new PutObjectRequest(bucketName, inputFileName, f);
        por.withCannedAcl(CannedAccessControlList.PublicRead);
        s3.putObject(por);
        System.out.println(inputFileName + " uploaded.");

        // upload worker bootstrap to s3
        // f = new File("/Users/mtoledano/Dropbox/Courses_2016/Distributed Systems/Assignment1/worker_bootstrap.sh");

        //        por = new PutObjectRequest(bucketName, "worker_bootstrap.sh", f);
        //        por.withCannedAcl(CannedAccessControlList.PublicRead);
        //        s3.putObject(por);
        		
	}

	private static void initEC2() throws FileNotFoundException, IOException {
		credentials = new PropertiesCredentials(new FileInputStream("C:\\Users\\Haymi\\Documents\\BGU\\DSP\\rootkey.properties"));
        ec2 = new AmazonEC2Client(credentials);        
    }

	/*
	 * Makes sure the Manager is running, if not - run it.
	 */
	private static void runManagerInstance() {

        Instance managerInstance = getManagerInstance();
        if (managerInstance != null) { // Manager already exists.            
            managerInstanceId = managerInstance.getInstanceId();
            InstanceState managerState =  getInstanceState(managerInstance);
            if (managerState != InstanceState.Running) {
            	StartInstancesRequest startRequest = new StartInstancesRequest().withInstanceIds(managerInstanceId);
                ec2.startInstances(startRequest);
            }
        }
        else { // Manager doesn't exist.
            RunInstancesRequest runManagerRequest = new RunInstancesRequest("ami-b66ed3de", 1, 1);
            runManagerRequest.setInstanceType(InstanceType.T2Micro.toString());
            //runManagerRequest.setUserData(fileToBase64String("C:\\Users\\Haymi\\Documents\\BGU\\DSP\\bootstart.sh"));
            runManagerRequest.setKeyName("nirhaymi");
            managerInstance = ec2.runInstances(runManagerRequest).getReservation().getInstances().get(0);
            managerInstanceId = managerInstance.getInstanceId();
            CreateTagsRequest createTagsRequest = new CreateTagsRequest()
            		.withResources(managerInstanceId)
                    .withTags(new Tag("Type", "Manager"));
            ec2.createTags(createTagsRequest);
        }
        waitForManagerInstanceState(InstanceState.Running, 5000);

        System.out.println("Manager is running.");
		
	}

	private static Instance getManagerInstance(){
		Filter tagFilter = new Filter("tag:Type", Arrays.asList("Manager"));
        Filter stateFilter = new Filter("instance-state-name", Arrays.asList("stopped", "pending", "running"));
        
        DescribeInstancesResult response = ec2.describeInstances((new DescribeInstancesRequest())
        		.withFilters(tagFilter, stateFilter));

        List<Reservation> managerInstances = response.getReservations();
        if(managerInstances.isEmpty())
        	return null;
        return managerInstances.get(0).getInstances().get(0);
	}
	
	private static void waitForManagerInstanceState(InstanceState state, int sleepTime) {
		System.out.println("waiting for instance and state");
		Instance manager;
		InstanceState curr;
		while(true){
			manager = getManagerInstance();
			if (manager != null) {
				curr = getInstanceState(manager);
				if (curr == state)
					break;
				System.out.println("curr state is " + curr + " sleeping...");
			} else {
				System.out.println("manager is null, sleeping...");
			}
			
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static String fileToBase64String(String path) {
		StringBuilder sb = new StringBuilder();
		String line = null;
	    try {
	    	BufferedReader br = new BufferedReader(new FileReader(path));
			
		    while ( (line= br.readLine()) != null) {
		        sb.append(line);
		        sb.append(System.lineSeparator());
		    }
		    br.close();
		}
	    catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	    return new String(Base64.encodeBase64(sb.toString().getBytes()));
	}

	private static InstanceState getInstanceState(Instance managerInstance) {
		Integer response = managerInstance.getState().getCode();
		Integer key = new Integer(managerInstance.getState().getCode().intValue() & 0xff);
		System.out.println("Response is " + response + " and key is " + key);
		System.out.println("Returning " + InstanceStatesDictionary.instanceStatesDictionary.get(key));
		return InstanceStatesDictionary.instanceStatesDictionary.get(key);
	}
}

