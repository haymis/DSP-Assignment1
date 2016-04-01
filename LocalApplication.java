import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

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
import com.amazonaws.services.sqs.AmazonSQSClient;


public class LocalApplication {

    private static String inputFile;
    private static String outputFile;
    
    private static UUID uuid;
    
    // Number of messages per worker.
    private static int n;
    private static boolean terminate = false;
    private static String managerInstanceId;
    private static AmazonEC2 ec2;
    private static AmazonS3 s3;
    private static String bucketName = "mateli";
    private static AWSCredentials credentials;
    private static AmazonSQSClient sqs;
    		
	public static void main(String[] args){
		
		
		try {
			initEC2();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		runManagerInstance();
		/*uploadToS3();
		sendMessageToSQS();
		waitForResponse();
		handleResponse();
		*/
		
		
	}
	
	private static void initEC2() throws FileNotFoundException, IOException {
		credentials = new PropertiesCredentials(new FileInputStream("C:\\Users\\Haymi\\Documents\\BGU\\DSP\\rootkey.properties"));
        ec2 = new AmazonEC2Client(credentials);        
    }

	/*
	 * Makes sure the Manager is running, if not - run it.
	 */
	private static void runManagerInstance() {
		Filter tagFilter = new Filter("tag:Type", Arrays.asList("Manager"));
        Filter stateFilter = new Filter("instance-state-name", Arrays.asList("stopped", "pending", "running"));
        
        DescribeInstancesResult response = ec2.describeInstances((new DescribeInstancesRequest())
        		.withFilters(tagFilter, stateFilter));

        List<Reservation> managerInstances = response.getReservations();
        Instance managerInstance;
        if (!managerInstances.isEmpty()) { // Manager already exists.            
            managerInstance = getManagerInstance(managerInstances);
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
            runManagerRequest.setUserData(fileToBase64String("C:\\Users\\Haymi\\Documents\\BGU\\DSP\\bootstart.sh"));
            managerInstance = ec2.runInstances(runManagerRequest).getReservation().getInstances().get(0);
            managerInstanceId = managerInstance.getInstanceId();
            CreateTagsRequest createTagsRequest = new CreateTagsRequest()
            		.withResources(managerInstanceId)
                    .withTags(new Tag("Type", "Manager"));
            ec2.createTags(createTagsRequest);
        }
        waitForInstanceState(managerInstance, InstanceState.Running, 1905);

        System.out.println("Manager is running.");
		
	}

	private static void waitForInstanceState(Instance instance,
			InstanceState state, int sleepTime) {
		System.out.println("waiting for instance and state");
		InstanceState curr;
		while((curr = getInstanceState(instance)) != state){
			System.out.println("curr state is " + curr + " sleeping...");
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

	private static Instance getManagerInstance(List<Reservation> managerInstances) {
		return managerInstances.get(0).getInstances().get(0);
	}
	
}
