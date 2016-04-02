package Manager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;

public class Reducer implements Runnable{
	
	private String jobDoneAckQueue;
	private AmazonSQSClient sqs;
	private ExecutorService reducerExecutor;
	private ConcurrentHashMap<String, Integer> clientsUUIDToURLLeft;
	private AtomicBoolean shouldTerminate;
	private Logger logger;
	private AWSCredentials credentials;
	
	public Reducer(String jobDoneAckQueue, ConcurrentHashMap<String, Integer> clientsUUIDToURLLeft,
			AmazonSQSClient sqs, AtomicBoolean shouldTerminate, int numOfThreads, Logger logger, AWSCredentials credentials) {
		this.jobDoneAckQueue = jobDoneAckQueue;
		this.clientsUUIDToURLLeft = clientsUUIDToURLLeft;
		this.sqs = sqs;
		this.shouldTerminate = shouldTerminate;
		this.reducerExecutor = Executors.newFixedThreadPool(numOfThreads);
		this.logger = logger;
		this.credentials = credentials;
		logger.info("Reducer Started");
	}

	public void run() {
		
	}

}
