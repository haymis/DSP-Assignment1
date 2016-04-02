package Tests;
//import java.io.*;
//import java.net.URL;
//import java.net.URLConnection;
//import java.util.*;
//
//import com.amazonaws.AmazonServiceException;
//import com.amazonaws.auth.AWSCredentials;
//import com.amazonaws.auth.PropertiesCredentials;
//import com.amazonaws.services.ec2.AmazonEC2;
//import com.amazonaws.services.ec2.AmazonEC2Client;
//import com.amazonaws.services.ec2.model.*;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3Client;
//import com.amazonaws.services.s3.model.CannedAccessControlList;
//import com.amazonaws.services.s3.model.PutObjectRequest;
//import com.amazonaws.services.sqs.AmazonSQS;
//import com.amazonaws.services.sqs.AmazonSQSClient;
//import com.amazonaws.services.sqs.model.*;
//import org.apache.commons.codec.binary.Base64;
//
//
///**
// * Created by mtoledano on 12/5/15.
// */
//
//
//public class LocalApp {
//
//    private static String path = "/Users/mtoledano/Dropbox/Courses_2016/Distributed Systems/Assignment1/";
//    private static String inputFile;
//    private static String outputFile;
//    private static UUID uuid;
//    private static int n;
//    private static boolean terminate = false;
//    private static String managerId;
//    private static AmazonEC2 ec2;
//    private static AmazonS3 s3;
//    private static String bucketName = "mateli";
//    private static AWSCredentials credentials;
//    private static AmazonSQSClient sqs;
//    private static String jobsQueueURL;
//    private static String mySummaryQueueURL;
//    private static String htmlTemplateStart = "<!DOCTYPE html>"
//                                                + "<html>"
//                                                + "<head>"
//                                                + "<title>Results</title>"
//                                                + "</head>"
//                                                + "<body>";
//
//    private static String htmlTemplateEnd = "</body>"
//                                            + "</html>";
//    private static String infoQueueURL;
//    private static String inputFileName;
//
//
//
//    public static void main (String args[]) throws IOException, InterruptedException {
//
//
//        // Creating a unique ID to the LocalApp
//        uuid = UUID.randomUUID();
//
//        /* Parsing the command line arguments  */
//
//        // Path to url file.
//        inputFile = args[0];
//        // Path to final HTML output file.
//        outputFile = args[1];
//        // Worker Ratio
//        n = Integer.parseInt(args[2]);
//
//        if ((args.length == 4) && (args[3].equals("terminate"))) {
//            terminate = true;
//        }
//
//        /*  Running main program */
//
//        try
//        {
//            initEC2Client();
//
//
//            uploadFilesToS3();
//            createQueues();
//            startManager();
//            waitForManagerToFinish();
//            if (terminate)
//            {
//                sendTerminationMessage();
//            }
//
//        }
//        catch (IOException e) {
//            System.out.println(e.getMessage());
//        }
//
//        cleanUp();
//
//    }
//
//    private static void sendTerminationMessage() {
//
//        Map<String,MessageAttributeValue> attributes = new HashMap<>();
//
//        attributes.put("Terminate", new MessageAttributeValue().withDataType("String").withStringValue("yes"));
//        attributes.put("Manager-ID", new MessageAttributeValue().withDataType("String").withStringValue(managerId));
//        sqs.sendMessage(new SendMessageRequest().withMessageBody("Terminate please.").withQueueUrl(infoQueueURL).withMessageAttributes(attributes));
//
//    }
//
//    private static void waitForManagerToFinish() throws InterruptedException, IOException {
//
//        ReceiveMessageResult result;
//        boolean summaryIsReady = false;
//
//        System.out.println("**** Awaiting summary file ****");
//
//
//
//        // Waiting for summary file on localApp's personal response queue.
//
//        while (!summaryIsReady) {
//            Thread.sleep(1000);
//            result = sqs.receiveMessage(new ReceiveMessageRequest()
//                    .withQueueUrl(String.valueOf(uuid))
//                    .withMaxNumberOfMessages(1)
//                    .withMessageAttributeNames("All")
//            );
//            List<Message> messages = result.getMessages();
//
//            if (!messages.isEmpty()) {
//                // We received final summary.
//                String pathToSummaryFile = messages.get(0).getBody();
//                System.out.println(pathToSummaryFile);
//                downloadFileFromS3(pathToSummaryFile);
//                createOutputHTML();
//                summaryIsReady = true;
//            }
//        }
//
//    }
//
//    private static void cleanUp() {
//
//        sqs.deleteQueue(String.valueOf(uuid));
//    }
//
//    private static void createOutputHTML() throws IOException {
//
//        System.out.println("Start creating the output HTML");
//
//        List<String> HTMLFiles = createGroupsHTMLs();
//        PrintWriter writer;
//        String links = "";
//
//        for (String fileName : HTMLFiles) {
//            links += "<a href='"+fileName+"'>" + fileName+ "</a><br>";
//        }
//
//        writer = new PrintWriter(outputFile + "-" + uuid + ".html", "UTF-8");
//        writer.println(htmlTemplateStart + links + htmlTemplateEnd);
//        writer.close();
//
//        System.out.println("Finished creating the output HTML");
//    }
//
//    private static List<String> createGroupsHTMLs() throws IOException {
//
//        System.out.println("Start creating the groups HTML");
//
//        List<String> groupHTMLFiles = new ArrayList<>();
//
//        System.out.println("=== Creating HTML files ===");
//
//        Map<String, List<String>> imagesMap ;
//
//        imagesMap = summaryFileToMap();
//        PrintWriter writer ;
//        // Create html files
//        List<String> urls;
//
//        for ( String groupName : imagesMap.keySet())
//        {
//
//            System.out.println("Start creating HTML for " + groupName);
//
//            urls = imagesMap.get(groupName);
//            String images = "";
//
//            // Build group html
//            for( String url: urls)
//            {
//                images += "<a href='"+url+"'><img hspace='5' src='"+url+"'></img></a><br>\n";
//            }
//
//            writer = new PrintWriter(groupName + uuid + ".html", "UTF-8");
//            String groupHTML = htmlTemplateStart + images + htmlTemplateEnd;
//            writer.println(groupHTML);
//            writer.close();
//            groupHTMLFiles.add(groupName + uuid + ".html");
//
//            System.out.println("Finished creating HTML for " + groupName);
//        }
//
//        System.out.println("Finished creating the groups HTML");
//
//        return groupHTMLFiles;
//
//    }
//
//    private static Map<String, List<String>> summaryFileToMap() throws IOException {
//
//        System.out.println("=== Mapping all url's ===");
//
//        Map<String, List<String>> ret = new HashMap<>();
//
//        BufferedReader reader = new BufferedReader(new FileReader(path+"summaryFile"));
//        String line ;
//
//        while ( (line=reader.readLine()) != null)
//        {
//            String[] splitLine = line.split(" ");
//            String url = splitLine[0];
//            String group = splitLine[1];
//
//            if(!ret.containsKey(group))
//            {
//
//                List<String> urls = new ArrayList<>();
//                ret.put(group, urls);
//            }
//            ret.get(group).add(url);
//        }
//
//        return ret;
//    }
//
//    private static void downloadFileFromS3(String pathToSummaryFile) throws IOException {
//
//        System.out.println("=== Download summary file from s3 ===");
//
//        URLConnection connection= new URL(pathToSummaryFile).openConnection();
//        InputStream retData = connection.getInputStream();
//        File file = new File(path + "summaryFile");
//        if (!file.exists()) {
//            file.createNewFile();
//        }
//        OutputStream writer = new BufferedOutputStream(new FileOutputStream(file));
//
//        int read;
//
//        while (( read = retData.read()) != -1 )
//        {
//            writer.write(read);
//        }
//
//        writer.flush();
//        writer.close();
//        retData.close();
//
//        System.out.println("=== Finished downloading summary file ===");
//    }
//
//    private static void createQueues() {
//        sqs = new AmazonSQSClient(credentials);
//
//        System.out.println("Creating new SQS queues.");
//
//        Map<String,MessageAttributeValue> attributes = new HashMap<>();
//
//        int numOfURLs = countURLs();
//
//        attributes.put("File-Path", new MessageAttributeValue().withDataType("String").withStringValue("https://mateli.s3.amazonaws.com/image-urls.txt"));
//        attributes.put("UUID", new MessageAttributeValue().withDataType("String").withStringValue(String.valueOf(uuid)));
//        attributes.put("Worker-Ratio", new MessageAttributeValue().withDataType("Number").withStringValue(String.valueOf(n)));
//        attributes.put("Input-File-Name", new MessageAttributeValue().withDataType("String").withStringValue(String.valueOf(inputFileName)));
//        attributes.put("Num-of-URLs", new MessageAttributeValue().withDataType("Number").withStringValue(String.valueOf(numOfURLs)));
//
//        //Creating the Manager-LocalApp Queue
//        CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName("InformationQueue");
//        infoQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();
//
//        System.out.println("InformationQueue created ");
//
//        //Creating the unique LocalApp Queue
//        createQueueRequest = new CreateQueueRequest().withQueueName(String.valueOf(uuid));
//        mySummaryQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();
//
//        System.out.println("Personal localapp queue created.");
//
//        // Creating the Jobs queue
//        Map<String,String> attributes2 = new HashMap<>();
//        attributes2.put("VisibilityTimeout", "160");
//        createQueueRequest = new CreateQueueRequest().withQueueName("JobsQueue").withAttributes(attributes2);
//        jobsQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();
//
//        // Sending the message with the location of the input file to the manager
//        try{
//            sqs.sendMessage(new SendMessageRequest().withQueueUrl(infoQueueURL).withMessageBody("Please process these URLS").withMessageAttributes(attributes));
//        }
//        catch (QueueDoesNotExistException e){
//            System.out.println("Manager is no longer running. Exiting...");
//            System.exit(0);
//        }
//
//
//        System.out.println("SQS queues created.");
//
//    }
//
//    private static int countURLs() {
//
//        int counter = 0;
//        try
//        {
//            File f = new File(inputFile);
//            BufferedReader reader = new BufferedReader(new FileReader(f));
//            while( reader.readLine() != null)
//            {
//                counter++;
//            }
//        }
//        catch (IOException e)
//        {
//            System.out.println("No file found or could not read from file.");
//        }
//        return counter;
//    }
//
//    private static void uploadFilesToS3() {
//
//        s3 = new AmazonS3Client(credentials);
//        System.out.println("AmazonS3Client created.");
//
//        if (!s3.doesBucketExist(bucketName)){
//            s3.createBucket(bucketName);
//        }
//
//        System.out.println("Bucket exists.");
//
//        File f;
//
//        // Upload input ( URLS ) file.
//        f = new File(inputFile);
//        inputFileName = f.getName();
//        PutObjectRequest por = new PutObjectRequest(bucketName, inputFileName, f);
//        por.withCannedAcl(CannedAccessControlList.PublicRead);
//        s3.putObject(por);
//
//
//        // upload worker bootstrap to s3
//        f = new File("/Users/mtoledano/Dropbox/Courses_2016/Distributed Systems/Assignment1/worker_bootstrap.sh");
//
//        por = new PutObjectRequest(bucketName, "worker_bootstrap.sh", f);
//        por.withCannedAcl(CannedAccessControlList.PublicRead);
//        s3.putObject(por);
//
//        System.out.println("Files uploaded.");
//
//
//    }
//
//    private static void startManager() throws InterruptedException, IOException {
//
//        DescribeInstancesRequest request = new DescribeInstancesRequest();
//        List<String> tagValues = new ArrayList<>();
//        tagValues.add("Manager");
//        List stateValues = new ArrayList();
//        stateValues.add("stopped");
//        stateValues.add("running");
//        stateValues.add("pending");
//
//        Filter filter = new Filter("tag:Name", tagValues);
//        Filter filter2 = new Filter("instance-state-name", stateValues);
//        DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter, filter2));
//
//        List<Reservation> reservations = result.getReservations();
//
//        if (!reservations.isEmpty()) {
//            // Entering here means that a manager node exists and need to be started
//            List<Instance> instances = reservations.get(0).getInstances();
//
//            Instance instance = instances.get(0);
//            managerId = instance.getInstanceId();
//            if (!instance.getState().getName().equals("running")) {
//
//                StartInstancesRequest startRequest = new StartInstancesRequest().withInstanceIds(managerId);
//                ec2.startInstances(startRequest);
//
//                // Wait for the instance to be started
//                try {
//                    waitForManager("running");
//                }
//                catch (InterruptedException e) {
//                    System.out.println("Interrupted!, " + e.getMessage());
//                }
//            }
//        }
//        else {
//            // Entering here means that a manager node doesn't exist and need to be created
//            RunInstancesRequest runManagerRequest = new RunInstancesRequest("ami-146e2a7c", 1, 1);
//            runManagerRequest.setInstanceType(InstanceType.T2Micro.toString());
//            // Path to manager bootstrap file.
//            runManagerRequest.setUserData(getBootStrapScript("/Users/mtoledano/Dropbox/Courses_2016/Distributed Systems/Assignment1/manager_bootstrap.sh"));
//            runManagerRequest.setKeyName("ds161");
//            List<Instance> instances = ec2.runInstances(runManagerRequest).getReservation().getInstances();
//            managerId = instances.get(0).getInstanceId();
//            CreateTagsRequest createTagsRequest = new CreateTagsRequest().withResources(managerId)
//                                                                            .withTags(new Tag("Name", "Manager"));
//            ec2.createTags(createTagsRequest);
//            waitForManager("running");
//        }
//
//        System.out.println("Manager is running.");
//    }
//
//
//    private static String getBootStrapScript(String bootStrap) throws IOException
//    {
//        String line,str;
//        ArrayList<String> lines = new ArrayList<>();
//        InputStream fileInputStream = new FileInputStream(bootStrap);
//        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
//        BufferedReader br = new BufferedReader(inputStreamReader);
//
//        while ((line = br.readLine()) != null)
//            lines.add(line);
//
//        br.close();Base64
//        str = new String(.encodeBase64(join(lines, "\n").getBytes()));
//        return str;
//    }
//
//    private  static String join(Collection<String> s, String delimiter) {
//        StringBuilder sBuilder = new StringBuilder();
//        Iterator<String> iterator = s.iterator();
//        while (iterator.hasNext()) {
//            sBuilder.append(iterator.next());
//            if (!iterator.hasNext()) {
//                break;
//            }
//            sBuilder.append(delimiter);
//        }
//        return sBuilder.toString();
//    }
//
//    private static void initEC2Client() throws IOException {
//
//        System.out.println("Beginning EC2 Client initialization.");
//        credentials = new PropertiesCredentials(new FileInputStream("/Users/mtoledano/Documents/access_key.properties"));
//        ec2 = new AmazonEC2Client(credentials);
//        System.out.println("EC2 Client initialized.");
//    }
//
//    private static void waitForManager(String state) throws InterruptedException {
//
//
//        DescribeInstancesRequest stateRequest;
//        DescribeInstancesResult describeResult;
//        List<Reservation> reservations;
//        List<Instance> instances;
//
//        List<String> ids = new ArrayList<>();
//        ids.add(managerId);
//
//        while (true) {
//            stateRequest = new DescribeInstancesRequest();
//            describeResult = ec2.describeInstances(stateRequest.withInstanceIds(ids));
//            reservations = describeResult.getReservations();
//            instances = reservations.get(0).getInstances();
//            if (instances.get(0).getState().getName().equals(state)) {
//                break;
//            }
//            else {
//                Thread.sleep(5000);
//            }
//        }
//
//    }
//
//
//
//}
