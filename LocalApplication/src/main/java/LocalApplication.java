import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.ec2.model.InstanceType;

public class LocalApplication {
    private static String THREE_GRAM_HEBREW = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    private static String BUCKET_NAME = "bucketqoghawn0ehuw2njlvyexsmxt5dczxfwc";
    private static String KEY_PAIR;


    public static void main(String[] args) {
//        AWSCredentials credentials_profile = null;
//        try {
//            credentials_profile = new ProfileCredentialsProvider("default").getCredentials();
//        } catch (Exception e) {
//            throw new AmazonClientException(
//                    "Cannot load the credentials from the credential profiles file. " +
//                            "Please make sure that your credentials file is at the correct " +
//                            "location (/home/<user>/.aws/credentials), and is in valid format.",
//                    e);
//        }

//        AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials_profile);
//        Region usEast1= Region.getRegion(Regions.US_EAST_1);
//        emr.setRegion(usEast1);

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClient.builder().standard().withRegion(Regions.US_EAST_1).build();

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://" + BUCKET_NAME + "/Extractor.jar") // TODO: Change the jar name according to our name
                .withMainClass("LocalApplication.jar") // TODO: Change to the extract colletion
                .withArgs("s3n://" + BUCKET_NAME + "/input.txt", "s3n://" + BUCKET_NAME + "/output/"); //TODO: change to output requested name

        StepConfig stepConfig = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("Ass1") //TODO: Maybe need to change
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("jobname")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3n://" + BUCKET_NAME + "/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.16.0");
//                .withAmiVersion("ami-0878fb723a9a1c5db");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
