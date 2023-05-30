import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PostProcessor {

    private static final String PREFIX1 = "@RELATION nounpair\n\n";
    private static final String PREFIX2 = "@RELATION nounpair\n\n@ATTRIBUTE nounPair STRING\n";
    private static final String POSTFIX = "@ATTRIBUTE ans {true, false}\n\n@DATA\n";
    public static final String BUCKET_NAME = "bucket1638974297772";

    public static void main(String[] args) throws IOException {
        // Read M-R output from S3 and NumOfFeatures from S3
        BufferedReader brMROutput = getS3BufferedReader(BUCKET_NAME, "output2/part-r-00000");
        BufferedReader brNumOfFeatures = getS3BufferedReader(BUCKET_NAME, "numOfFeatures.txt");
//        BufferedReader brMROutput = new BufferedReader(new FileReader("output2/part-r-00000"));

        // Create local directories and file writers
        createLocalDirectories();
        BufferedWriter bwClassifierInput1 = new BufferedWriter(new FileWriter(new File("classifier_input/processed_single_corpus.arff")));
        BufferedWriter bwClassifierInput2 = new BufferedWriter(new FileWriter(new File("classifier_input/processed_single_corpus_with_words.arff")));
        BufferedWriter bwcopy = new BufferedWriter(new FileWriter("output/part-r-00000"));

        // Get vector length from NumOfFeatures file
        int vectorLength = Integer.parseInt(brNumOfFeatures.readLine());
//        int vectorLength = 2;

        // Write prefixes and attribute declarations to arff files
        writeARFFHeader(bwClassifierInput1, vectorLength, PREFIX1);
        writeARFFHeader(bwClassifierInput2, vectorLength, PREFIX2);

        // Read and process each line of M-R output
        String line;
        while ((line = brMROutput.readLine()) != null) {
            // Copy M-R output to local file
            bwcopy.write(line + "\n");

            // Write data to arff files
            bwClassifierInput1.write(line.substring(line.indexOf("\t") + 1) + "\n");
            bwClassifierInput2.write(line + "\n");
        }

        // Close file readers and writers
        brMROutput.close();
        //brNumOfFeatures.close();
        bwClassifierInput1.close();
        bwClassifierInput2.close();
        bwcopy.close();
    }

    /**
     * Gets a BufferedReader for an object in S3.
     *
     * @param bucket the name of the S3 bucket
     * @param key the key of the object
     * @return a BufferedReader for the object
     * @throws IOException if there is an error reading from the object
     */
    public static BufferedReader getS3BufferedReader(String bucket, String key) throws IOException {
        S3Object object = getS3Object(bucket, key);
        return new BufferedReader(new InputStreamReader(object.getObjectContent()));
    }

    /**
     * Writes the header for an ARFF file.
     *
     * @param bw the BufferedWriter to write to
     * @param vectorLength the number of attributes in the ARFF file
     * @throws IOException if there is an error writing to the BufferedWriter
     */
    public static void writeARFFHeader(BufferedWriter bw, int vectorLength, String prefix) throws IOException {
        bw.write(prefix); // or PREFIX2 if writing the file with N1 N2
        for (int i = 0; i < vectorLength; i++) {
            bw.write("@ATTRIBUTE p" + i + " REAL\n");
        }
        bw.write(POSTFIX);
    }

    /**
     * Creates the local directories "classifier_input" and "output" if they do not already exist.
     *
     * @throws IOException if there is an error creating the directories
     */
    public static void createLocalDirectories() throws IOException {
        Path path = Paths.get("classifier_input");
        if (!Files.exists(path)) {
            Files.createDirectory(path);
        }
        path = Paths.get("output");
        if (!Files.exists(path)) {
            Files.createDirectory(path);
        }
    }

    /**
     * Gets an object from an S3 bucket.
     *
     * @param bucketName the name of the S3 bucket
     * @param key the key of the object
     * @return the S3 object
     */
    private static S3Object getS3Object(String bucketName, String key) {
        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        AWSCredentials credentials = credentialsProvider.getCredentials();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_EAST_1)
                .build();

        // Download the file
        return s3Client.getObject(new GetObjectRequest(bucketName, key));
    }
}