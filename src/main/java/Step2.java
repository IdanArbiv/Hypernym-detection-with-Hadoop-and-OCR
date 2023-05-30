import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class Step2 {

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * Input:
         * 1) Key = ** Value = DependencyPath
         * 2) Key = N1N2 \t Value = <DependencyPath Number of occurrences \t DependencyPath Number of occurrences \t ...>
         * Output:
         * 1) Key = ** Value = DependencyPath
         * 2) Key = N1N2 \t Value = <DependencyPath Number of occurrences \t DependencyPath Number of occurrences \t ...>
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] pairAndDependencyPaths = value.toString().split("\t");
            context.write(new Text(pairAndDependencyPaths[0]), new Text(pairAndDependencyPaths[1]));
        }
    }


    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {

        private Map<String, Long> dependencyPathsMap;

        private HashMap<String, Boolean> hypernymMap;
        public static final String BUCKET_NAME = "bucket1638974297772";
        private final String HYPERNYM_LIST = "hypernym.txt";
        private Stemmer stemmer;
        private AmazonS3 s3;

        /**
         * This function is called once before map() is called the first time. It is used
         * to setup any necessary resources.
         *
         * @param context The Hadoop context object
         * @throws IOException If there is an error reading from S3
         */
        public void setup(Context context) throws IOException {
            // Initialize the map that will store the dependency paths
            dependencyPathsMap = new HashMap<>();

            // Initialize an S3 client and set the region to US East (N. Virginia)
            s3 = AmazonS3ClientBuilder.standard().withRegion("us-east-1").build();

            // Download the hypernym list file from S3
            System.out.print("[INFO] Downloading hypernym list file from S3... ");
            S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, HYPERNYM_LIST));
            System.out.println("[INFO] Done.");

            // Initialize a BufferedReader to read the file and a HashMap to store the hypernym information
            BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
//            BufferedReader br = new BufferedReader(new FileReader("/home/spl211/IdeaProjects/DSP_project_3/hypernym.txt"));
            hypernymMap = new HashMap<>();

            // Initialize a stemmer to normalize the words
            stemmer = new Stemmer();

            // Read each line of the file and add the hypernym information to the map
            String line;
            while ((line = br.readLine()) != null) {
                String[] pieces = line.split("\\s");

                // Stem the words
                stemmer.add(pieces[0].toCharArray(), pieces[0].length());
                stemmer.stem();
                pieces[0] = stemmer.toString();
                stemmer.add(pieces[1].toCharArray(), pieces[1].length());
                stemmer.stem();
                pieces[1] = stemmer.toString();

                // Add the hypernym information to the map
                hypernymMap.put(pieces[0] + "$" + pieces[1], pieces[2].equals("True"));
            }

            // Close the BufferedReader
            br.close();
        }

        /**
         * Input:
         * 1) Key = ** Value = DependencyPath
         * 2) Key = N1N2 Value = [DependencyPath # Number of occurrences @ DependencyPath # Number of occurrences @   ......]
         * Output:
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Get the key as a string
            String strKey = key.toString();

            // If the key is "**", this is a list of all possible dependency paths
            if (strKey.equals("**")) {
                // Add each dependency path to the map with a value of 0
                for (Text dependencyPath : values) {
                    dependencyPathsMap.put(dependencyPath.toString(), 0L);
                }
            }
            // If the key is not "**", this is a list of dependency paths and their occurrences for a specific pair of words
            else if(hypernymMap.containsKey(key.toString())){
                // For each dependency path and its occurrences, update the value in the map
                for (Text dependencyPathsAndOccurrences : values) {
                    String[] parts = dependencyPathsAndOccurrences.toString().split("@");  // [DependencyPath # Number of occurrences]
                    for (String part : parts) {
                        String[] depPathAndOcc = part.split("#");  // [DependencyPath, Number of occurrences]
                        String dependencyPath = depPathAndOcc[0];
                        long occurrences = Long.parseLong(depPathAndOcc[1]);
                        dependencyPathsMap.replace(dependencyPath, occurrences);
                    }
                }

                // Convert the map to a feature vector
                long[] featuresVector = parseDepMapToFeaturesVector();

                // Build the output string
                StringBuilder sb = new StringBuilder();
                for (long entry : featuresVector) {
                    sb.append(entry).append(",");
                }
                sb.append(hypernymMap.get(strKey));

                // Write the output to the context
                context.write(key, new Text(sb.toString()));
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            System.out.println("Features vector length: " + dependencyPathsMap.size());
            File numOfFeaturesFile = new File("numOfFeatures.txt");
            BufferedWriter bw = new BufferedWriter(new FileWriter(numOfFeaturesFile));
            bw.write(dependencyPathsMap.size() + "\n");
            bw.close();
            s3 = AmazonS3ClientBuilder.standard().withRegion("us-east-1").build();
            System.out.print("Uploading num of features file to S3... ");
            s3.putObject(new PutObjectRequest("bucket1638974297772", "numOfFeatures.txt", numOfFeaturesFile));
            System.out.println("Done.");
        }

        /**
         * This function converts the dependency paths map into a feature vector.
         *
         * @return The feature vector (long[])
         */
        public long[] parseDepMapToFeaturesVector() {
            // Create an array with the same size as the dependency paths map
            long[] featureVector = new long[dependencyPathsMap.size()];

            // Iterate through the entries of the map and add the values to the feature vector
            int i = 0;
            for (Map.Entry<String, Long> entry : dependencyPathsMap.entrySet()) {
                featureVector[i] = entry.getValue();
                entry.setValue(0L);  // Reset the value of the entry to 0
                i++;
                System.out.println("[DEBUG]: featureVector: " + Arrays.toString(featureVector));
            }

            return featureVector;
        }

    }

    /**
     * Main method for this Map-Reduce step. Processes the noun pairs and their dependency paths into a file which
     * contains the pairs and their features vector. This file would afterwards be processed with PostProcessor.java
     * into an .arff file for use by WEKA.
     *
     * @param args an array of 2 Strings: input path, output path.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step2.class);
        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path("s3://bucket1638974297772/output1"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket1638974297772/output2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}