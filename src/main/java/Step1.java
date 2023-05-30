
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class Step1 {

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
        public static final String DELIMITER_1 = "$";
        public static final String DELIMITER_2 = ":";
        public static final String BUCKET_NAME = "bucket1638974297772";
//        public static final String HYPERNYM_TXT_PATH = "hypernym.txt";
        private Stemmer stemmer;
//        private HashMap<String, Boolean> testSet;

        /**
         * Setup the Mapper node.
         *
         * @param context the Map-Reduce job context.
         */
        @Override
        public void setup(Context context) throws IOException {
//            testSet = new HashMap<>();
            stemmer = new Stemmer();
//            BufferedReader br;
//            AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
//            S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, HYPERNYM_TXT_PATH));
//            br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
//            br = new BufferedReader(new FileReader("/home/spl211/IdeaProjects/DSP_project_3/hypernym.txt"));
//            createTestSetHashMap(br);
        }

        /**
         * Creates a hash map of related words from the given input stream.
         *
         * @param bufferedReader a BufferedReader object containing input data in the format "word1 word2 True/False"
         */
//        private void createTestSetHashMap(BufferedReader bufferedReader) throws IOException {
//            String line;
//            while ((line = bufferedReader.readLine()) != null) {
//                String[] pieces = line.split("\\s");
//                stemmer.add(pieces[0].toCharArray(), pieces[0].length());
//                stemmer.stem();
//                pieces[0] = stemmer.toString();
//                stemmer.add(pieces[1].toCharArray(), pieces[1].length());
//                stemmer.stem();
//                pieces[1] = stemmer.toString();
//                testSet.put(pieces[0] + DELIMITER_1 + pieces[1], pieces[2].equals("True"));
//            }
//        }

        /**
         * Input:
         * key - lineId value - head_word \t syntactic-ngram \t total_count \t counts_by_year
         * syntactic-ngram format : word/pos-tag/dep-label/head-index
         * <p>
         * Output:
         * key - <N1$N2>  value - Dependency_path_1 (NN:VB:IT:NN)
         * key - Dependency_path_1 (NN:VB:IT:NN)  value - <N1$N2>
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String inputLine = value.toString().split("\t")[1];
            System.out.println("[DEBUG]: inputLine: " + inputLine);
            String[] ngram_parts = inputLine.split(" ");
            System.out.println("[DEBUG]: ngram_parts: " + Arrays.toString(ngram_parts));
            Node[] nodes = createNodesFromNgramParts(ngram_parts);
            if(nodes == null){
                return;
            }
            Node root = constructParsedTree(nodes);
            searchDependencyPath(root, "", root, context);
        }

        /**
         * Transforms a biarc into a an array of Nodes
         *
         * @param ngramParts an array of Strings, each index being a biarc
         * @return an array of Nodes, each one containing a biarc in an accessible container.
         */
        private Node[] createNodesFromNgramParts(String[] ngramParts) {
            Node[] partsAsNodes = new Node[ngramParts.length];
            for (int i = 0; i < ngramParts.length; i++) {
                String[] ngramEntryComponents = ngramParts[i].split("/");
                //Delete non-numeric characters
                final String FIND_NON_NUMERIC_CHARS_REGEX = "[^a-zA-Z ]+";
                System.out.println("[DEBUG]: ngramEntryComponents: " + Arrays.toString(ngramEntryComponents));
                ngramEntryComponents[0] = ngramEntryComponents[0].replaceAll(FIND_NON_NUMERIC_CHARS_REGEX, "");
                ngramEntryComponents[1] = ngramEntryComponents[1].replaceAll(FIND_NON_NUMERIC_CHARS_REGEX, "");
                if (ngramEntryComponents.length != 4 || ngramEntryComponents[0].replaceAll(FIND_NON_NUMERIC_CHARS_REGEX, "").equals("") || ngramEntryComponents[1].replaceAll(FIND_NON_NUMERIC_CHARS_REGEX, "").equals(""))
                    return null;
                partsAsNodes[i] = new Node(ngramEntryComponents, stemmer);
            }
            return partsAsNodes;
        }

        /**
         * Transforms an array of Nodes into a tree, which represents the dependencies defined in the original biarc.
         *
         * @param nodes an array of Nodes.
         * @return the root of the tree.
         */
        private Node constructParsedTree(Node[] nodes) {
            int rootIndex = 0;
            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i].getFather() > 0)
                    nodes[nodes[i].getFather() - 1].addChild(nodes[i]);
                else
                    rootIndex = i;
            }
            return nodes[rootIndex];
        }

        /**
         * A recursive method to find all shortest paths between nouns in a syntactic tree.
         *
         * @param node           a node that is inquired as to being a start or end of a shortest path.
         * @param dependencyPath an accumulator that holds the shortest path so far as a String.
         * @param pathStart      the first node of the noun pair.
         * @param context        the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
        private void searchDependencyPath(Node node, String dependencyPath, Node pathStart, Context context) throws IOException, InterruptedException {
            // Found the first Noun
            if (node.isNoun() && dependencyPath.isEmpty())
                for (Node child : node.getChildren())
                    searchDependencyPath(child, node.getDepencdencyPathComponent(), node, context);
                // Found the second Noun
            else if (node.isNoun()) {
                Text fullDependencyPath = new Text(dependencyPath + DELIMITER_2 + node.getDepencdencyPathComponent());
                Text nounPair = new Text(pathStart.getStemmedWord() + DELIMITER_1 + node.getStemmedWord());
                context.write(fullDependencyPath, nounPair);
                context.write(nounPair, new Text(fullDependencyPath + DELIMITER_1 + "1"));

                //Keep search for another noun
                searchDependencyPath(node, "", node, context);
            } else {
                for (Node child : node.getChildren())
                    // If dependencyPath is empty it means we did not found any noun yet, so we will keep the dependency path empty
                    searchDependencyPath(child, dependencyPath.isEmpty() ? dependencyPath : dependencyPath + DELIMITER_2 + node.getDepencdencyPathComponent(), pathStart, context);
            }
        }
    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        public static final String DELIMITER_1 = "$";

        /**
         * Reduces the input data. If the key is a noun pair, counts the occurrences of each dependency path in the values.
         * If the key is a dependency path, writes the key and values to the Reducer context.
         *
         * @param key     the key
         * @param values  the values
         * @param context the Reducer context
         * @throws IOException          if an I/O error occurs
         * @throws InterruptedException if the operation is interrupted
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            if (key.toString().split("\\$").length > 1) {
                HashMap<String, Integer> countMap = new HashMap<>();
                // Iterate over the values
                for (Text value : values) {
                    String[] dependencyPathProp = value.toString().split("\\$");
                    String dependencyPathString = dependencyPathProp[0];
                    String dependencyPathOccurrences = dependencyPathProp[1];
                    if (!countMap.containsKey(dependencyPathString))
                        countMap.put(dependencyPathString, Integer.parseInt(dependencyPathOccurrences));
                    else {
                        int count = countMap.get(dependencyPathString);
                        countMap.put(dependencyPathString, count + Integer.parseInt(dependencyPathOccurrences));
                    }
                }
                for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
                    context.write(key, new Text(entry.getKey() + DELIMITER_1 + entry.getValue()));
                }
            }
            // If the key is a dependency path
            else {
                // Iterate over the values and write them to the Reducer context
                for (Text value : values) context.write(key, value);
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        public static final String DELIMITER_1 = "#";
        public static final String DELIMITER_2 = "@";
        private int DPmin;

        /**
         * Setup up a Reducer node.
         *
         * @param context the Map-Reduce job context.
         */
        @Override
        public void setup(Context context) {
            DPmin = Integer.parseInt(context.getConfiguration().get("DPMIN"));
            System.out.println("Reducer: DPmin is set to " + DPmin);
        }

        /**
         * Input:
         * key - <N1 N2>  value - [Dependency_path_1 1, Dependency_path_1 1 , Dependency_path_2 1, ... ]
         * key - Dependency_path_1 (NN:VB:IT:NN)  value - [<N1 N2> , <N1,N2> , <N2,N3>]
         * <p>
         * Output:
         * key - **  value - Dependency_path_1 (If D >= DPmin)
         * key - <N1 N2>  value - < Dependency_path_1: 3, Dependency_path_2 : 7  , ... >
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //  key - <N1 N2>
            if (key.toString().split("\\$").length > 1) {
                countDependencyPaths(key, values, context);
            }
            // key - Dependency_path
            else {
                checkDPminCondition(key, values, context);
            }

        }

        /**
         * Determines if the number of unique noun pairs in the given values meets the minimum requirement (DPmin).
         * If the requirement is met, the key is written to the Reducer context.
         *
         * @param key     the key
         * @param values  the values
         * @param context the Reducer context
         * @throws IOException          if an I/O error occurs
         * @throws InterruptedException if the operation is interrupted
         */
        private void checkDPminCondition(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // Initialize a variable to store the last noun pair encountered
            String lastNounPairs = null;
            // Initialize a counter for the number of unique noun pairs
            int uniqueNounPairs = 0;
            // Iterate over the values
            for (Text nounPair : values) {
                // If the current noun pair is different from the last one encountered
                if (!nounPair.toString().equals(lastNounPairs)) {
                    // Update the last noun pair encountered
                    lastNounPairs = nounPair.toString();
                    // Increment the counter
                    uniqueNounPairs++;
                }
                // If the number of unique noun pairs meets the minimum requirement, exit the loop
                if (uniqueNounPairs == DPmin) break;
            }
            // If the number of unique noun pairs meets the minimum requirement
            if (uniqueNounPairs >= DPmin) {
                // Write the key to the Reducer context
                context.write(new Text("**"), key);
            }
        }

        /**
         * Counts the occurrences of each dependency path in the given values.
         *
         * @param key     the key
         * @param values  the values
         * @param context the Reducer context
         * @throws IOException          if an I/O error occurs
         * @throws InterruptedException if the operation is interrupted
         */
        private void countDependencyPaths(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //Initialize countMap to count how many unique dependency paths we have
            HashMap<String, Integer> countMap = new HashMap<>();
            // Iterate over the values
            for (Text value : values) {
                String[] dependencyPathProp = value.toString().split("\\$");
                String dependencyPathString = dependencyPathProp[0];
                String dependencyPathOccurrences = dependencyPathProp[1];
                if (!countMap.containsKey(dependencyPathString))
                    countMap.put(dependencyPathString, Integer.parseInt(dependencyPathOccurrences));
                else {
                    int count = countMap.get(dependencyPathString);
                    countMap.put(dependencyPathString, count + Integer.parseInt(dependencyPathOccurrences));
                }
            }
            String res = countMap.entrySet().stream().map(entry -> entry.getKey() + DELIMITER_1 + entry.getValue() + DELIMITER_2).collect(Collectors.joining());
            // Write the results to the Reducer context
            context.write(key, new Text(res));
        }
    }

    /**
     * Main method for this Map-Reduce step. Extracts all dependency paths between pairs of nouns that are to be treated
     * as features in a features vector, in preperation for post-processing in WEKA.
     *
     * @param args an array of 3 Strings: input path, output path, DPmin.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        conf.set("DPMIN", "3");
        Job job = Job.getInstance(conf, "Step1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(Mapper1.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://bucket1638974297772/input/"));
//        FileInputFormat.addInputPath(job, new Path("s3://bucket1638974297772/inputIdan.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket1638974297772/output1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}