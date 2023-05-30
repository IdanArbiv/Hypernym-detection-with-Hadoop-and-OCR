import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.lazy.IBk;
import weka.core.Debug;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;
import weka.core.converters.ConverterUtils.DataSource;

/**
 * Class that contains methods for training and testing a J48 decision tree
 * classifier on a dataset and writing the classified instances to a new file.
 */
public class ClassifierTester {
    private static final String HEADER1 = "classifier_input";
    private static final String HEADER2 = "classifier_output";

    public static void main(String[] args) throws Exception {

        HashMap<String, String> tpSet = new HashMap<>(10); // true positives
        HashMap<String, String> fpSet = new HashMap<>(10); // false positives
        HashMap<String, String> tnSet = new HashMap<>(10); // true negatives
        HashMap<String, String> fnSet = new HashMap<>(10); // false negatives

        // Create the directories if they do not exist
        createDir(HEADER1);
        createDir(HEADER2);

        // Read in the tagged dataset and perform cross-validation
        Instances taggedSet = readDataset(HEADER1 + "/processed_single_corpus.arff");
        Classifier tree = new IBk();
        Evaluation crossValidation = crossValidate(tree, taggedSet, 10, 1);
        System.out.println(crossValidation.toSummaryString("\nCross validation - Results\n\n", false));
        System.out.println(crossValidation.toClassDetailsString("\nCross validation - Statistics\n\n"));

        trainAndTest(tpSet, fpSet, tnSet, fnSet, taggedSet, tree); // train and test

        analysisData(tpSet, fpSet, tnSet, fnSet);

        resultData(tpSet, fpSet, fnSet);
    }

    /**
     * Performs classification on a set of data using the given classifier,
     * and stores information about TP, FP, TN, and FN cases in separate HashMaps.
     *
     * @param tpSet HashMap to store information about TP cases
     * @param fpSet HashMap to store information about FP cases
     * @param tnSet HashMap to store information about TN cases
     * @param fnSet HashMap to store information about FN cases
     * @param taggedSet Instances object representing the training set
     * @param tree Classifier object to use for classification
     * @throws Exception if there is an error reading or writing data to files
     */

    private static void trainAndTest(HashMap<String, String> tpSet, HashMap<String, String> fpSet, HashMap<String, String> tnSet, HashMap<String, String> fnSet, Instances taggedSet, Classifier tree) throws Exception {
        // build the classifier using the training set
        tree.buildClassifier(taggedSet);

        // read in the test set
        Instances testInput = DataSource.read(HEADER1 + "/processed_single_corpus.arff");
        testInput.setClassIndex(taggedSet.numAttributes() - 1);

        // classify the test set
        Instances classifiedSet = new Instances(testInput);
        for (int i = 0; i < testInput.numInstances(); i++) {
            double clsLabel = tree.classifyInstance(testInput.instance(i));
            classifiedSet.instance(i).setClassValue(clsLabel);
        }

        // write the classified test set to a file
        writeClassifiedSetToFile(classifiedSet, HEADER2 + "/classified_set.arff");

        // read in the classified set from the file
        classifiedSet = DataSource.read(HEADER2 + "/classified_set.arff");

        // check that the classified set has the same number of entries as the training set
        if (classifiedSet.size() != taggedSet.size()) {
            throw new Exception("Training set and tagged set differ in number of entries.");
        }

        // open the file containing the word pairs and vectors for the test set
        BufferedReader br = new BufferedReader(new FileReader(HEADER1 + "/processed_single_corpus_with_words.arff"));

        // skip all of the arff file header
        String line;
        while (!br.readLine().contains("@DATA")) {
        }

        line = br.readLine();
        // iterate through the entries in the classified set
        for (int i = 0; i < taggedSet.size(); i++, line = br.readLine()) {
            // get the truth values for the current entry in the training set and classified set
            String trainSetEntry = taggedSet.get(i).toString();
            String testSetEntry = classifiedSet.get(i).toString();
            boolean trainTruthValue = trainSetEntry.substring(trainSetEntry.lastIndexOf(",") + 1).equals("true");
            boolean testTruthValue = testSetEntry.substring(testSetEntry.lastIndexOf(",") + 1).equals("true");

            // get the noun pair and vector for the current entry in the test set
            String nounPair = line.substring(0, line.indexOf("\t"));
            String vector = line.substring(line.indexOf("\t") + 1);

            // store the information in the appropriate HashMap
            if (trainTruthValue && testTruthValue && tpSet.size() < 10) {
                tpSet.put(nounPair, vector);
            } else if (!trainTruthValue && testTruthValue && fpSet.size() < 10) {
                fpSet.put(nounPair, vector);
            } else if (!trainTruthValue && !testTruthValue && tnSet.size() < 10) {
                tnSet.put(nounPair, vector);
            } else if (trainTruthValue && !testTruthValue && fnSet.size() < 10) {
                fnSet.put(nounPair, vector);
            }
        }
    }

    /**
     * Performs cross-validation on a dataset using a classifier.
     * @param classifier the classifier to use for cross-validation
     * @param dataset the dataset to perform cross-validation on
     * @param numFolds the number of folds to use for cross-validation
     * @param randomSeed the seed for the random number generator
     * @return the evaluation of the cross-validation
     * @throws Exception if an error occurs while performing cross-validation
     */
    public static Evaluation crossValidate(Classifier classifier, Instances dataset, int numFolds, int randomSeed) throws Exception {
        Evaluation evaluation = new Evaluation(dataset);
        evaluation.crossValidateModel(classifier, dataset, numFolds, new Debug.Random(randomSeed));
        return evaluation;
    }


    /**
     * Creates a directory if it does not already exist.
     * @param dirName the name of the directory to create
     * @throws Exception if an error occurs while creating the directory
     */
    public static void createDir(String dirName) throws Exception {
        java.nio.file.Path path = Paths.get(dirName);
        if (!Files.exists(path)) {
            Files.createDirectory(path);
        }
    }

    /**
     * Reads a dataset from a file and sets the class index to the last attribute.
     * @param fileName the name of the file to read the dataset from
     * @return the dataset read from the file
     * @throws Exception if an error occurs while reading the dataset
     */
    public static Instances readDataset(String fileName) throws Exception {
        Instances dataset = DataSource.read(fileName);
        dataset.setClassIndex(dataset.numAttributes() - 1);
        return dataset;
    }


    /**
     * Writes the given classified dataset to a file.
     *
     * @param classifiedSet the classified Instances object to write to a file
     * @param filePath the file path to write the classified dataset to
     * @throws Exception if there is an error during writing
     */
    public static void writeClassifiedSetToFile(Instances classifiedSet, String filePath) throws Exception {
        ConverterUtils.DataSink.write(filePath, classifiedSet);
    }


    /**
     * Calculates and write to result file evaluation metrics based on true positive and false positive sets.
     *
     * @param tpSet A set of true positive values.
     * @param fpSet A set of false positive values.
     * @param fnSet A set of false negative values.
     */
    private static void resultData(HashMap<String, String> tpSet, HashMap<String, String> fpSet, HashMap<String, String> fnSet) {
        // Calculate the number of true positives
        int tp = tpSet.size();

        // Calculate the number of false positives
        int fp = fpSet.size();

        // Calculate the number of false negatives
        int fn = fnSet.size();

        // Calculate precision
        double precision = (double) tp / (tp + fp);

        // Calculate recall
        double recall = (double) tp / (tp + fn);

        // Calculate F1 measure
        double f1 = 2 * (precision * recall) / (precision + recall);

        try {
            // Create a new file called "myfile.txt"
            FileWriter fw = new FileWriter("output/result");

            // Wrap the FileWriter in a BufferedWriter
            BufferedWriter bw = new BufferedWriter(fw);

            // Write the content to the file
            bw.write("Result data:\n");
            bw.write("Precision: " + precision + "\n");
            bw.write("Recall: " + recall + "\n");
            bw.write("F1 measure: " + f1 + "\n");


            // Close the BufferedWriter and the FileWriter
            bw.close();
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Analyzes the true positive, false positive, true negative, and false negative sets and outputs
     * the results.
     *
     * @param tpSet the true positive set
     * @param fpSet the false positive set
     * @param tnSet the true negative set
     * @param fnSet the false negative set
     */
    private static void analysisData(HashMap<String, String> tpSet, HashMap<String, String> fpSet, HashMap<String, String> tnSet, HashMap<String, String> fnSet) {
        System.out.println("\n\nanalysis data:");
        System.out.println("\n\nTrue positives:");
        for (Map.Entry<String, String> entry : tpSet.entrySet())
            System.out.println(entry.getKey() + "\n" + entry.getValue());
        System.out.println("\n\nFalse positives:");
        for (Map.Entry<String, String> entry : fpSet.entrySet())
            System.out.println(entry.getKey() + "\n" + entry.getValue());
        System.out.println("\n\nTrue negatives:");
        for (Map.Entry<String, String> entry : tnSet.entrySet())
            System.out.println(entry.getKey() + "\n" + entry.getValue());
        System.out.println("\n\nFalse negatives:");
        for (Map.Entry<String, String> entry : fnSet.entrySet())
            System.out.println(entry.getKey() + "\n" + entry.getValue());
    }

}