import java.util.LinkedList;
import java.util.List;

/**
 * Created by asafchelouche on 26/7/16.
 * A node in the tree representation of a syntactic Ngram.
 */

class Node {

    private String stemmedWord;

    public String getWord() {
        return word;
    }

    private String word;
    private String pos_tag; // part of speech
    private String dep_label; // stanford dependency
    private int father;
    private List<Node> children;

    Node(String[] args, Stemmer stemmer) {
        stemmer.add(args[0].toCharArray(), args[0].length());
        stemmer.stem();
        this.word = args[0];
        this.stemmedWord = stemmer.toString();
        this.pos_tag = args[1];
        this.dep_label = args[2];
        try {
            this.father = Integer.parseInt(args[3]);

        } catch (Exception e) {
        }
        children = new LinkedList<>();
    }

    String getStemmedWord() {
        return stemmedWord;
    }

    void addChild(Node child) {
        children.add(child);
    }

    int getFather() {
        return father;
    }

    String getDepencdencyPathComponent() {
//        return String.join(":", stemmedWord, pos_tag); // for later use?
        return pos_tag;
    }

    boolean isNoun() {
        return pos_tag.equals("NN") || pos_tag.equals("NNS") || pos_tag.equals("NNP") || pos_tag.equals("NNPS");
    }

    List<Node> getChildren() {
        return children;
    }

}
