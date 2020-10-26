import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class main {
    public static void main(String[] args) throws Exception {
        BTree bT = new BTree(5);
        BTree.Node root = bT.rootNode;
        root.addEntry(1,null);
        System.out.println(root);
        root.addEntry(0,null);
        System.out.println(root);
        root.addEntry(4,null);
        System.out.println(root);
        root.addEntry(3,null);
        System.out.println(root);
        root.removeEntry(1);
        System.out.println(root);
        root.removeEntry(3);
        System.out.println(root);

    }

    public BTree read_toy_example() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("../toy-tree.txt"));
        String line = br.readLine();
        int M = Integer.parseInt(line);
        BTree bT = new BTree(M);

        while ((line = br.readLine()) != null) {
            //TODO once we have insert function
        }
        return bT;
    }
}
