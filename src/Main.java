import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {

        /*
         * BTree bT = new BTree(5); 
         * BTree.Node root = bT.rootNode;
         * root.addEntry(1,null); 
         * System.out.println(root); 
         * root.addEntry(0,null);
         * System.out.println(root); 
         * root.addEntry(4,null); 
         * System.out.println(root);
         * root.addEntry(3,null); 
         * System.out.println(root); 
         * root.removeEntry(1);
         * System.out.println(root); 
         * root.removeEntry(3); 
         * System.out.println(root);
         */

         
        BTree bTree = test_insertion();
        // display the BTree after insertion
        display(bTree.rootNode);
        System.out.println("-------------");

        BTree.Entry entries[] = test_get(bTree);

        for(int i=0; i<entries.length; i++)
            System.out.println(entries[i]);


    }

    public BTree read_toy_example() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("../toy-tree.txt"));
        String line = br.readLine();
        int M = Integer.parseInt(line);
        BTree bT = new BTree(M);

        while ((line = br.readLine()) != null) {
            // TODO once we have insert function
        }
        return bT;
    }

    public static BTree test_insertion() throws Exception {

        BufferedReader br = new BufferedReader(new FileReader("data.txt"));
        String line[] = br.readLine().split("\\s+");

        BTree bT = new BTree(5);
        SingleExecutioner sExecutioner = new SingleExecutioner(bT);

        // create an array of entries
        BTree.Entry entries[] = new BTree.Entry[line.length];
        for (int i = 0; i < line.length; i++) {
            int key = Integer.parseInt(line[i]);
            entries[i] = bT.new Entry(key, key);
        }

        sExecutioner.insert(entries);
        
        return bT;
    }

    public static BTree.Entry[] test_get(BTree bt) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("data.txt"));
        String line[] = br.readLine().split("\\s+");

        SingleExecutioner sExecutioner = new SingleExecutioner(bt);
        
        //create an array of keys
        int keys [] = new int[line.length];
        for(int i=0; i<line.length; i++)
            keys[i] = Integer.parseInt(line[i]);

        //get entries    
        BTree.Entry entries[] = sExecutioner.get(keys);
        
        return entries;

    }
   public static void display(BTree.Node root)
    {
        if(root==null)
            return;
        
        System.out.println(root);
            
        if(root.isLeaf)
            return;

        for(int i=0;i<root.used_slots+1;i++)
                if(root.childrenNode[i] != null)
                    display(root.childrenNode[i]);
        
    }
}
