import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;
import guru.nidi.graphviz.model.Node;

import javax.swing.*;
import java.io.*;
import java.sql.Time;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static guru.nidi.graphviz.model.Factory.*;

public class main {
    public static void main(String[] args) throws Exception {
        PerformanceTest t = new PerformanceTest("B-Tree/generated_data2.csv", 100, 8);
        /*ArrayList<int[]> r = t.test_search(0, 1000000, 50000, 5);
        for (int[] a : r) {
            System.out.println(a[0] + " entries : " + a[1] + " | " + a[2] + " | " + a[3]);
        }
        ArrayList<int[]> r = t.test_insert(0, 1000000, 50000, 5);
        for (int[] a : r) {
            System.out.println(a[0] + " entries : " + a[1] + " | " + a[2] + " | " + a[3]);
        }
        ArrayList<int[]> r2 = t.test_delete(0, 1000000, 50000, 5);
        for (int[] a : r2) {
            System.out.println(a[0] + " entries : " + a[1] + " | " + a[2] + " | " + a[3]);
        }
        */
        //BTree bTree = random_tree(60,9);
        //System.setProperty("java.awt.headless", "false");

        // Multi-threading
        //BTree bTree = test_multi(5);

        /*String rel_path = graphviz_display(bTree.rootNode, null, null, "1. Build tree");

        JFrame f = new JFrame("1. Build tree");
        ImageIcon i = new ImageIcon(rel_path);
        f.setSize(i.getIconWidth(), i.getIconHeight() + 50);
        f.add(new JPanel().add(new JLabel(i)));
        f.setLocationRelativeTo(null);
        f.show();
        */

        //Single-threading
        /*
        BTree bTree = test_insertion(5);
        String rel_path = graphviz_display(bTree.rootNode, null, null, "1. Build tree");

        JFrame f = new JFrame("1. Build tree");
        ImageIcon i = new ImageIcon(rel_path);
        f.setSize(i.getIconWidth(), i.getIconHeight() + 50);
        f.add(new JPanel().add(new JLabel(i)));
        f.setLocationRelativeTo(null);
        f.show();


        int ops_counter = 1;

        while (true) {
            System.out.println("Insert or delete a node (example : 'delete 15', 'insert 45):\n");
            Scanner sc = new Scanner(System.in);
            String user_command = sc.nextLine();
            String[] user_command_array = user_command.strip().split("\\s+");
            boolean valid_user_command = true;

            if (user_command_array.length == 2) {
                int key = Integer.parseInt(user_command_array[1]);
                if (user_command_array[0].equals("delete")) {
                    bTree.single_executioner.delete(new int[]{key});
                    rel_path = graphviz_display(bTree.rootNode, null, null, ops_counter + ". " + user_command.strip());
                    ops_counter++;
                } else if (user_command_array[0].equals("insert")) {
                    bTree.single_executioner.insert(new BTree.Entry[]{bTree.new Entry(key, key)});
                    rel_path = graphviz_display(bTree.rootNode, null, null, ops_counter + ". " + user_command.strip());
                    ops_counter++;
                } else {
                    System.out.println("error");
                    valid_user_command = false;
                }

                if(valid_user_command){
                    f = new JFrame(ops_counter + ". " + user_command.strip());
                    i = new ImageIcon(rel_path);
                    f.setSize(i.getIconWidth(), i.getIconHeight() + 50);
                    f.add(new JPanel().add(new JLabel(i)));
                    f.setLocationRelativeTo(null);
                    f.show();
                }
            }
        }
        */
    }

    public static BTree test_insertion(int tree_order) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader("B-Tree/data.txt"));
        String line[] = br.readLine().split("\\s+");
        System.out.println(Arrays.toString(line));

        BTree bT = new BTree(tree_order);
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

    public static BTree random_tree(int n_nodes, int tree_order) {
        BTree bT = new BTree(tree_order);
        Random random = new Random();
        int[] array = random.ints(n_nodes, 0, n_nodes).toArray();
        BTree.Entry[] entries_array = new BTree.Entry[n_nodes];
        for (int i = 0; i < n_nodes; i++) {
            entries_array[i] = bT.new Entry(array[i], array[i]);
        }
        bT.single_executioner.insert(entries_array);
        return bT;
    }

    public static String graphviz_display(BTree.Node current, MutableNode graphviz_current, List<MutableNode> nodes, String tree_name) throws IOException {
        if (nodes == null)
            nodes = new ArrayList<>();
        graphviz_current = mutNode(current.getEntriesString());
        nodes.add(graphviz_current);

        for (int i = 0; i < current.used_slots + 1; i++) {
            System.out.println(current.childrenNode[i].getEntriesString());
            MutableNode child = mutNode(current.childrenNode[i].getEntriesString());
            graphviz_current.addLink(child);
            if (!current.childrenNode[i].isLeaf) {
                graphviz_display(current.childrenNode[i], child, nodes, tree_name);
            }
            nodes.add(child);
        }

        Graph g = graph(tree_name).directed().with(nodes);
        String rel_path = "example/" + tree_name + ".png";
        Graphviz.fromGraph(g).scale(1.5).render(Format.PNG).toFile(new File(rel_path));
        return rel_path;
    }
}
