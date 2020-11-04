import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;
import guru.nidi.graphviz.model.Node;

import javax.swing.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static guru.nidi.graphviz.model.Factory.*;

public class main {
    public static void main(String[] args) throws Exception {

//        BTree bTree = random_tree(60,9);
        System.setProperty("java.awt.headless", "false");

        BTree bTree = test_insertion(5);
        String rel_path = graphviz_display(bTree.rootNode, null, null, "1. Build tree");

        JFrame f = new JFrame("B Tree");
        ImageIcon i = new ImageIcon(rel_path);
        JLabel l = new JLabel(i);
        JPanel p = new JPanel();
        p.add(l);
        f.add(p);
        f.setSize(i.getIconWidth(), i.getIconHeight() + 50);
        f.show();


        int ops_counter = 2;

        while (true) {
            System.out.println("Insert or delete a node (example : 'delete 15', 'insert 45):\n");
            Scanner sc = new Scanner(System.in);
            String user_command = sc.nextLine();
            String[] user_command_array = user_command.strip().split("\\s+");
            if (user_command_array.length == 2) {
                int key = Integer.parseInt(user_command_array[1]);
                if (user_command_array[0].equals("delete")) {
                    bTree.single_executioner.delete(new int[]{key});
                    rel_path = graphviz_display(bTree.rootNode, null, null, ops_counter + ". " + user_command.strip());
                    i = new ImageIcon(rel_path);
                    l.setIcon(i);
                    ops_counter++;
                } else if (user_command_array[0].equals("insert")) {
                    bTree.single_executioner.insert(new BTree.Entry[]{bTree.new Entry(key, key)});
                    rel_path = graphviz_display(bTree.rootNode, null, null, ops_counter + ". " + user_command.strip());
                    i = new ImageIcon(rel_path);
                    l.setIcon(i);
                    ops_counter++;
                } else {
                    System.out.println("error");
                }
            }
        }
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

    public static BTree test_insertion(int tree_order) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader("data.txt"));
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

    public static BTree.Entry[] test_get(BTree bt) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("data.txt"));
        String line[] = br.readLine().split("\\s+");

        SingleExecutioner sExecutioner = new SingleExecutioner(bt);

        //create an array of keys
        int keys[] = new int[line.length];
        for (int i = 0; i < line.length; i++)
            keys[i] = Integer.parseInt(line[i]);

        //get entries
        BTree.Entry entries[] = sExecutioner.get(keys);

        return entries;

    }

    public static void display(BTree.Node root) {
        if (root == null)
            return;

        System.out.println(root);

        if (root.isLeaf)
            return;

        for (int i = 0; i < root.used_slots + 1; i++)
            if (root.childrenNode[i] != null)
                display(root.childrenNode[i]);

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
