import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.MutableNode;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.IntStream;

import static guru.nidi.graphviz.model.Factory.graph;
import static guru.nidi.graphviz.model.Factory.mutNode;

public class main {
    public static void main(String[] args) throws Exception {

        // Tests section. Long runtime.

//        PerformanceTest t;
//        ArrayList<int[]> r;
//        t = new PerformanceTest("./generated_data.csv", 100, 12);
//
//        t.test_search(0, 1000000, 20000, 5);
//        t.test_search(0, 8000, 50, 10);
//
//        t.test_insert(0,100000,20000,5);
//        t.test_insert(0,8000,50,10);
//
//        t.test_delete(0,8000,500,1);

        BTree bTree = build_tree_from_list(IntStream.range(0, 50).toArray(), 5);
        System.setProperty("java.awt.headless", "false");

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
                    System.out.println("Invalid user command");
                    valid_user_command = false;
                }

                if (valid_user_command) {
                    f = new JFrame(ops_counter + ". " + user_command.strip());
                    i = new ImageIcon(rel_path);
                    f.setSize(i.getIconWidth(), i.getIconHeight() + 50);
                    f.add(new JPanel().add(new JLabel(i)));
                    f.setLocationRelativeTo(null);
                    f.show();
                }
            }
        }
    }

    public static BTree build_tree_from_list(int[] list, int tree_order) {
        BTree bT = new BTree(tree_order);
        BTree.Entry[] entries_array = new BTree.Entry[list.length];
        for (int i = 0; i < list.length; i++) {
            entries_array[i] = bT.new Entry(list[i], list[i]);
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
            MutableNode child = mutNode(current.childrenNode[i].getEntriesString());
            graphviz_current.addLink(child);
            if (!current.childrenNode[i].isLeaf) {
                graphviz_display(current.childrenNode[i], child, nodes, tree_name);
            }
            nodes.add(child);
        }

        Graph g = graph(tree_name).directed().with(nodes);
        String rel_path = "example/" + tree_name + ".png";
        Graphviz.fromGraph(g).scale(1.2).render(Format.PNG).toFile(new File(rel_path));
        return rel_path;
    }
}
