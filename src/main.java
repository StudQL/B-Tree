import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.MutableNode;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

import static guru.nidi.graphviz.model.Factory.graph;
import static guru.nidi.graphviz.model.Factory.mutNode;

public class main {
    public static void main(String[] args) throws Exception {

        PerformanceTest t = new PerformanceTest("./generated_data_medium.csv", 100, 8);
        ArrayList<int[]> r = t.test_search(0, 1000000, 50000, 20);
        for (int[] a : r) {
            System.out.println(a[0] + " entries : " + a[1] + " | " + a[2] + " | " + a[3]);
        }

//        HashMap<Integer, String> map =  new HashMap<Integer, String>();
//        map.put(1000001,"hi");
//        t.sbt.insert(map);
//        ArrayList a = new ArrayList<>();
//        a.add(1000001);
//        System.out.println(t.sbt.get(a).collect());

//        System.out.println(Arrays.toString(array));
//        System.out.println(Arrays.toString(output_single));

//        System.out.println("hi");
//        BTree bTree = build_tree_from_list(keys,7);
//        System.out.println("hi");
//        System.setProperty("java.awt.headless", "false");
////        BTree bTree = test_insertion(5);
//
//        String rel_path = graphviz_display(bTree.rootNode, null, null, "1. Build tree");
//
//        JFrame f = new JFrame("1. Build tree");
//        ImageIcon i = new ImageIcon(rel_path);
//        f.setSize(i.getIconWidth(), i.getIconHeight() + 50);
//        f.add(new JPanel().add(new JLabel(i)));
//        f.setLocationRelativeTo(null);
//        f.show();
//
//
//        int ops_counter = 1;
//
//        while (true) {
//            System.out.println("Insert or delete a node (example : 'delete 15', 'insert 45):\n");
//            Scanner sc = new Scanner(System.in);
//            String user_command = sc.nextLine();
//            String[] user_command_array = user_command.strip().split("\\s+");
//            boolean valid_user_command = true;
//
//            if (user_command_array.length == 2) {
//                int key = Integer.parseInt(user_command_array[1]);
//                if (user_command_array[0].equals("delete")) {
//                    bTree.single_executioner.delete(new int[]{key});
//                    rel_path = graphviz_display(bTree.rootNode, null, null, ops_counter + ". " + user_command.strip());
//                    ops_counter++;
//                } else if (user_command_array[0].equals("insert")) {
//                    bTree.single_executioner.insert(new BTree.Entry[]{bTree.new Entry(key, key)});
//                    rel_path = graphviz_display(bTree.rootNode, null, null, ops_counter + ". " + user_command.strip());
//                    ops_counter++;
//                } else {
//                    System.out.println("error");
//                    valid_user_command = false;
//                }
//
//                if(valid_user_command){
//                    f = new JFrame(ops_counter + ". " + user_command.strip());
//                    i = new ImageIcon(rel_path);
//                    f.setSize(i.getIconWidth(), i.getIconHeight() + 50);
//                    f.add(new JPanel().add(new JLabel(i)));
//                    f.setLocationRelativeTo(null);
//                    f.show();
//                }
//            }
//        }
    }

    public static BTree build_tree_from_list(List<Integer> list, int tree_order) {
        BTree bT = new BTree(tree_order);
        BTree.Entry[] entries_array = new BTree.Entry[list.size()];
        for (int i = 0; i < list.size(); i++) {
            entries_array[i] = bT.new Entry(list.get(i), list.get(i));
        }
        bT.single_executioner.insert(entries_array);
        return bT;
    }

    private static List<Integer> random_numbers(int n_nodes) {
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 0; i < n_nodes; i++) {
            list.add(i);
        }
        Collections.shuffle(list);
        return list;
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
        Graphviz.fromGraph(g).scale(1.2).render(Format.PNG).toFile(new File(rel_path));
        return rel_path;
    }
}
