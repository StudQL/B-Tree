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
import java.sql.Time;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static guru.nidi.graphviz.model.Factory.*;

public class main {
    public static void main(String[] args) throws Exception {

//        BTree bTree = random_tree(60,9);
        System.setProperty("java.awt.headless", "false");

        // Multi-threading
        long startTime = System.currentTimeMillis();
        BTree bTree = test_multi(10);
        String rel_path = graphviz_display(bTree.rootNode, null, null, "1. Build tree");

        JFrame f = new JFrame("1. Build tree");
        ImageIcon i = new ImageIcon(rel_path);
        f.setSize(i.getIconWidth(), i.getIconHeight() + 50);
        f.add(new JPanel().add(new JLabel(i)));
        f.setLocationRelativeTo(null);
        f.show();
        long endTime = System.currentTimeMillis();
        long TimeElapsed = endTime - startTime;
        System.out.println();
        System.out.printf("Total Execution Time : " + TimeElapsed);

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
    public static BTree test_multi(int tree_order) throws Exception {
        BTree bT = new BTree(tree_order);
        MultiExecutioner mExecutioner = new MultiExecutioner(bT);
        int n_nodes = 400;

        // create an array of entries
        Random random = new Random();
        int[] array = random.ints(n_nodes, 0, n_nodes).toArray();
        BTree.Entry[] entries_array = new BTree.Entry[n_nodes];
        for (int i = 0; i < n_nodes; i++) {
            entries_array[i] = bT.new Entry(array[i], array[i]);
        }
        // Initialize Executor
        ExecutorService executor = mExecutioner.initialize_executor();

        // Insert Tasks - creation of the Tree
        List<Callable<String>> tasks = new ArrayList<>();
        int id = 0;
        for (BTree.Entry entry : entries_array) {
            //System.out.println(entry);
            try {
                Callable<String> task = mExecutioner.insert_task(entry, id);
                tasks.add(task);
            } catch (Exception e) {
                e.printStackTrace();
            }
            id++;
        }
        List<Future<String>> futures = mExecutioner.execute_tasks(executor, tasks);
        // Search Tasks
        List<Callable<String>> tasks2 = new ArrayList<>();
        int id2 = 1;
        int[] search = new int[]{92, 75,60, 17, 45, 50};
        BTree.Entry[] test = new BTree.Entry[600];
        int idx = 0;
        for(int j = 0; j < 600; j++){
            if(idx >= 6){
                idx = 0;
            }
            test[j] = bT.new Entry(search[idx], search[idx]);
            idx++;
        }
        for(BTree.Entry entry : test)
        {
            try {
                Callable<String> task = mExecutioner.search_task(entry.key, id2);
                tasks2.add(task);
            } catch (Exception e) {
                e.printStackTrace();
            }
            id2++;
        }
        List<Future<String>> futures2 = mExecutioner.execute_tasks(executor, tasks2);
        for(Future<String> future : futures2){
            try {
                System.out.println(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        // Delete Tasks
        /*
        List<Callable<String>> tasks3 = new ArrayList<>();
        int id3 = 0;
        int[] keys = new int[]{92, 75, 60};
        for(int key : keys)
        {

            try {
                Callable<String> task = mExecutioner.delete_task(new int[]{key}, id3);
                tasks3.add(task);
            } catch (Exception e) {
                e.printStackTrace();
            }
            id3++;
        }
        List<Future<String>> futures3 = mExecutioner.execute_tasks(executor, tasks3);
        for(Future<String> future : futures3){
            try {
                System.out.println(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }*/
        mExecutioner.stop_executor(executor);

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
