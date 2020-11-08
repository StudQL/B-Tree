import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class MultiExecutioner extends AbstractExecutioner implements Serializable {
    public Lock rootLock= new ReentrantLock();

    MultiExecutioner(BTree bTree) {
        super(bTree);
    }

    public ExecutorService initialize_executor(){
        int nbProcs = Runtime.getRuntime().availableProcessors();
        System.out.println("Nb of available Threads :" + nbProcs);
        ExecutorService executor = Executors.newFixedThreadPool(nbProcs);
        return executor;
    }

    public ExecutorService initialize_executor_single(){
        ExecutorService executor = Executors.newSingleThreadExecutor();
        return executor;
    }

    public void stop_executor(ExecutorService executor){
        try {
            System.out.println("attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            System.err.println("tasks interrupted");
        }
        finally {
            if (!executor.isTerminated()) {
                System.err.println("cancel non-finished tasks");
            }
            executor.shutdownNow();
            System.out.println("shutdown finished");
        }
    }

    public List<Future<String>> execute_tasks(ExecutorService executor, List<Callable<String>> tasks){
        List<Future<String>> futures = null;
        try {
            futures = executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return futures;
    }

    public Callable<String> search_task(int[] keys, int id){
        Callable<String> task = () -> {
            try {
                BTree.Entry[] entries = get(keys);
                String text = "";
                if(entries != null) {
                    text = "search task success n°" + id ;
                }
                else{
                    text = "search task finished but unsuccessful n°" + id;
                }
                return text;
            }
            catch (Exception e) {
                throw new IllegalStateException("task interrupted", e);
            }
        };
        return task;
    }

    public Callable<String> insert_task(BTree.Entry entry, int id){
        Callable<String> task = () -> {
            try {
                rootLock.lock();
                single_insert(entry);
                rootLock.unlock();
                String text = "insert task success n°" + id;
                return text;
            }
            catch (Exception e) {
                throw new IllegalStateException("task interrupted", e);
            }
        };
        return task;
    }

    public Callable<String> delete_task(int[] keys, int id){
        Callable<String> task = () -> {
            try {
                rootLock.lock();
                delete(keys);
                rootLock.unlock();
                String text = "delete task success n°" + id;
                return text;
            }
            catch (Exception e) {
                throw new IllegalStateException("task interrupted", e);
            }
        };
        return task;
    }

    BTree.Entry[] get(int[] keys) {
        Object[][] node_entry_pairs = get_node_entry_pairs(keys);
        BTree.Entry[] output = new BTree.Entry[keys.length];
        for (int i = 0; i < keys.length; i++)
            output[i] = (BTree.Entry) node_entry_pairs[i][1];
        return output;
    }

    private Object[][] get_node_entry_pairs(int[] keys) {
        Object[][] output = new Object[keys.length][2];
        for (int i = 0; i < keys.length; i++)
            output[i] = single_get(keys[i]);
        return output;
    }

    private Object[] single_get(int key) {
        BTree.Node root = bT.rootNode;
        root.lock.readLock().lock();
        while (!root.isLeaf) {
            if (key > root.entries[root.used_slots - 1].key) {
                root.lock.readLock().unlock();
                root = root.childrenNode[root.used_slots];
                root.lock.readLock().lock();
                continue;
            } else {
                for (int i = 0; i < root.used_slots; i++) {
                    if (root.entries[i].key == key) {
                        root.lock.readLock().unlock();
                        return new Object[]{root, root.entries[i]};
                    }

                    else if (root.entries[i].key > key) {
                        root.lock.readLock().unlock();
                        root = root.childrenNode[i];
                        root.lock.readLock().lock();
                        break;
                    }
                }
            }
        }
        if (root.getKeyIndex(key) != -1) {
            root.lock.readLock().unlock();
            return new Object[]{root, root.entries[root.getKeyIndex(key)]};
        }
        else
            return null;
    }

    @Override
    void insert(BTree.Entry[] entries) {
        ExecutorService executor = initialize_executor();
        List<Callable<String>> tasks = new ArrayList<>();
        int id = 0;
        for (BTree.Entry entry : entries) {
            System.out.println(entry);
            try {
                Callable<String> task = insert_task(entry, id);
                tasks.add(task);
            } catch (Exception e) {
                e.printStackTrace();
            }
            id++;
        }
        List<Future<String>> futures = execute_tasks(executor, tasks);
        for(Future<String> future : futures){
            try {
                System.out.println(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        stop_executor(executor);
    }

    void single_insert(BTree.Entry entry) throws Exception {

        if (entry == null)
            throw new IllegalArgumentException("entry argument is null");
        // Root node without entry
        if ((bT.rootNode.isRoot) && (bT.rootNode.used_slots == 0)) {
            bT.rootNode.entries[0] = entry;
            bT.rootNode.used_slots++;
            return;
        }
        BTree.Node temp = bT.rootNode;
        while (!temp.isLeaf) {
            if (temp.entries[temp.used_slots - 1].key == entry.key)
                return;

            if (entry.key > temp.entries[temp.used_slots - 1].key) {
                temp = temp.childrenNode[temp.used_slots];
                continue;
            }
            for (int i = 0; i < temp.used_slots; i++) {
                if (temp.entries[i].key == entry.key)
                    return;

                if (entry.key < temp.entries[i].key) {
                    temp = temp.childrenNode[i];
                    break;
                }
            }
        }

        if (temp.used_slots == bT.max_keys)
            temp = splitNode(temp, entry);
        temp.addEntry(entry);


    }

    private BTree.Node splitRootNode(BTree.Node node, BTree.Entry entry) {

        int mid = bT.max_keys / 2;
        BTree.Node leftNode = bT.new Node(false, true);
        BTree.Node rightNode = bT.new Node(false, true);
        System.arraycopy(node.entries, 0, leftNode.entries, 0, mid);
        leftNode.used_slots = mid;
        System.arraycopy(node.entries, mid + 1, rightNode.entries, 0, mid - 1);
        rightNode.used_slots = mid - 1;

        BTree.Entry tmp = node.entries[mid];
        node.entries = new BTree.Entry[bT.max_keys];
        node.entries[0] = tmp;
        node.used_slots = 1;

        node.isLeaf = false;
        node.childrenNode = new BTree.Node[bT.M];
        node.childrenNode[0] = leftNode;
        node.childrenNode[1] = rightNode;
        leftNode.parentNode = node;
        rightNode.parentNode = node;
        leftNode.rightSibling = rightNode;
        rightNode.leftSibling = leftNode;

        if (entry.key > node.entries[0].key)
            return rightNode;

        return leftNode;
    }

    private BTree.Node splitParentNode(BTree.Node node, BTree.Entry entry) {

        int mid = bT.max_keys / 2;
        BTree.Node leftNode = bT.new Node(false, false);
        BTree.Node rightNode = bT.new Node(false, false);
        System.arraycopy(node.entries, 0, leftNode.entries, 0, mid);
        leftNode.used_slots = mid;
        System.arraycopy(node.entries, mid + 1, rightNode.entries, 0, mid - 1);
        rightNode.used_slots = mid - 1;

        BTree.Entry tmp = node.entries[mid];
        node.entries = new BTree.Entry[bT.max_keys];
        node.entries[0] = tmp;
        node.used_slots = 1;

        leftNode.childrenNode = new BTree.Node[bT.M];
        rightNode.childrenNode = new BTree.Node[bT.M];

        for (int i = 0; i < mid; i++) {
            leftNode.childrenNode[i] = node.childrenNode[i];
            leftNode.childrenNode[i].parentNode = leftNode;
            rightNode.childrenNode[i] = node.childrenNode[i + mid + 1];
            rightNode.childrenNode[i].parentNode = rightNode;
        }
        leftNode.childrenNode[mid] = node.childrenNode[mid];
        leftNode.childrenNode[mid].parentNode = leftNode;


        leftNode.childrenNode[mid].rightSibling = null;
        rightNode.childrenNode[0].leftSibling = null;

        node.childrenNode = new BTree.Node[bT.M];
        node.childrenNode[0] = leftNode;
        node.childrenNode[1] = rightNode;
        leftNode.parentNode = node;
        rightNode.parentNode = node;
        leftNode.rightSibling = rightNode;
        rightNode.leftSibling = leftNode;

        if (entry.key > node.entries[0].key)
            return rightNode;

        return leftNode;
    }

    public BTree.Node splitNode(BTree.Node node, BTree.Entry entry) {

        // root node
        if (node.isRoot)
            return splitRootNode(node, entry);

        int mid = bT.max_keys / 2;
        BTree.Node parent = node.parentNode;

        // parent node full of entries so we have to split it before
        if ((parent.used_slots == bT.max_keys) && (parent.parentNode == null))
            parent = splitParentNode(parent, node.entries[mid]);
        else if (parent.used_slots == bT.max_keys)
            parent = splitNode(parent, node.entries[mid]);

        BTree.Node rightNode = bT.new Node(false, true);
        System.arraycopy(node.entries, mid + 1, rightNode.entries, 0, mid - 1);
        rightNode.used_slots = mid - 1;
        node.used_slots = mid;

        //for recursive call
        if (node.childrenNode != null)
        {
            rightNode.childrenNode = new BTree.Node[bT.M];
            for (int i = 0; i < mid; i++)
            {   rightNode.childrenNode[i] = node.childrenNode[i + mid + 1];
                node.childrenNode[mid + 1 + i] = null;
                rightNode.childrenNode[i].parentNode = rightNode;
            }
            node.childrenNode[mid].rightSibling = null;
            rightNode.childrenNode[0].leftSibling = null;
            rightNode.parentNode = parent;
            node.rightSibling = rightNode;
            rightNode.leftSibling = node;
            node.isLeaf = false;
            rightNode.isLeaf = false;
        }

        int index = parent.used_slots;
        BTree.Entry midEntry = node.entries[mid];
        for(int i=mid; i<2*mid; i++)
            node.entries[i] = null;

        for(int i=parent.used_slots-1; i>-1;i--)
        {   if(midEntry.key<parent.entries[i].key)
        {
            parent.entries[i+1] = parent.entries[i];
            parent.childrenNode[i+2] = parent.childrenNode[i+1];
            index = i;
        }
        else break;
        }
        parent.entries[index] = midEntry;
        parent.childrenNode[index+1] = rightNode;
        parent.used_slots++;
        rightNode.parentNode = parent;
        rightNode.leftSibling = node;
        rightNode.rightSibling = node.rightSibling;
        node.rightSibling = rightNode;

        if(index + 1 != bT.M-1)
            if(parent.childrenNode[index+2] != null)
                parent.childrenNode[index+2].leftSibling = rightNode;

        if(entry.key>midEntry.key) return rightNode;
        return node;
    }

    @Override
    void delete(int[] keys) throws Exception {
        Object[][] node_entry_pairs = this.get_node_entry_pairs(keys);
        for (Object[] node_entry : node_entry_pairs) {
            single_delete((BTree.Node) node_entry[0], (BTree.Entry) node_entry[1]);
        }
    }

    private void single_delete(BTree.Node e_node, BTree.Entry e) throws Exception {
        int index = e_node.getKeyIndex(e.key);

        if (e_node.isLeaf) {

            // DELETE KEY
            e_node.entries[index] = null;

            // FIX TREE IF NODE IS IMBALANCED
            if (e_node.used_slots - 1 < bT.min_keys) {
                rebalance_tree(e_node, index);
            } else {
                // LOCK NOT REQUIRED
                shift_entries_left(e_node);
                e_node.used_slots--;
            }
        } else {
            BTree.Node inorder = getInorderPredecessorNode(e_node, index);
            BTree.Entry backup = e_node.entries[index];
            if (inorder.used_slots > bT.min_keys) {
                e_node.entries[index] = inorder.entries[inorder.used_slots - 1];
                inorder.entries[inorder.used_slots-1] = backup;
            } else {
                inorder = getInorderSuccessorNode(e_node, index);
                e_node.entries[index] = inorder.entries[0];
                inorder.entries[0] = backup;
            }
            single_delete(inorder, backup);
        }

    }

    private void rebalance_tree(BTree.Node node, int entryIndex) throws Exception {
        if (!node.isRoot) {
            int node_pos = node.parentNode.getChildNodePosition(node);

            // LOCK NODE AND SIBLINGS
            if (node.leftSibling != null && node.leftSibling.used_slots > bT.min_keys) {
                take_entry_from_left(node, node_pos);
                // LOCK NODE AND SIBLINGS
            } else if (node.rightSibling != null && node.rightSibling.used_slots > bT.min_keys) {
                take_entry_from_right(node, node_pos);
            } else {
                int parent_entry_pos = node_pos;
                if (node.leftSibling != null) {
                    merge_with_left(node, node_pos, entryIndex);
                    parent_entry_pos = node_pos - 1;
                } else {
                    merge_with_right(node, node_pos, entryIndex);
                }
                if (node.parentNode != null) {
                    if (node.parentNode.used_slots - 1 < bT.min_keys && !node.parentNode.isRoot) {
                        rebalance_tree(node.parentNode, parent_entry_pos);
                    } else {
                        shift_entries_left(node.parentNode);
                        node.parentNode.used_slots--;
                    }
                }
            }
        }
    }

    private void take_entry_from_left(BTree.Node node, int node_pos) {

        // store replacement entries
        BTree.Entry new_node_entry = node.parentNode.entries[node_pos - 1];
        BTree.Entry new_parent_entry = node.leftSibling.entries[node.leftSibling.used_slots - 1];

        // remove entry from sibling, store rightChildNode, remove rightChildNode
        node.leftSibling.entries[node.leftSibling.used_slots - 1] = null;
        node.leftSibling.used_slots--;

        // set new entries and child node
        node.parentNode.entries[node_pos - 1] = new_parent_entry;
        shift_entries_right(node);
        node.entries[0] = new_node_entry;

        if (!node.leftSibling.isLeaf) {
            BTree.Node rightChild = node.leftSibling.childrenNode[node.leftSibling.used_slots];
            node.leftSibling.childrenNode[node.leftSibling.used_slots] = null;
            shift_children_right(node);
            node.childrenNode[0] = rightChild;
            rightChild.parentNode = node;
            rightChild.rightSibling = node.childrenNode[1];
            rightChild.leftSibling = null;
        }

    }

    private void take_entry_from_right(BTree.Node node, int node_pos) {

        // store replacement entries
        BTree.Entry new_node_entry = node.parentNode.entries[node_pos];
        BTree.Entry new_parent_entry = node.rightSibling.entries[0];

        // remove entry from sibling, store leftChildNode, remove leftChildNode
        node.rightSibling.entries[0] = null;
        shift_entries_left(node.rightSibling);
        node.rightSibling.used_slots--;

        // set new entries and child node
        node.parentNode.entries[node_pos] = new_parent_entry;
        shift_entries_left(node);
        node.entries[node.used_slots - 1] = new_node_entry;

        if (!node.rightSibling.isLeaf) {
            BTree.Node leftChild = node.rightSibling.childrenNode[0];
            node.rightSibling.childrenNode[0] = null;
            shift_children_left(node.rightSibling);
            node.childrenNode[node.used_slots + 1] = leftChild;
            leftChild.parentNode = node;
            leftChild.leftSibling = node.childrenNode[node.used_slots];
            leftChild.rightSibling = null;
        }

    }

    private void merge_with_left(BTree.Node node, int node_pos, int entry_index) throws Exception {

        BTree.Entry new_sibling_entry = node.parentNode.entries[node_pos - 1];
        node.parentNode.entries[node_pos - 1] = null;
        node.parentNode.childrenNode[node_pos] = null;
        shift_children_left(node.parentNode);

        // merge
        node.leftSibling.entries[node.leftSibling.used_slots] = new_sibling_entry;
        node.leftSibling.used_slots++;

        for (int i = 0; i < node.used_slots; i++) {
            if (!node.isLeaf) {
                node.leftSibling.childrenNode[node.leftSibling.used_slots + i] = node.childrenNode[i];
                node.leftSibling.childrenNode[node.leftSibling.used_slots + i - 1].rightSibling = node.childrenNode[i];
                node.childrenNode[i].leftSibling = node.leftSibling.childrenNode[node.leftSibling.used_slots + i - 1];
                node.childrenNode[i].parentNode = node.leftSibling;
            }
            if (i == entry_index) {
                continue;
            } else if (i < entry_index) {
                node.leftSibling.entries[node.leftSibling.used_slots + i] = node.entries[i];
            } else {
                node.leftSibling.entries[node.leftSibling.used_slots + i - 1] = node.entries[i];
            }
        }
        node.leftSibling.used_slots += node.used_slots - 1;

        node.leftSibling.rightSibling = node.rightSibling;
        if (node.parentNode.isRoot && node.parentNode.used_slots - 1 == 0) {
            node.leftSibling.isRoot = true;
            bT.rootNode = node.leftSibling;
            node.leftSibling.parentNode = null;
        }
    }

    private void merge_with_right(BTree.Node node, int node_pos, int entry_index) {
        BTree.Entry new_sibling_entry = node.parentNode.entries[node_pos];
        node.parentNode.entries[node_pos] = null;
        node.parentNode.childrenNode[node_pos + 1] = null;
        shift_children_left(node.parentNode);

        // merge
        shift_entries_left(node);
        node.entries[node.used_slots - 1] = new_sibling_entry;

        for (int i = 0; i < node.rightSibling.used_slots; i++) {
            node.entries[node.used_slots] = node.rightSibling.entries[i];

            if (!node.isLeaf) {
                node.childrenNode[node.used_slots] = node.rightSibling.childrenNode[i];
                node.childrenNode[node.used_slots - 1].rightSibling = node.rightSibling.childrenNode[i];
                node.rightSibling.childrenNode[i].leftSibling = node.childrenNode[node.used_slots - 1];
                node.rightSibling.childrenNode[i].parentNode = node;
            }
            node.used_slots++;
        }
        node.childrenNode[node.used_slots] = node.rightSibling.childrenNode[node.rightSibling.used_slots];

        node.rightSibling = node.rightSibling.rightSibling;
        if (node.parentNode.isRoot && node.parentNode.used_slots - 1 == 0) {
            node.isRoot = true;
            bT.rootNode = node;
            node.parentNode = null;
        }
    }

    private void shift_entries_right(BTree.Node node) {
        BTree.Entry previous = node.entries[0];
        node.entries[0] = null;
        for (int j = 0; j < node.used_slots; j++) {
            if (previous == null) {
                break;
            }
            BTree.Entry backup = node.entries[j + 1];
            node.entries[j + 1] = previous;
            previous = backup;
        }
    }

    private void shift_entries_left(BTree.Node node) {
        BTree.Entry previous = node.entries[node.used_slots - 1];
        node.entries[node.used_slots - 1] = null;
        for (int j = node.used_slots - 1; j > 0; j--) {
            if (previous == null) {
                break;
            }
            BTree.Entry backup = node.entries[j - 1];
            node.entries[j - 1] = previous;
            previous = backup;
        }
    }

    private void shift_children_right(BTree.Node node) {
        BTree.Node previous = node.childrenNode[0];
        node.childrenNode[0] = null;
        for (int j = 0; j < node.used_slots + 1; j++) {
            if (previous == null) {
                break;
            }
            BTree.Node backup = node.childrenNode[j + 1];
            node.childrenNode[j + 1] = previous;
            previous = backup;
        }
    }


    private void shift_children_left(BTree.Node node) {
        BTree.Node previous = node.childrenNode[node.used_slots];
        node.childrenNode[node.used_slots] = null;
        for (int j = node.used_slots; j > 0; j--) {
            if (previous == null) {
                break;
            }
            BTree.Node backup = node.childrenNode[j - 1];
            node.childrenNode[j - 1] = previous;
            previous = backup;
        }
    }

    private BTree.Node getInorderPredecessorNode(BTree.Node e_node, int entry_index) {
        BTree.Node current = e_node.childrenNode[entry_index];
        while (!current.isLeaf) {
            current = current.childrenNode[current.used_slots];
        }
        return current;
    }

    private BTree.Node getInorderSuccessorNode(BTree.Node e_node, int entry_index) {
        BTree.Node current = e_node.childrenNode[entry_index + 1];
        while (!current.isLeaf) {
            current = current.childrenNode[0];
        }
        return current;
    }
}
