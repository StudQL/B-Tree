class SingleExecutioner extends AbstractExecutioner {

    SingleExecutioner(BTree bTree) {
        super(bTree);
    }

    @Override
    BTree.Entry[] get(int[] keys) {
        return null;
    }

    @Override
    void insert(BTree.Entry[] entries) {

    }

    @Override
    void delete(int[] keys) throws Exception {
        BTree.Entry[] entries = this.get(keys);
        for (BTree.Entry e : entries) {
            single_delete(e);
        }
    }

    private void single_delete(BTree.Entry e) throws Exception {
        BTree.Node e_node = e.selfNode;
        if (e_node.isLeaf) {

            // DELETE KEY
            int index = e_node.getKeyIndex(e.key);
            e_node.entries[index] = null;
            e_node.used_slots--;

            // FIX TREE IF NODE IS IMBALANCED
            if (e_node.used_slots < bT.min_keys) {
                rebalance_tree(e_node, index);
            }
            else{
                shift_entries_left(e_node,index);
            }
        }
        else{
            // GET INORDER PREDECESSOR
            // IF INORDER PREDECESSOR NODE HAS MIN KEYS, GET INORDER SUCCESSOR
            // REPLACE ENTRY WITH INORDER
            // DELETE INORDER
        }

    }

    private void rebalance_tree(BTree.Node node, int entryIndex) throws Exception {
        int node_pos = node.parentNode.getChildNodePosition(node);
        if (node.leftSibling != null && node.leftSibling.used_slots > bT.min_keys) {
            take_entry_from_left(node, node_pos, entryIndex);
        } else if (node.rightSibling != null && node.rightSibling.used_slots > bT.min_keys) {
            take_entry_from_right(node, node_pos, entryIndex);
        } else {
            merge_with_left(node, node_pos);
            if(node.parentNode.used_slots < bT.min_keys){
                rebalance_tree(node.parentNode, );
            }
        }
    }

    private void take_entry_from_left(BTree.Node node, int node_pos, int entryIndex) {

        // store replacement entries
        BTree.Entry new_node_entry = node.parentNode.entries[node_pos-1];
        BTree.Entry new_parent_entry = node.leftSibling.entries[node.leftSibling.used_slots-1];

        // remove entry from sibling, store rightChildNode, remove rightChildNode
        node.leftSibling.entries[node.leftSibling.used_slots-1] = null;
        BTree.Node rightChild = node.leftSibling.childrenNode[node.leftSibling.used_slots];
        node.leftSibling.childrenNode[node.leftSibling.used_slots] = null;
        node.leftSibling.used_slots--;

        // set new entries and child node
        node.parentNode.entries[node_pos-1] = new_parent_entry;
        shift_entries_right(node, entryIndex);
        node.entries[0] = new_node_entry;
        shift_children_right(node, node.used_slots+1);
        node.childrenNode[0] = rightChild;
        rightChild.parentNode = node;
        rightChild.rightSibling = node.childrenNode[1];

    }

    private void take_entry_from_right(BTree.Node node, int node_pos, int entryIndex) {

        // store replacement entries
        BTree.Entry new_node_entry = node.parentNode.entries[node_pos];
        BTree.Entry new_parent_entry = node.rightSibling.entries[0];

        // remove entry from sibling, store rightChildNode, remove rightChildNode
        node.rightSibling.entries[0] = null;
        BTree.Node leftChild = node.rightSibling.childrenNode[0];
        node.rightSibling.childrenNode[0] = null;
        node.rightSibling.used_slots--;

        // set new entries and child node
        node.parentNode.entries[node_pos] = new_parent_entry;
        shift_entries_left(node, entryIndex);
        node.entries[node.used_slots] = new_node_entry;
        node.childrenNode[node.used_slots+1] = leftChild;
        leftChild.parentNode = node;
        leftChild.leftSibling = node.childrenNode[node.used_slots];
    }

    private void merge_with_left(BTree.Node node, int node_pos) {

    }

    private void remove_entry_at_index(int index, BTree.Node e_node) {
        e_node.entries[index] = null;
        e_node.used_slots--;
        for (int j = index + 1; j <= e_node.used_slots; j++) {
            e_node.entries[j - 1] = e_node.entries[j];
            if (j == e_node.used_slots) {
                e_node.entries[j] = null;
            }
        }
    }

    private void remove_entry(int key, BTree.Node e_node) throws Exception {
        int i = e_node.getKeyIndex(key);
        if (i == -1) {
            throw new Exception("Key not found");
        }
        remove_entry_at_index(i, e_node);
    }

    private void remove_entry(BTree.Entry e, BTree.Node e_node) throws Exception {
        remove_entry(e.key,e_node);
    }

    private void shift_entries_right(BTree.Node node, int index){
        for (int j = index - 1; j >= 0; j--) {
            node.entries[j + 1] = node.entries[j];
        }
        node.entries[0] = null;
    }

    private void shift_entries_left(BTree.Node node, int index){
        for (int j = index + 1; j <= node.used_slots; j++) {
            node.entries[j - 1] = node.entries[j];
        }
        node.entries[node.used_slots] = null;
    }

    private void shift_children_right(BTree.Node node, int index){
        for (int j = index - 1; j >= 0; j--) {
            node.childrenNode[j + 1] = node.childrenNode[j];
        }
        node.childrenNode[0] = null;
    }

    private void shift_children_left(BTree.Node node, int index){
        for (int j = index + 1; j <= node.used_slots; j++) {
            node.childrenNode[j - 1] = node.childrenNode[j];
        }
        node.childrenNode[node.used_slots] = null;
    }


//    private void single_delete(BTree.Entry e) throws Exception {
//        BTree.Node e_node = e.selfNode;
//        if(!e_node.isLeaf){
//            BTree.Node leftChild = e.getLeftChild();
//            BTree.Node rightChild = e.getRightChild();
//            if(leftChild.used_slots > bT.min_keys){
//                BTree.Entry in_pre = e.getInorderPredecessor();
//
//
//            }
//            else if(rightChild.used_slots > bT.min_keys){
//                BTree.Entry in_suc = e.getInorderSuccessor();
//
//            }
//            else{
//
//            }
//        }
//    }

//    private void single_delete(BTree.Entry e) throws Exception {
//        BTree.Node e_node = e.selfNode;
//        if(e_node.isLeaf){
//            e_node.removeEntry(e);
//            if(e_node.used_slots < bT.min_keys){
//                int e_pos = e_node.parentNode.getChildNodePosition(e_node);
//                if(e_node.leftSibling != null && e_node.leftSibling.used_slots > bT.min_keys){
//                    e_node.addEntryAtIndex(e_node.parentNode.entries[e_pos-1], 0);
//                    e_node.parentNode.addEntryAtIndex(e_node.leftSibling.pollLastEntry(), e_pos-1);
//                }
//                else if (e_node.rightSibling != null && e_node.rightSibling.used_slots > bT.min_keys){
//                    e_node.addEntryAtIndex(e_node.parentNode.entries[e_pos], e_node.used_slots);
//                    e_node.parentNode.addEntryAtIndex(e_node.rightSibling.pollFirstEntry(), e_pos);
//                }
//                else{
//                    if(e_node.leftSibling != null){
//                        e_node.leftSibling.addEntryAtIndex(e_node.parentNode.entries[e_pos-1], e_node.leftSibling.used_slots);
//                        for(int i = 0; i < e_node.used_slots; i++){
//                            e_node.leftSibling.addEntryAtIndex(e_node.entries[i], e_node.leftSibling.used_slots);
//                        }
//                        single_delete(e_node.parentNode.entries[e_pos-1]);
//                        removeNode(e_node, e_pos);
//                    }
//                    else if(e_node.rightSibling != null){
//                        e_node.rightSibling.addEntryAtIndex(e_node.parentNode.entries[e_pos], 0);
//                        for(int i = 0; i < e_node.used_slots; i++){
//                            e_node.rightSibling.addEntryAtIndex(e_node.entries[i], i);
//                        }
//                        single_delete(e_node.parentNode.entries[e_pos]);
//                        removeNode(e_node, e_pos);
//                    }
//                }
//            }
//        }
//    }
//
//    private void merge_nodes(BTree.Node leftNode, BTree.Node rightNode){
//
//    }
//
//     private void removeNode(BTree.Node node, int position){
//         node.parentNode.childrenNode[position] = null;
//         node.leftSibling.rightSibling = node.rightSibling;
//         node.rightSibling.leftSibling = node.leftSibling;
//         for (int j = position + 1; j < bT.M; j++) {
//             node.parentNode.childrenNode[j - 1] = node.parentNode.childrenNode[j];
//             if(node.parentNode.childrenNode[j] == null) {
//                 break;
//             }
//         }
//     }
}
