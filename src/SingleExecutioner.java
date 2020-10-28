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

    private void single_delete(BTree.Entry e) throws Exception {
        BTree.Node e_node = e.selfNode;
        if(e_node.isLeaf){
            e_node.removeEntry(e);
            if(e_node.used_slots < bT.min_keys){
                int e_pos = e_node.parentNode.getChildNodePosition(e_node);
                if(e_node.leftSibling != null && e_node.leftSibling.used_slots > bT.min_keys){
                    e_node.addEntryAtIndex(e_node.parentNode.entries[e_pos-1], 0);
                    e_node.parentNode.addEntryAtIndex(e_node.leftSibling.pollLastEntry(), e_pos-1);
                }
                else if (e_node.rightSibling != null && e_node.rightSibling.used_slots > bT.min_keys){
                    e_node.addEntryAtIndex(e_node.parentNode.entries[e_pos], e_node.used_slots);
                    e_node.parentNode.addEntryAtIndex(e_node.rightSibling.pollFirstEntry(), e_pos);
                }
                else{
                    if(e_node.leftSibling != null){
                        e_node.leftSibling.addEntryAtIndex(e_node.parentNode.entries[e_pos-1], e_node.leftSibling.used_slots);
                        for(int i = 0; i < e_node.used_slots; i++){
                            e_node.leftSibling.addEntryAtIndex(e_node.entries[i], e_node.leftSibling.used_slots);
                        }
                        single_delete(e_node.parentNode.entries[e_pos-1]);
                        removeNode(e_node, e_pos);
                    }
                    else if(e_node.rightSibling != null){
                        e_node.rightSibling.addEntryAtIndex(e_node.parentNode.entries[e_pos], 0);
                        for(int i = 0; i < e_node.used_slots; i++){
                            e_node.rightSibling.addEntryAtIndex(e_node.entries[i], i);
                        }
                        single_delete(e_node.parentNode.entries[e_pos]);
                        removeNode(e_node, e_pos);
                    }
                }
            }
        }
    }

    private void merge_nodes(BTree.Node leftNode, BTree.Node rightNode){

    }

     private void removeNode(BTree.Node node, int position){
         node.parentNode.childrenNode[position] = null;
         node.leftSibling.rightSibling = node.rightSibling;
         node.rightSibling.leftSibling = node.leftSibling;
         for (int j = position + 1; j < bT.M; j++) {
             node.parentNode.childrenNode[j - 1] = node.parentNode.childrenNode[j];
             if(node.parentNode.childrenNode[j] == null) {
                 break;
             }
         }
     }
}
