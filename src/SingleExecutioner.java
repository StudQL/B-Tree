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
        for (BTree.Entry e : entries){
            single_delete(e);
        }
    }

    private void single_delete(BTree.Entry e) throws Exception {
        BTree.Node e_node = e.selfNode;
        if(e_node.isLeaf){
            e_node.removeEntry(e);
            if(e_node.used_slots < bT.min_keys){
                if(e_node.leftSibling.used_slots > bT.min_keys){

                }
                else if (e_node.rightSibling.used_slots > bT.min_keys){

                }
                else{

                }
            }
        }
    }
}
