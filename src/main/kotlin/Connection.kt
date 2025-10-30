package io.sqlitek

import io.sqlitek.btree.Table
import io.sqlitek.btree.initializeLeafNode
import io.sqlitek.btree.openPager
import io.sqlitek.btree.setNodeRoot

// By opening a connection, we mean:
// - opening the database file
// - initializing a pager data structure
// - initializing a table data structure
fun createConnection(filename: String): Table {
    val pager = openPager(filename)
    val numberOfPages = pager.numberOfPages
    if (numberOfPages == 0) {
        // New database file. Initialize page 0 as leaf node.
        val root = pager.getPage(0)
        initializeLeafNode(root)
        setNodeRoot(root, true)
    }
    return Table(pager, 0)
}