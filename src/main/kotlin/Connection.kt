package io.sqlitek

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
    }
    return Table(pager, 0)
}