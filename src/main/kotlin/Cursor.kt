package io.sqlitek

class Cursor(
    val table: Table,
    var pageNumber: Int,
    var cellNumber: Int,
    // Indicates a position one past the last element.
    var endOfTable: Boolean
) {
    val pager = table.pager
}

fun tableStart(table: Table): Cursor {
    val pageNumber = table.rootPageNumber
    val root = table.pager.getPage(pageNumber)
    val numCells = getLeafNodeNumCells(root)
    return Cursor(table, 0, numCells, numCells == 0)
}

/*
 * Returns the position of the given key.
 * If the key is not present, returns the position where it should be inserted.
 */
fun find(table: Table, key: Int): Cursor {
    val rootPageNumber = table.rootPageNumber
    val rootNode = table.pager.getPage(rootPageNumber)
    if (getNodeType(rootNode) == NodeType.LEAF) {
        return findLeafNode(table, rootPageNumber, key)
    }
    error("Need to implement searching an internal node")
}

// This will either return:
// - the position of the key,
// - the position of another key that we’ll need to move if we want to insert the new key, or
// - the position one past the last key
fun findLeafNode(table: Table, pageNum: Int, key: Int): Cursor {
    val node = table.pager.getPage(pageNum)
    val numberOfCells = getLeafNodeNumCells(node)
    // Binary search:
    var minIndex = 0
    var onePastMaxIndex = numberOfCells
    while (onePastMaxIndex != minIndex) {
        val index = (minIndex + onePastMaxIndex) / 2 //TODO: guard against overflow
        val keyAtIndex = getLeafNodeKey(node, index)
        if (key == keyAtIndex) {
            return Cursor(table, pageNum, index, false)
        }
        if (key < keyAtIndex) {
            onePastMaxIndex = index
        } else {
            minIndex = index + 1
        }
    }
    return Cursor(table, pageNum, minIndex, false)
}