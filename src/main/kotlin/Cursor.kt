package io.sqlitek

import io.sqlitek.btree.NodeType
import io.sqlitek.btree.getInternalNodeChild
import io.sqlitek.btree.getInternalNodeKey
import io.sqlitek.btree.getInternalNodeNumKeys
import io.sqlitek.btree.getLeafNodeKey
import io.sqlitek.btree.getLeafNodeNumCells
import io.sqlitek.btree.getNodeType

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
    // Even if key 0 does not exist in the table,
    // this method will return the position of the lowest id (the start of the left-most leaf node).
    val cursor = find(table, 0)
    val node = table.pager.getPage(cursor.pageNumber)
    val numCells = getLeafNodeNumCells(node)
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
    return findInternalNode(table, rootPageNumber, key)
}

// This function performs binary search to find the child that should contain the given key.
// Note that the key to the right of each child pointer is the maximum key contained by that child.
fun findInternalNode(table: Table, pageNum: Int, key: Int): Cursor {
    val node = table.pager.getPage(pageNum)
    val numKeys = getInternalNodeNumKeys(node)
    /* Binary search to find index of child to search */
    var min = 0
    var max = numKeys
    while (min != max) {
        val index = (min + max) / 2 //TODO: guard against overflow
        val rightmost = getInternalNodeKey(node, index)
        if (rightmost >= key) {
            max = index
        } else {
            min = index + 1
        }
    }
    val childNum = getInternalNodeChild(node, min)
    val child = table.pager.getPage(childNum)
    return when (getNodeType(child)) {
        NodeType.INTERNAL -> findInternalNode(table, childNum, key)
        NodeType.LEAF -> findLeafNode(table, childNum, key)
    }
}

// This will either return:
// - the position of the key,
// - the position of another key that we’ll need to move if we want to insert the new key, or
// - the position one past the last key
fun findLeafNode(table: Table, pageNum: Int, key: Int): Cursor {
    val node = table.pager.getPage(pageNum)
    val numberOfCells = getLeafNodeNumCells(node)
    // Binary search:
    var min = 0
    var onePastMaxIndex = numberOfCells
    while (onePastMaxIndex != min) {
        val index = (min + onePastMaxIndex) / 2 //TODO: guard against overflow
        val keyAtIndex = getLeafNodeKey(node, index)
        if (key == keyAtIndex) {
            return Cursor(table, pageNum, index, false)
        }
        if (key < keyAtIndex) {
            onePastMaxIndex = index
        } else {
            min = index + 1
        }
    }
    return Cursor(table, pageNum, min, false)
}