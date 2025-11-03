package io.sqlitek.btree

import io.sqlitek.Row
import io.sqlitek.RowLayout.ROW_SIZE
import io.sqlitek.copyInto
import io.sqlitek.moveInto
import io.sqlitek.serialize
import java.nio.ByteBuffer

/*
 * Common Node Header Layout
 */

const val NODE_TYPE_SIZE = UByte.SIZE_BYTES         // 1
const val NODE_TYPE_OFFSET = 0

const val IS_ROOT_SIZE = UByte.SIZE_BYTES           // 1
const val IS_ROOT_OFFSET = NODE_TYPE_OFFSET + NODE_TYPE_SIZE

fun isRoot(node: ByteBuffer): Boolean {
    return node.get(IS_ROOT_OFFSET) != 0.toByte()
}

fun setNodeRoot(node: ByteBuffer, isRoot: Boolean) {
    node.put(IS_ROOT_OFFSET, if (isRoot) 1 else 0)
}

const val PARENT_POINTER_SIZE = UInt.SIZE_BYTES     // 4
const val PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE

const val COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

// -------------------------------- leaf node --------------------------------

/*
 * Leaf Node Header Layout
 */

const val LEAF_NODE_NUM_CELLS_SIZE = UInt.SIZE_BYTES            // 4
const val LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE

const val LEAF_NODE_NEXT_LEAF_SIZE = UInt.SIZE_BYTES
const val LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE

const val LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE

/*
 * Leaf Node Body Layout
 */

const val LEAF_NODE_KEY_SIZE = Int.SIZE_BYTES                // 4
const val LEAF_NODE_KEY_OFFSET = 0

const val LEAF_NODE_VALUE_SIZE = ROW_SIZE
const val LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE

const val LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE

const val LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE

const val LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE


/*
 * To keep the tree balanced, we evenly distribute cells between the two new nodes.
 * If a leaf node can hold N cells, then during a split we need to distribute N+1 cells between two nodes (N original cells plus one new one).
 * I’m arbitrarily choosing the left node to get one more cell if N+1 is odd.
 */
const val LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2 // floors-up
const val LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT


fun getLeafNodeNumCells(node: ByteBuffer): Int {
    // Read Int at the offset where num_cells is stored.
    return node.getInt(LEAF_NODE_NUM_CELLS_OFFSET)
}

fun setLeafNodeNumCells(node: ByteBuffer, value: Int) {
    node.putInt(LEAF_NODE_NUM_CELLS_OFFSET, value)
}

fun getLeafNodeCellOffset(cellNumber: Int): Int {
    return LEAF_NODE_HEADER_SIZE + cellNumber * LEAF_NODE_CELL_SIZE
}

fun getLeafNodeKey(node: ByteBuffer, cellNumber: Int): Int {
    val offset = getLeafNodeCellOffset(cellNumber)
    return node.getInt(offset) // read 4-byte key
}

fun setLeafNodeKey(node: ByteBuffer, cellNumber: Int, key: Int) {
    val offset = getLeafNodeCellOffset(cellNumber)
    node.putInt(offset, key)
}

fun leafNodeValueOffset(cellNum: Int): Int {
    return getLeafNodeCellOffset(cellNum) + LEAF_NODE_KEY_SIZE
}

fun getLeafNodeValue(node: ByteBuffer, cellNumber: Int): ByteBuffer {
    val offset = leafNodeValueOffset(cellNumber)
    val value = ByteArray(LEAF_NODE_VALUE_SIZE)
    node.position(offset)
    node.get(value)
    return ByteBuffer.wrap(value)
}

fun setLeafNodeValue(node: ByteBuffer, cellNum: Int, value: ByteArray) {
    require(value.size == LEAF_NODE_VALUE_SIZE) {
        "Value must be exactly $LEAF_NODE_VALUE_SIZE bytes"
    }
    val offset = leafNodeValueOffset(cellNum)
    node.position(offset)
    node.put(value)
}

fun initializeLeafNode(node: ByteBuffer) {
    setNodeType(node, NodeType.LEAF)
    setNodeRoot(node, false)
    setLeafNodeNumCells(node, 0)
    setLeafNodeNextLeaf(node, 0) // 0 represents no sibling
}

fun getNodeType(node: ByteBuffer): NodeType {
    val value = node.get(NODE_TYPE_OFFSET)
    return NodeType.from(value)
}

fun setNodeType(node: ByteBuffer, type: NodeType) {
    node.put(NODE_TYPE_OFFSET, type.value)
}

fun getLeafNodeNextLeaf(node: ByteBuffer): Int {
    return node.getInt(LEAF_NODE_NEXT_LEAF_OFFSET)
}

fun setLeafNodeNextLeaf(node: ByteBuffer, value: Int) {
    node.putInt(LEAF_NODE_NEXT_LEAF_OFFSET, value)
}

fun leafNodeToStringDebug(node: ByteBuffer): String {
    return buildString {
        val num = getLeafNodeNumCells(node)
        append("leaf (size:$num)\n")
        for (i in 0..<num) {
            val key = getLeafNodeKey(node, i)
            append(" - $i : $key \n")
        }
    }
}

fun printLeafNode(node: ByteBuffer) {
    println(leafNodeToStringDebug(node))
}

// -------------------------------- internal node --------------------------------

/*
 * Internal Node Header Layout
 */

const val INTERNAL_NODE_NUM_KEYS_SIZE = Int.SIZE_BYTES
const val INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE

// The page number of its rightmost child.
const val INTERNAL_NODE_RIGHT_CHILD_SIZE = Int.SIZE_BYTES
const val INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
const val INTERNAL_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE

/*
 * Internal Node Body Layout.
 * The body is an array of cells where each cell contains a child pointer and a key.
 * Every key should be the maximum key contained in the child to its left.
 */

const val INTERNAL_NODE_KEY_SIZE = Int.SIZE_BYTES
const val INTERNAL_NODE_CHILD_SIZE = Int.SIZE_BYTES
const val INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE

// Keep this small for testing.
const val INTERNAL_NODE_MAX_KEYS = 3

fun getInternalNodeNumKeys(node: ByteBuffer): Int {
    return node.getInt(INTERNAL_NODE_NUM_KEYS_OFFSET)
}

fun setInternalNodeNumKeys(node: ByteBuffer, value: Int) {
    node.putInt(INTERNAL_NODE_NUM_KEYS_OFFSET, value)
}

fun getInternalNodeRightChild(node: ByteBuffer): Int {
    return node.getInt(INTERNAL_NODE_RIGHT_CHILD_OFFSET)
}

fun setInternalNodeRightChild(node: ByteBuffer, value: Int) {
    node.putInt(INTERNAL_NODE_RIGHT_CHILD_OFFSET, value)
}

fun getInternalNodeCellOffset(cellNum: Int): Int {
    return INTERNAL_NODE_HEADER_SIZE + cellNum * INTERNAL_NODE_CELL_SIZE
}

fun getInternalNodeChild(node: ByteBuffer, childNum: Int): Int {
    val numberOfKeys = getInternalNodeNumKeys(node)
    return when {
        childNum > numberOfKeys -> error("Tried to access childNum:$childNum > num_keys:$numberOfKeys")
        childNum == numberOfKeys -> {
            val rightChild = getInternalNodeRightChild(node)
            if (rightChild == INVALID_PAGE_NUMBER) {
                error("Tried to access right child of node:$node but was empty")
            }
            rightChild
        }

        else -> {
            val offset = getInternalNodeCellOffset(childNum)
            val child = node.getInt(offset)
            if (child == INVALID_PAGE_NUMBER) {
                error("Tried to access child of node:$node but was empty")
            }
            child
        }
    }
}

fun setInternalNodeChild(node: ByteBuffer, childNum: Int, value: Int) {
    val numberOfKeys = getInternalNodeNumKeys(node)
    when {
        childNum > numberOfKeys -> error("Tried to access childNum $childNum > numKeys $numberOfKeys")
        childNum == numberOfKeys -> setInternalNodeRightChild(node, value)
        else -> {
            val offset = getInternalNodeCellOffset(childNum)
            node.putInt(offset, value)
        }
    }
}

fun getInternalNodeKey(node: ByteBuffer, keyNum: Int): Int {
    return getInternalNodeCellOffset(keyNum) + INTERNAL_NODE_CHILD_SIZE
}

fun setInternalNodeKey(node: ByteBuffer, keyNum: Int, value: Int) {
    val cellOffset = getInternalNodeCellOffset(keyNum)
    node.putInt(cellOffset + INTERNAL_NODE_CHILD_SIZE, value)
}

fun getNodeMaxKey(node: ByteBuffer): Int {
    return when (getNodeType(node)) {
        NodeType.INTERNAL -> {
            val numberOfKeys = getInternalNodeNumKeys(node)
            getInternalNodeKey(node, numberOfKeys - 1)
        }

        NodeType.LEAF -> {
            val numberOfCells = getLeafNodeNumCells(node)
            getLeafNodeKey(node, numberOfCells - 1)
        }
    }
}

tailrec fun getNodeMaxKey(table: Table, node: ByteBuffer): Int {
    if (getNodeType(node) == NodeType.LEAF) {
        val cellsNumber = getLeafNodeNumCells(node)
        return getLeafNodeKey(node, cellsNumber - 1)
    }
    val nodePtr = getInternalNodeRightChild(node)
    val rightChild = table.getPage(nodePtr)
    return getNodeMaxKey(table, rightChild)
}

fun initializeInternalNode(node: ByteBuffer) {
    setNodeType(node, NodeType.INTERNAL)
    setNodeRoot(node, false)
    setLeafNodeNumCells(node, 0)
    /*
      Necessary because the root page number is 0; by not initializing an internal
      node's right child to an invalid page number when initializing the node, we may
      end up with 0 as the node's right child, which makes the node a parent of the root
    */
    setInternalNodeRightChild(node, INVALID_PAGE_NUMBER)
}


// -------------------------------- btree --------------------------------

fun toStringBtree(table: Table, pageNumber: Int, indentationLevel: Int): String {
    val node = table.getPage(pageNumber)
    val builder = StringBuilder()
    when (getNodeType(node)) {
        NodeType.INTERNAL -> {
            val numberOfKeys = getInternalNodeNumKeys(node)
            builder.indent(indentationLevel)
            builder.append("- internal (size $numberOfKeys)\n")
            if (numberOfKeys > 0) {
                for (i in 0..<numberOfKeys) {
                    //builder.indent(indentationLevel + 1)
                    val childPtr = getInternalNodeChild(node, i)
                    toStringBtree(table, childPtr, indentationLevel + 1)
                        .also { builder.append(it) }
                    builder.indent(indentationLevel + 1)
                    val key = getInternalNodeKey(node, i)
                    builder.append("- key $key\n")
                }
                val childPtr = getInternalNodeRightChild(node)
                toStringBtree(table, childPtr, indentationLevel + 1).also { builder.append(it) }
            }
        }

        NodeType.LEAF -> {
            val numberOfKeys = getLeafNodeNumCells(node)
            builder.indent(indentationLevel)
            builder.append("- leaf (size $numberOfKeys)\n")
            for (i in 0..<numberOfKeys) {
                builder.indent(indentationLevel + 1)
                val key = getLeafNodeKey(node, i)
                builder.append("- $key\n")
            }
        }
    }
    return builder.toString()
}

fun StringBuilder.indent(level: Int) {
    repeat(level) { append("   ") }
}


fun printTree(table: Table, pageNum: Int, indentationLevel: Int) {
    println(toStringBtree(table, pageNum, indentationLevel))
}


// Each node will correspond to one page.
// Internal nodes will point to their children by storing the page number that stores the child.
// The btree asks the pager for a particular page number and gets back a pointer into the page cache.
// Pages are stored in the database file one after the other in order of page number.
//
// Nodes need to store some metadata in a header at the beginning of the page.
// Every node will store what type of node it is, whether or not it is the root node,
// and a pointer to its parent (to allow finding a node’s siblings).
// I define constants for the size and offset of every header field:
class Btree

/**
 * Let N be the root node. First allocate two nodes, say L and R. Move lower half of N into L and the upper half into R.
 * Now N is empty. Add 〈L, K,R〉 in N, where K is the max key in L. Page N remains the root.
 * Note that the depth of the tree has increased by one, but the new tree remains height balanced without violating any B+-tree property.
 */
private fun createNewRoot(table: Table, rightChildPageNum: Int) {
    /*
     Handle splitting the root.
     Old root copied to new page, becomes left child.
     Address of right child passed in.
     Re-initialize root page to contain the new root node.
     New root node points to two children.
   */
    val root = table.getRootPage()
    val leftChildPageNumber = table.getUnsuedPageNumber()
    val rightChild = table.getPage(rightChildPageNum)
    val leftChild = table.getPage(leftChildPageNumber)
    if (getNodeType(root) == NodeType.INTERNAL) {
        initializeInternalNode(rightChild)
        initializeInternalNode(leftChild)
    }
    // The old root is copied to the left child so we can reuse the root page.
    root.copyInto(leftChild, length = PAGE_SIZE)
    setNodeRoot(leftChild, false)
    if (getNodeType(leftChild) == NodeType.INTERNAL) {
        var child: ByteBuffer
        val numKeys = getInternalNodeNumKeys(leftChild)
        for (i in 0..<numKeys) {
            val node = getInternalNodeChild(leftChild, i)
            child = table.getPage(node)
            setParentNode(child, leftChildPageNumber)
        }
        val node = getInternalNodeRightChild(leftChild)
        child = table.getPage(node)
        setParentNode(child, leftChildPageNumber)
    }
    // Finally, we initialize the root page as a new internal node with two children.
    initializeInternalNode(root)
    setNodeRoot(root, true)
    setInternalNodeNumKeys(root, 1)
    setInternalNodeChild(root, 0, leftChildPageNumber)
    val leftChildMaxKey = getNodeMaxKey(table, leftChild)
    setInternalNodeKey(root, 0, leftChildMaxKey)
    setInternalNodeRightChild(root, rightChildPageNum)
    val rootPageNumber = table.rootPageNumber
    setParentNode(leftChild, rootPageNumber)
    setParentNode(rightChild, rootPageNumber)
}

// -------------------------------- leaf node --------------------------------

fun leafNodeInsert(cursor: Cursor, key: Int, value: Row) {
    val node = cursor.table.getPage(cursor.pageNumber)
    val numCells = getLeafNodeNumCells(node)
    if (numCells >= LEAF_NODE_MAX_CELLS) {
        // Node is full.
        leafNodeSplitAndInsert(cursor, key, value)
        return
    }
    val cellNumber = cursor.cellNumber
    if (cellNumber < numCells) {
        // Shift cells to make room.
        for (i in numCells downTo cellNumber + 1) {
            val destOffset = getLeafNodeCellOffset(i)
            val srcOffset = getLeafNodeCellOffset(i - 1)
            val temp = ByteArray(LEAF_NODE_CELL_SIZE)
            node.position(srcOffset)
            node.get(temp)
            node.position(destOffset)
            node.put(temp)
        }
    }
    setLeafNodeNumCells(node, numCells + 1)
    setLeafNodeKey(node, cellNumber, key)
    val serialized = serialize(value)
    setLeafNodeValue(node, cellNumber, serialized)
}

/**
 * If there is no space on the leaf node, we would split the existing entries residing there and the new one (being inserted)
 * into two equal halves: lower and upper halves (Keys on the upper half are strictly greater than those on the lower half).
 * We allocate a new leaf node and move the upper half into the new node.
 */
private fun leafNodeSplitAndInsert(cursor: Cursor, key: Int, value: Row) {
    /*
     Create a new node and move half the cells over.
     Insert the new value in one of the two nodes.
     Update parent or create a new parent.
    */
    val table = cursor.table
    val oldNode = table.getPage(cursor.pageNumber)
    val oldMax = getNodeMaxKey(oldNode)
    val newPageNumber = table.getUnsuedPageNumber()
    val newNode = table.getPage(newPageNumber)
    initializeLeafNode(newNode)
    val oldParentPage = getParentNode(oldNode)
    setParentNode(oldNode, oldParentPage)
    val oldNext = getLeafNodeNextLeaf(oldNode)
    setLeafNodeNextLeaf(newNode, oldNext)
    setLeafNodeNextLeaf(oldNode, newPageNumber)
    /*
     All existing keys plus new key should be divided
     evenly between old (left) and new (right) nodes.
     Starting from the right, move each key to correct position.
    */
    val cellNum = cursor.cellNumber
    for (i in LEAF_NODE_MAX_CELLS downTo 0) {
        val destinationNode = if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) newNode else oldNode
        val indexWithinNode = i % LEAF_NODE_LEFT_SPLIT_COUNT
        val destOffset = getLeafNodeCellOffset(i)
        when {
            // Place the brand-new cell.
            i == cellNum -> {
                setLeafNodeKey(destinationNode, indexWithinNode, key)
                val serialized = serialize(value)
                setLeafNodeValue(destinationNode, indexWithinNode, serialized)
            }

            // Shift from old position (i - 1) into destination.
            i > cellNum -> {
                val srcOffset = getLeafNodeCellOffset(i - 1)
                moveCell(oldNode, srcOffset, destinationNode, destOffset)
            }

            // i < cursor.cellNum -> copy from same i.
            else -> {
                val srcOffset = getLeafNodeCellOffset(i)
                moveCell(oldNode, srcOffset, destinationNode, destOffset)
            }
        }
    }
    // Update cell counts in each node’s header:
    setLeafNodeNumCells(oldNode, LEAF_NODE_LEFT_SPLIT_COUNT)
    setLeafNodeNumCells(newNode, LEAF_NODE_RIGHT_SPLIT_COUNT)
    table.numberOfPages = maxOf(table.numberOfPages, newPageNumber + 1)
    if (isRoot(oldNode)) {
        createNewRoot(table, newPageNumber)
    } else {
        val parentPageNumber = getParentNode(oldNode)
        val newMax = getNodeMaxKey(oldNode)
        val parent = table.getPage(parentPageNumber)
        updateInternalNodeKey(parent, oldMax, newMax)
        internalNodeInsert(table, parentPageNumber, newPageNumber)
    }
}

// -------------------------------- internal node --------------------------------

fun getParentNode(node: ByteBuffer): Int {
    return node.getInt(PARENT_POINTER_OFFSET)
}

fun setParentNode(node: ByteBuffer, pageNumber: Int) {
    node.putInt(PARENT_POINTER_OFFSET, pageNumber)
}

fun updateInternalNodeKey(node: ByteBuffer, oldKey: Int, newKey: Int) {
    val childIndex = findInternalNodeChildIndex(node, oldKey)
    setInternalNodeKey(node, childIndex, newKey)
}

fun internalNodeInsert(table: Table, parentPageNumber: Int, childPageNumber: Int) {
    // Adds a new child/key pair to parent that corresponds to child.
    val parent = table.getPage(parentPageNumber)
    val child = table.getPage(childPageNumber)
    val childMaxKey = getNodeMaxKey(table, child)
    val index = findInternalNodeChildIndex(parent, childMaxKey)
    val parentNumKeys = getInternalNodeNumKeys(parent)
    if (parentNumKeys >= INTERNAL_NODE_MAX_KEYS) {
        internalNodeSplitAndInsert(table, parentPageNumber, childPageNumber)
        return
    }
    val rightChildPageNumber = getInternalNodeRightChild(parent)
    // An internal node with a right child of INVALID_PAGE_NUM is empty.
    if (rightChildPageNumber == INVALID_PAGE_NUMBER) {
        setInternalNodeRightChild(parent, childPageNumber)
        return
    }
    val rightChild = table.getPage(rightChildPageNumber)
    /*
       If we are already at the max number of cells for a node, we cannot increment
       before splitting. Incrementing without inserting a new key/child pair
       and immediately calling internal_node_split_and_insert has the effect
       of creating a new key at (max_cells + 1) with an uninitialized value
     */
    setInternalNodeNumKeys(parent, parentNumKeys + 1)
    val rightChildMaxKey = getNodeMaxKey(table, rightChild)
    if (childMaxKey > rightChildMaxKey) {
        // Replace right child.
        setInternalNodeChild(parent, parentNumKeys, rightChildPageNumber)
        setInternalNodeKey(parent, parentNumKeys, rightChildMaxKey)
        setInternalNodeRightChild(parent, childPageNumber)
    } else {
        // Make room for new cell.
        for (i in parentNumKeys downTo index + 1) {
            val destOffset = getInternalNodeCellOffset(i)
            val srcOffset = getInternalNodeCellOffset(i - 1)
            parent.copyInto(destOffset, srcOffset, INTERNAL_NODE_CELL_SIZE)
        }
        setInternalNodeChild(parent, index, childPageNumber)
        setInternalNodeKey(parent, index, childMaxKey)
    }
}

//TODO: check correctness
fun internalNodeSplitAndInsert(table: Table, parentPageNumber: Int, childPageNumber: Int) {
    var oldPageNumber = parentPageNumber
    var oldNode = table.getPage(parentPageNumber)
    val oldMax = getNodeMaxKey(oldNode)
    val child = table.getPage(childPageNumber)
    val childMax = getNodeMaxKey(child)
    val newPageNumber = table.getUnsuedPageNumber()
    /*
        Declaring a flag before updating pointers which
        records whether this operation involves splitting the root -
        if it does, we will insert our newly created node during
        the step where the table's new root is created. If it does
        not, we have to insert the newly created node into its parent
        after the old node's keys have been transferred over. We are not
        able to do this if the newly created node's parent is not a newly
        initialized root node, because in that case its parent may have existing
        keys aside from our old node which we are splitting. If that is true, we
        need to find a place for our newly created node in its parent, and we
        cannot insert it at the correct index if it does not yet have any keys.
   */
    val parent: ByteBuffer
    var newNode: ByteBuffer? = null
    val isRoot = isRoot(oldNode)
    if (isRoot) {
        createNewRoot(table, newPageNumber)
        parent = table.getRootPage()
        // If we are splitting the root, we need to update `oldNode` to point
        // to the new root's left child, `newPageNumber` will already point to
        // the new root's right child.
        oldPageNumber = getInternalNodeChild(parent, 0)
        oldNode = table.getPage(oldPageNumber)
    } else {
        val parentNode = getParentNode(oldNode)
        parent = table.getPage(parentNode)
        newNode = table.getPage(newPageNumber)
        initializeInternalNode(newNode)
    }
    var oldNumberKeys = getInternalNodeNumKeys(oldNode)
    var curPageNumber = getInternalNodeRightChild(oldNode)
    var cur = table.getPage(curPageNumber)
    // First put right child into new node and set right child of old node to invalid page number.
    internalNodeInsert(table, newPageNumber, curPageNumber)
    setParentNode(cur, newPageNumber)
    setInternalNodeRightChild(oldNode, INVALID_PAGE_NUMBER)
    // For each key until you get to the middle key, move the key and the child to the new node.
    for (i in INTERNAL_NODE_MAX_KEYS - 1 downTo INTERNAL_NODE_MAX_KEYS + 1 / 2) {
        curPageNumber = getInternalNodeChild(oldNode, i)
        cur = table.getPage(curPageNumber)
        internalNodeInsert(table, newPageNumber, curPageNumber)
        setParentNode(cur, newPageNumber)
        setInternalNodeNumKeys(oldNode, --oldNumberKeys)
    }
    // Set child before middle key, which is now the highest key, to be node's right child,
    // and decrement number of keys.
    val newValue = getInternalNodeChild(oldNode, oldNumberKeys - 1)
    setInternalNodeRightChild(oldNode, newValue)
    setInternalNodeNumKeys(oldNode, --oldNumberKeys)
    // Determine which of the two nodes after the split
    // should contain the child to be inserted and insert the child.
    val maxAfterSplit = getNodeMaxKey(table, oldNode)
    val destPageNumber = if (childMax < maxAfterSplit) oldPageNumber else newPageNumber
    internalNodeInsert(table, destPageNumber, childPageNumber)
    setParentNode(child, destPageNumber)
    val curMax = getNodeMaxKey(table, oldNode)
    updateInternalNodeKey(parent, oldMax, curMax)
    if (!isRoot) {
        val parent = getParentNode(oldNode)
        internalNodeInsert(table, parent, newPageNumber)
        setParentNode(newNode!!, getParentNode(oldNode))
    }
}

private fun moveCell(src: ByteBuffer, srcOffset: Int, dest: ByteBuffer, destOffset: Int) {
    src.moveInto(dest, destOffset, srcOffset, LEAF_NODE_CELL_SIZE)
}

// Represents an empty node.
const val INVALID_PAGE_NUMBER = Int.MAX_VALUE