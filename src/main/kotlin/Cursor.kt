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

fun tableEnd(table: Table): Cursor {
    val pageNumber = table.rootPageNumber
    val root = table.pager.getPage(pageNumber)
    val numCells = getLeafNodeNumCells(root)
    return Cursor(table, pageNumber, numCells, true)
}