package io.sqlitek

class Cursor(
    val table: Table,
    var rowNumber: Int,
    // Indicates a position one past the last element.
    var endOfTable: Boolean
) {
    val pager = table.pager
}

fun tableStart(table: Table): Cursor {
    return Cursor(table, 0, table.numberOfRows == 0)
}

fun tableEnd(table: Table): Cursor {
    return Cursor(table, table.numberOfRows, true)
}