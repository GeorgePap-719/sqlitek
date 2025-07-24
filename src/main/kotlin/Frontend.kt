package github.io

import github.io.ExecuteResult.*
import github.io.RowLayout.ROW_SIZE
import kotlin.system.exitProcess

// Non-SQL statements like .exit are called “meta-commands”.
// They all start with a dot, so we check for them and handle them in a separate function.
enum class MetaCommand(val value: String) {
    EXIT(".exit")
}

fun parseMetaCommand(input: String): MetaCommand? {
    if (input.first() != '.') {
        return null
    }
    for (command in MetaCommand.entries) {
        if (command.value == input) return command
    }
    return null
}

fun execute(command: MetaCommand) {
    when (command) {
        MetaCommand.EXIT -> exitProcess(0)
    }
}

class PrepareStatement(
    val type: PrepareStatementType,
    val row: Row?
)

enum class PrepareStatementType(val value: String) {
    SELECT("select"),
    INSERT("insert")
}

data class Row(
    val id: Int,
    val username: String,
    val email: String
)

fun parsePrepareStatement(input: String): PrepareStatement? {
    val tokens = input.split(" ")
    val firstWord = tokens.first()
    for (statement in PrepareStatementType.entries) {
        if (statement.value == firstWord) {
            if (statement == PrepareStatementType.SELECT) {
                return PrepareStatement(statement, null)
            }
            val id = tokens[1].toInt()
            val username = tokens[2]
            val email = tokens[3]
            val row = Row(id, username, email)
            return PrepareStatement(
                statement,
                row
            )
        }
    }
    return null
}

sealed class ExecuteResult {
    object Success : ExecuteResult()
    sealed class Failure : ExecuteResult()
    object TableIsFull : Failure()
}

fun execute(statement: PrepareStatement, table: Table) {
    when (statement.type) {
        PrepareStatementType.SELECT -> executeSelect(table)
        PrepareStatementType.INSERT -> executeInsert(statement.row!!, table)
    }
}

fun executeInsert(row: Row, table: Table): ExecuteResult {
    if (table.numberOfRows >= TABLE_MAX_ROWS) {
        return TableIsFull
    }
    val serialized = serialize(row)
    val slot = rowSlot(table, table.numberOfRows)
    slot.put(0, serialized, 0, serialized.size)
    table.numberOfRows++
    return Success
}

fun executeSelect(table: Table): ExecuteResult {
    for (i in 0..<table.numberOfRows) {
        val slot = rowSlot(table, i)
        slot.position(0)
        val raw = ByteArray(ROW_SIZE)
        slot.get(raw)
        val row = deserialize(raw)
        println(row)
    }
    return Success
}