package io.sqlitek

import io.sqlitek.ExecuteResult.*
import io.sqlitek.RowLayout.EMAIL_SIZE
import io.sqlitek.RowLayout.ROW_SIZE
import io.sqlitek.RowLayout.USERNAME_SIZE
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

fun execute(command: MetaCommand, table: Table) {
    when (command) {
        MetaCommand.EXIT -> {
            // This is needed again here,
            // because `exitProcess` overrides the `finally` keyword.
            closeDatabase(table)
            exitProcess(0)
        }
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

sealed class PrepareStatementResult {
    class Success(val value: PrepareStatement) : PrepareStatementResult()
    class Failure(val message: String) : PrepareStatementResult()
}

fun parsePrepareStatement(input: String): PrepareStatementResult {
    val tokens = input.split(" ")
    val firstWord = tokens.first()
    for (statement in PrepareStatementType.entries) {
        if (statement.value == firstWord) {
            if (statement == PrepareStatementType.SELECT) {
                val prepareStatement = PrepareStatement(statement, null)
                return PrepareStatementResult.Success(prepareStatement)
            }
            val id = tokens[1].toInt()
            if (id <= 0) {
                return PrepareStatementResult.Failure("`id` is not allowed to be negative")
            }
            val username = tokens[2]
            if (username.length > USERNAME_SIZE) {
                return PrepareStatementResult.Failure("`username` exceeds the max length:$USERNAME_SIZE")
            }
            val email = tokens[3]
            if (email.length > EMAIL_SIZE) {
                return PrepareStatementResult.Failure("`email` exceeds the max length:$USERNAME_SIZE")
            }
            val row = Row(id, username, email)
            val prepareStatement = PrepareStatement(
                statement,
                row
            )
            return PrepareStatementResult.Success(prepareStatement)
        }
    }
    return PrepareStatementResult.Failure("Unrecognized statement:$input")
}

sealed class ExecuteResult {
    object Success : ExecuteResult()
    sealed class Failure : ExecuteResult()
    object TableIsFull : Failure()
}

fun execute(statement: PrepareStatement, table: Table) {
    when (statement.type) {
        PrepareStatementType.SELECT -> executeSelect(table)
        PrepareStatementType.INSERT -> {
            val result = executeInsert(statement.row!!, table)
            if (result is TableIsFull) {
                println("Table is full!!")
            }
        }
    }
}

fun executeInsert(row: Row, table: Table): ExecuteResult {
    if (table.numberOfRows >= TABLE_MAX_ROWS) {
        return TableIsFull
    }
    val serialized = serialize(row)
    val slot = table.getCurRowSlot()
    slot.put(0, serialized, 0, serialized.size)
    table.numberOfRows++
    return Success
}

fun executeSelect(table: Table): ExecuteResult {
    for (i in 0..<table.numberOfRows) {
        val slot = table.getRowSlot(i)
        slot.position(0)
        val raw = ByteArray(ROW_SIZE)
        slot.get(raw)
        val row = deserialize(raw)
        println(row)
    }
    return Success
}