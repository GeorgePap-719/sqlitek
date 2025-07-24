package io.sqlitek

import io.sqlitek.ConnectionResult.*
import io.sqlitek.RowLayout.ROW_SIZE
import kotlin.system.exitProcess

// vim mydb.db
//:%!xxdq
fun main(args: Array<String>) {
    if (args.isEmpty()) {
        println("Must supply a database filename")
        exitProcess(-1)
    }
    val filename = args[0]
    createConnection(filename).use {
        while (true) {
            println("db > ")
            val input = readln()
            val result = parseInput(input, it)
            when (result) {
                InvalidMetaStatement -> println("Unrecognized meta-statement:$input")
                is Failure -> println(result.message)
                // Nothing, just continue.
                Success -> {}
            }
        }
    }
}

// By opening a connection, we mean:
// - opening the database file
// - initializing a pager data structure
// - initializing a table data structure
fun createConnection(filename: String): Table {
    val pager = openPager(filename)
    val rowsNum = pager.fileLength / ROW_SIZE
    val table = Table(pager, rowsNum.toInt())
    return table
}

fun parseInput(input: String, table: Table): ConnectionResult {
    if (input.first() == '.') {
        val command = parseMetaCommand(input)
        if (command == null) {
            return InvalidMetaStatement
        }
        execute(command, table)
        return Success
    }
    val statementResult = parsePrepareStatement(input)
    when (statementResult) {
        is PrepareStatementResult.Failure -> return Failure(statementResult.message)
        is PrepareStatementResult.Success -> {
            execute(statementResult.value, table)
            return Success
        }
    }
}

sealed class ConnectionResult {
    object Success : ConnectionResult()
    class Failure(val message: String) : ConnectionResult()
    object InvalidMetaStatement : ConnectionResult()
}