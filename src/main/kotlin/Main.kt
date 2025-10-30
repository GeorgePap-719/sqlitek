package io.sqlitek

import io.sqlitek.ConnectionResult.*
import io.sqlitek.btree.Table
import kotlin.system.exitProcess

// vim mydb.db
//:%!xxd
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
            when (val result = parseInput(input, it)) {
                InvalidMetaStatement -> println("Unrecognized meta-statement:$input")
                is Failure -> println(result.message)
                // Nothing, just continue.
                Success -> {}
            }
        }
    }
}

fun parseInput(input: String, table: Table): ConnectionResult {
    if (input.first() == '.') {
        val command = parseMetaCommand(input) ?: return InvalidMetaStatement
        execute(command, table)
        return Success
    }
    when (val statementResult = parsePrepareStatement(input)) {
        is PrepareStatementResult.Failure -> return Failure(statementResult.message)
        is PrepareStatementResult.Success -> {
            execute(statementResult.value, table)
            return Success
        }
    }
}

sealed class ConnectionResult {
    object Success : ConnectionResult() {
        override fun toString(): String = this::class.simpleName!!
    }
    class Failure(val message: String) : ConnectionResult()
    object InvalidMetaStatement : ConnectionResult()
}