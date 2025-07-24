package io.sqlitek

import io.sqlitek.ConnectionResult.*

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
fun main() {
    val table = Table()
    while (true) {
        println("db > ")
        val input = readln()
        val result = createConnection(input, table)
        when (result) {
            InvalidMetaStatement -> println("Unrecognized meta-statement:$input")
            is Failure -> println(result.message)
            // Nothing, just continue.
            Success -> {}
        }
    }
}

fun createConnection(input: String, table: Table): ConnectionResult {
    if (input.first() == '.') {
        val command = parseMetaCommand(input)
        if (command == null) {
            return InvalidMetaStatement
        }
        execute(command)
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