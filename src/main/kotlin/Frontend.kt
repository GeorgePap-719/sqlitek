package github.io

import kotlin.system.exitProcess

// Non-SQL statements like .exit are called “meta-commands”.
// They all start with a dot, so we check for them and handle them in a separate function.
enum class MetaCommand(val value: String) {
    EXIT(".exit")
}

fun parseMetaCommand(input: String): MetaCommand {
    if (input.first() != '.') {
        throw IllegalArgumentException("Unrecognized meta command:$input")
    }
    for (command in MetaCommand.entries) {
        if (command.value == input) return command
    }
    throw IllegalArgumentException("Unrecognized meta command:$input")
}

fun execute(command: MetaCommand) {
    when (command) {
        MetaCommand.EXIT -> exitProcess(0)
    }
}

class PrepareStatement(val value: String, val type: PrepareStatementType)

enum class PrepareStatementType(val value: String) {
    SELECT("select"),
    INSERT("insert")
}

fun parsePrepareStatement(input: String): PrepareStatement {
    val firstWord = input.split(" ").first()
    for (statement in PrepareStatementType.entries) {
        if (statement.value == firstWord) return PrepareStatement(input, statement)
    }
    throw IllegalArgumentException("Unrecognized prepare statement:$firstWord")
}

fun execute(statement: PrepareStatement) {
    when (statement.type) {
        PrepareStatementType.SELECT -> println("Would do a ${statement.value}")
        PrepareStatementType.INSERT -> println("Would do a ${statement.value}")
    }
}