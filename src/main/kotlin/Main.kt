package github.io

import kotlin.system.exitProcess

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
fun main() {
    while (true) {
        println("db > ")
        val input = readln()
        if (input.first() == '.') {
            val command = parseMetaCommand(input)
            execute(command)
            return
        }
        val statement = parsePrepareStatement(input)
        execute(statement)
    }
}