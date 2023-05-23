package utils

import java.util.Properties

class TypedProperties(propertiesName: String) : Properties() {

    init {
        javaClass.classLoader.getResourceAsStream("$propertiesName.properties").use {
            load(it)
        }
    }

    internal inline operator fun <reified T> get(key: String): T {
        return when (T::class) {
            Int::class -> super.get(key).toString().toInt()
            Long::class -> super.get(key).toString().toLong()
            String::class -> super.get(key).toString()

            IntArray::class -> super.get(key).toString().split(",").map { str -> str.toInt() }.toIntArray()
            LongArray::class -> super.get(key).toString().split(",").map { str -> str.toLong() }.toLongArray()
            Array<String>::class -> super.get(key).toString().split(",").toTypedArray()
            else -> null
        } as T
    }
}