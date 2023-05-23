package utils

open class PropertyHolder(vararg propertyPath: String) {
    protected val configs = TypedProperties(propertyPath.asList().joinToString("/"))
    constructor(propertyName: String) : this(*arrayOf(propertyName)) {}
}
