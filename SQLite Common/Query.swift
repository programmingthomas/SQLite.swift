//
// SQLite.Query
// Copyright (c) 2014 Stephen Celis.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//

public struct Query {

    private var database: Database

    internal init(_ database: Database, _ tableName: String) {
        self.database = database
        self.tableName = tableName
    }

    // MARK: - Keywords

    public enum Direction: String {

        case ASC = "ASC"

        case DESC = "DESC"

    }

    private var columnNames = ["*"]
    private var tableName: String
    private var conditions: String?
    private var bindings = [Datatype?]()
    private var groupByHaving: ([String], String?, [Datatype?])?
    private var order = [String, Direction]()
    private var limit: Int?
    private var offset: Int?

    // MARK: -

    public func select(columnNames: String...) -> Query {
        var query = self
        query.columnNames = columnNames
        return query
    }

    public func select(#distinct: String) -> Query {
        return select("DISTINCT \(distinct)")
    }

    public func filter(condition: [String: Datatype?]) -> Query {
        var query = self
        for (column, value) in condition {
            let quotedColumn = Query.quote(column)
            switch value {
            case nil:
                query = query.filter("\(quotedColumn) IS NULL")
            case let value:
                query = query.filter("\(quotedColumn) = ?", value)
            }
        }
        return query
    }

    public func filter(condition: [String: [Datatype?]]) -> Query {
        var query = self
        for (column, values) in condition {
            let quotedColumn = Query.quote(column)
            let templates = Swift.join(", ", [String](count: values.count, repeatedValue: "?"))
            query = query.filter("\(quotedColumn) IN (\(templates))", values)
        }
        return query
    }

    public func filter<T: Datatype>(condition: [String: Range<T>]) -> Query {
        var query = self
        for (column, value) in condition {
            let quotedColumn = Query.quote(column)
            query = query.filter("\(quotedColumn) BETWEEN ? AND ?", value.startIndex, value.endIndex)
        }
        return query
    }

    public func filter(condition: String, _ bindings: Datatype?...) -> Query {
        return filter(condition, bindings)
    }

    public func filter(condition: String, _ bindings: [Datatype?]) -> Query {
        var query = self
        if let conditions = query.conditions {
            query.conditions = "\(conditions) AND \(condition)"
        } else {
            query.conditions = condition
        }
        query.bindings += bindings
        return query
    }

    public func group(by: String...) -> Query {
        return group(by, bindings)
    }

    public func group(by: String, having: String? = nil, _ bindings: Datatype?...) -> Query {
        return group([by], having: having, bindings)
    }

    public func group(by: [String], having: String? = nil, _ bindings: Datatype?...) -> Query {
        return group(by, having: having, bindings)
    }

    private func group(by: [String], having: String? = nil, _ bindings: [Datatype?]) -> Query {
        var query = self
        query.groupByHaving = (by, having, bindings)
        return query
    }

    public func order(by: String...) -> Query {
        return order(by)
    }

    private func order(by: [String]) -> Query {
        return order(by.map { ($0, .ASC) })
    }

    public func order(by: String, _ direction: Direction) -> Query {
        return order([(by, direction)])
    }

    public func order(by: (String, Direction)...) -> Query {
        return order(by)
    }

    private func order(by: [(String, Direction)]) -> Query {
        var query = self
        query.order += by
        return query
    }

    public func reorder(by: String...) -> Query {
        var query = self
        query.order.removeAll()
        return query.order(by)
    }

    public func reorder(by: String, _ direction: Direction) -> Query {
        var query = self
        query.order.removeAll()
        return query.order(by, direction)
    }

    public func reorder(by: (String, Direction)...) -> Query {
        var query = self
        query.order.removeAll()
        return query.order(by)
    }

    private func reorder(by: [(String, Direction)]) -> Query {
        var query = self
        query.order.removeAll()
        return query.order(by)
    }

    public var reverseOrder: Query {
        return reorder(order.map { ($0, $1 == .ASC ? .DESC : .ASC) })
    }

    public func limit(to: Int?) -> Query {
        return limit(to: to, offset: nil)
    }

    public func limit(to: Int, offset: Int? = nil) -> Query {
        return limit(to: to, offset: offset)
    }

    // prevent limit(nil, offset: 5)
    private func limit(#to: Int?, offset: Int? = nil) -> Query {
        var query = self
        (query.limit, query.offset) = (to, offset)
        return query
    }

    // MARK: - Compiling Statements

    private var selectStatement: Statement {
        let columnNames = Swift.join(", ", self.columnNames)
        var parts = ["SELECT \(columnNames) FROM \(Query.quote(tableName))"]
        whereClause.map(parts.append)
        groupClause.map(parts.append)
        orderClause.map(parts.append)
        limitClause.map(parts.append)

        var bindings = self.bindings
        if let (_, _, values) = groupByHaving { bindings += values }
        return database.prepare(Swift.join(" ", parts), bindings)
    }

    private func insertStatement(values: [String: Datatype?]) -> Statement {
        var (parts, bindings) = (["INSERT INTO \(Query.quote(tableName))"], self.bindings)
        let valuesClause = Swift.join(", ", map(values) { columnName, value in
            bindings.append(value)
            return columnName
        })
        let templates = Swift.join(", ", [String](count: values.count, repeatedValue: "?"))
        parts.append("(\(valuesClause)) VALUES (\(templates))")
        return database.prepare(Swift.join(" ", parts), bindings)
    }

    private func updateStatement(values: [String: Datatype?]) -> Statement {
        var (parts, bindings) = (["UPDATE \(Query.quote(tableName))"], self.bindings)
        let valuesClause = Swift.join(", ", map(values) { columnName, value in
            bindings.append(value)
            return "\(columnName) = ?"
        })
        parts.append("SET \(valuesClause)")
        whereClause.map(parts.append)
        return database.prepare(Swift.join(" ", parts), bindings)
    }

    private var deleteStatement: Statement {
        var parts = ["DELETE FROM \(Query.quote(tableName))"]
        whereClause.map(parts.append)
        return database.prepare(Swift.join(" ", parts), bindings)
    }

    // MARK: -

    private var whereClause: String? {
        if let conditions = conditions { return "WHERE \(conditions)" }
        return nil
    }

    private var groupClause: String? {
        if let (groupBy, having, _) = groupByHaving {
            let groups = Swift.join(", ", groupBy)
            var clause = ["GROUP BY \(groups)"]
            having.map { clause.append("HAVING \($0)") }
            return Swift.join(" ", clause)
        }
        return nil
    }

    private var orderClause: String? {
        if order.count == 0 { return nil }
        let mapped = order.map { "\(Query.quote($0.0)) \($0.1.toRaw())" }
        let joined = Swift.join(", ", mapped)
        return "ORDER BY \(joined)"
    }

    private var limitClause: String? {
        if let to = limit {
            var clause = ["LIMIT \(to)"]
            offset.map { clause.append("OFFSET \($0)") }
            return Swift.join(" ", clause)
        }
        return nil
    }

    private static func quote(identifier: String) -> String {
        return "\"\(identifier)\""
    }

    // MARK: - Aggregate Functions

    public var count: Int {
        return select("count(*)").selectStatement.scalar() as Int
    }

    public func max(columnName: String) -> Datatype? {
        return calculate("max", columnName)
    }

    public func min(columnName: String) -> Datatype? {
        return calculate("min", columnName)
    }

    public func average(columnName: String) -> Double? {
        return calculate("avg", columnName) as? Double
    }

    public func sum(columnName: String) -> Datatype? {
        return calculate("sum", columnName)
    }

    public func total(columnName: String) -> Double? {
        return calculate("total", columnName) as? Double
    }

    private func calculate(function: String, _ columnName: String) -> Datatype? {
        return select("\(function)(\(Query.quote(columnName)))").selectStatement.scalar()
    }

    // MARK: -

    public func insert(values: [String: Datatype?]) -> Statement {
        return insertStatement(values).run()
    }

    public func insert(values: [String: Datatype?]) -> (ID: Int?, statement: Statement) {
        let statement = insert(values) as Statement
        return (statement.failed ? nil : database.lastID, statement)
    }

    public func update(values: [String: Datatype?]) -> Statement {
        return updateStatement(values).run()
    }

    public func update(values: [String: Datatype?]) -> (changes: Int, statement: Statement) {
        let statement = update(values) as Statement
        return (statement.failed ? 0 : database.lastChanges ?? 0, statement)
    }

    public func delete() -> Statement {
        return deleteStatement.run()
    }

    public func delete() -> (changes: Int, statement: Statement) {
        let statement = delete() as Statement
        return (statement.failed ? 0 : database.lastChanges ?? 0, statement)
    }

}

// MARK: - SequenceType
extension Query: SequenceType {

    public typealias Generator = QueryGenerator

    public func generate() -> Generator { return Generator(selectStatement) }

}

// MARK: - GeneratorType
public struct QueryGenerator: GeneratorType {

    public typealias Element = [String: Datatype?]

    private var statement: Statement

    private init(_ statement: Statement) { self.statement = statement }

    public func next() -> Element? {
        statement.next()
        return statement.values
    }

}
