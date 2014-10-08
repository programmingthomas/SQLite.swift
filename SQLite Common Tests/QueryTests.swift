import XCTest
import SQLite

class QueryTests: XCTestCase {

    let db = Database()
    var users: Query { return db["users"] }

    override func setUp() {
        super.setUp()

        CreateUsersTable(db)
    }

    func test_select_withString_compilesSelectClause() {
        let query = users.select("email")

        let SQL = "SELECT email FROM \"users\""
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_select_withVariadicStrings_compilesSelectClause() {
        let query = users.select("email", "count(*)")

        let SQL = "SELECT email, count(*) FROM \"users\""
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_selectDistinct_compilesSelectClause() {
        let query = users.select(distinct: "age")

        let SQL = "SELECT DISTINCT age FROM \"users\""
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_filter_withoutBindings_compilesWhereClause() {
        let query = users.filter("admin = 1")

        let SQL = "SELECT * FROM \"users\" WHERE admin = 1"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_filter_withExplicitBindings_compilesWhereClause() {
        let query = users.filter("admin = ?", true)

        let SQL = "SELECT * FROM \"users\" WHERE admin = 1"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_filter_withImplicitBindingsDictionary_compilesWhereClause() {
        let query = users.filter(["email": "alice@example.com", "age": 30])

        let SQL = "SELECT * FROM \"users\" " +
            "WHERE \"email\" = 'alice@example.com' " +
            "AND \"age\" = 30"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_filter_withArrayBindings_compilesWhereClause() {
        let query = users.filter(["id": [1, 2]])

        let SQL = "SELECT * FROM \"users\" WHERE \"id\" IN (1, 2)"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_filter_withRangeBindings_compilesWhereClause() {
        let query = users.filter(["age": 20..<30])

        let SQL = "SELECT * FROM \"users\" WHERE \"age\" BETWEEN 20 AND 30"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_filter_whenChained_compilesAggregateWhereClause() {
        let query = users
            .filter("email = ?", "alice@example.com")
            .filter("age >= ?", 21)

        let SQL = "SELECT * FROM \"users\" " +
            "WHERE email = 'alice@example.com' " +
            "AND age >= 21"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_order_withSingleColumnName_compilesOrderClause() {
        let query = users.order("age")

        let SQL = "SELECT * FROM \"users\" ORDER BY \"age\" ASC"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_order_withVariadicColumnNames_compilesOrderClause() {
        let query = users.order("age", "email")

        let SQL = "SELECT * FROM \"users\" ORDER BY \"age\" ASC, \"email\" ASC"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_order_withColumnAndDirection_compilesOrderClause() {
        let query = users.order("age", .DESC)

        let SQL = "SELECT * FROM \"users\" ORDER BY \"age\" DESC"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_order_withColumnDirectionTuple_compilesOrderClause() {
        let query = users.order(("age", .DESC))

        let SQL = "SELECT * FROM \"users\" ORDER BY \"age\" DESC"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_order_withVariadicColumnDirectionTuples_compilesOrderClause() {
        let query = users.order(("age", .DESC), ("email", .ASC))

        let SQL = "SELECT * FROM \"users\" ORDER BY \"age\" DESC, \"email\" ASC"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_order_whenChained_compilesAggregateOrderClause() {
        let query = users.order("age").order("email")

        let SQL = "SELECT * FROM \"users\" ORDER BY \"age\" ASC, \"email\" ASC"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_reorder_compilesFreshOrderClause() {
        let query = users.order("age", .DESC)

        let SQL = "SELECT * FROM \"users\" ORDER BY \"email\" ASC"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query.reorder("email", .ASC) {} }
    }

    func test_reverseOrder_reversesOrder() {
        let query = users.order(("age", .DESC), ("email", .ASC))

        let SQL = "SELECT * FROM \"users\" ORDER BY \"age\" ASC, \"email\" DESC"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query.reverseOrder {} }
    }

    func test_limit_compilesLimitClause() {
        let query = users.limit(5)

        let SQL = "SELECT * FROM \"users\" LIMIT 5"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_limit_withOffset_compilesOffsetClause() {
        let query = users.limit(5, offset: 5)

        let SQL = "SELECT * FROM \"users\" LIMIT 5 OFFSET 5"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_limit_whenChained_overridesLimit() {
        let query = users.limit(5).limit(10)

        var SQL = "SELECT * FROM \"users\" LIMIT 10"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }

        SQL = "SELECT * FROM \"users\""
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query.limit(nil) {} }
    }

    func test_limit_whenChained_withOffset_overridesOffset() {
        let query = users.limit(5, offset: 5).limit(10, offset: 10)

        var SQL = "SELECT * FROM \"users\" LIMIT 10 OFFSET 10"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }

        SQL = "SELECT * FROM \"users\""
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query.limit(nil) {} }
    }

    func test_SQL_compilesInOrder() {
        let query = users
            .select("email", "count(email) AS count")
            .filter("age >= ?", 21)
            .group("age", having: "count > ?", 1)
            .order("email", .ASC)
            .limit(1, offset: 2)

        let SQL = "SELECT email, count(email) AS count FROM \"users\" " +
            "WHERE age >= 21 " +
            "GROUP BY age HAVING count > 1 " +
            "ORDER BY \"email\" ASC " +
            "LIMIT 1 " +
            "OFFSET 2"
        ExpectExecutions(db, [SQL: 1]) { _ in for _ in query {} }
    }

    func test_count_returnsCount() {
        XCTAssertEqual(0, users.count)

        InsertUser(db, "alice")
        XCTAssertEqual(1, users.count)
    }

    func test_max_returnsMaximum() {
        XCTAssert(users.max("age") == nil)

        InsertUser(db, "alice", age: 20)
        InsertUser(db, "betsy", age: 30)
        XCTAssertEqual(30, users.max("age") as Int)
    }

    func test_max_returnsMinimum() {
        XCTAssert(users.min("age") == nil)

        InsertUser(db, "alice", age: 20)
        InsertUser(db, "betsy", age: 30)
        XCTAssertEqual(20, users.min("age") as Int)
    }

    func test_average_returnsAverage() {
        XCTAssert(users.average("age") == nil)

        InsertUser(db, "alice", age: 20)
        InsertUser(db, "betsy", age: 30)
        XCTAssertEqual(25.0, users.average("age")!)
    }

    func test_sum_returnsSum() {
        XCTAssert(users.sum("age") == nil)

        InsertUser(db, "alice", age: 20)
        InsertUser(db, "betsy", age: 30)
        XCTAssertEqual(50, users.sum("age") as Int)
    }

    func test_total_returnsTotal() {
        XCTAssertEqual(0.0, users.total("age")!)

        InsertUser(db, "alice", age: 20)
        InsertUser(db, "betsy", age: 30)
        XCTAssertEqual(50.0, users.total("age")!)
    }

    func test_insert_insertsRows() {
        let SQL = "INSERT INTO \"users\" (email, age) VALUES ('alice@example.com', 30)"
        ExpectExecutions(db, [SQL: 1]) { _ in
            XCTAssertEqual(1, self.users.insert(["email": "alice@example.com", "age": 30]).ID!)
        }

        XCTAssert(users.insert(["email": "alice@example.com", "age": 30]).ID == nil)
    }

    func test_update_updatesRows() {
        InsertUsers(db, "alice", "betsy")
        InsertUser(db, "dolly", age: 20)

        XCTAssertEqual(1, users.filter("age IS NOT NULL").update(["age": 30]).changes)
        XCTAssertEqual(0, users.filter("age > 30").update(["age": 30]).changes)
    }

    func test_delete_deletesRows() {
        InsertUser(db, "alice", age: 20)
        XCTAssertEqual(0, users.filter(["email": "betsy@example.com"]).delete().changes)

        InsertUser(db, "betsy", age: 30)
        XCTAssertEqual(2, users.delete().changes)
        XCTAssertEqual(0, users.delete().changes)
    }

}
