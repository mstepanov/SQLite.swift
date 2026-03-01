//
// VirtualTableTests.swift
// SQLite.swift
//

import XCTest
@testable import SQLite

// MARK: - Test Virtual Table Module: Sequence Generator

/// A simple virtual table that generates a sequence of integers
final class SequenceModule: VirtualTableModule {
    static let moduleName = "test_sequence"
    
    let start: Int64
    let end: Int64
    let step: Int64
    
    var columns: [VirtualTableColumn] {
        [
            VirtualTableColumn(name: "value", type: .integer),
            VirtualTableColumn(name: "ordinal", type: .integer)
        ]
    }
    
    required init(arguments: [String]) throws {
        // arguments[0] = module name
        // arguments[1] = database name
        // arguments[2] = table name
        // arguments[3...] = user arguments
        
        guard arguments.count >= 5 else {
            throw VirtualTableError.invalidArgument(
                "Expected: CREATE VIRTUAL TABLE t USING test_sequence(start, end)"
            )
        }
        
        guard let startVal = Int64(arguments[3].trimmingCharacters(in: .whitespaces)),
              let endVal = Int64(arguments[4].trimmingCharacters(in: .whitespaces)) else {
            throw VirtualTableError.invalidArgument("start and end must be integers")
        }
        
        self.start = startVal
        self.end = endVal
        self.step = arguments.count > 5 ? Int64(arguments[5].trimmingCharacters(in: .whitespaces)) ?? 1 : 1
    }
    
    func openCursor() throws -> SequenceCursor {
        SequenceCursor(module: self)
    }
}

final class SequenceCursor: VirtualTableCursor {
    let module: SequenceModule
    var currentValue: Int64
    var currentOrdinal: Int64 = 0
    var isEof: Bool = false
    
    init(module: SequenceModule) {
        self.module = module
        self.currentValue = module.start
    }
    
    func filter(indexNumber: Int32, indexString: String?, arguments: [Binding?]) throws {
        currentValue = module.start
        currentOrdinal = 0
        isEof = module.step > 0 ? currentValue > module.end : currentValue < module.end
    }
    
    func next() throws {
        currentValue += module.step
        currentOrdinal += 1
        isEof = module.step > 0 ? currentValue > module.end : currentValue < module.end
    }
    
    var eof: Bool { isEof }
    
    func column(_ index: Int32) -> Binding? {
        switch index {
        case 0: return currentValue
        case 1: return currentOrdinal
        default: return nil
        }
    }
    
    var rowid: Int64 { currentOrdinal }
    
    func close() {
        // No resources to release
    }
}

// MARK: - Test Virtual Table Module: Static Data

/// A virtual table that returns static data with multiple column types
final class StaticDataModule: VirtualTableModule {
    static let moduleName = "test_static"
    
    struct Row {
        let id: Int64
        let name: String
        let score: Double
        let data: Blob?
    }
    
    let rows: [Row]
    
    var columns: [VirtualTableColumn] {
        [
            VirtualTableColumn(name: "id", type: .integer),
            VirtualTableColumn(name: "name", type: .text),
            VirtualTableColumn(name: "score", type: .real),
            VirtualTableColumn(name: "data", type: .blob)
        ]
    }
    
    required init(arguments: [String]) throws {
        // Create some static test data
        self.rows = [
            Row(id: 1, name: "Alice", score: 95.5, data: Blob(bytes: [0x01, 0x02, 0x03])),
            Row(id: 2, name: "Bob", score: 87.3, data: nil),
            Row(id: 3, name: "Carol", score: 92.1, data: Blob(bytes: [0x04, 0x05]))
        ]
    }
    
    func openCursor() throws -> StaticDataCursor {
        StaticDataCursor(rows: rows)
    }
}

final class StaticDataCursor: VirtualTableCursor {
    let rows: [StaticDataModule.Row]
    var currentIndex: Int = -1
    
    init(rows: [StaticDataModule.Row]) {
        self.rows = rows
    }
    
    func filter(indexNumber: Int32, indexString: String?, arguments: [Binding?]) throws {
        currentIndex = 0
    }
    
    func next() throws {
        currentIndex += 1
    }
    
    var eof: Bool { currentIndex >= rows.count }
    
    func column(_ index: Int32) -> Binding? {
        guard currentIndex >= 0 && currentIndex < rows.count else { return nil }
        let row = rows[currentIndex]
        switch index {
        case 0: return row.id
        case 1: return row.name
        case 2: return row.score
        case 3: return row.data
        default: return nil
        }
    }
    
    var rowid: Int64 { currentIndex >= 0 ? Int64(currentIndex) : 0 }
    
    func close() {}
}

// MARK: - Test Virtual Table Module: Empty Table

/// A virtual table that returns no rows
final class EmptyModule: VirtualTableModule {
    static let moduleName = "test_empty"
    
    var columns: [VirtualTableColumn] {
        [VirtualTableColumn(name: "value", type: .integer)]
    }
    
    required init(arguments: [String]) throws {}
    
    func openCursor() throws -> EmptyCursor {
        EmptyCursor()
    }
}

final class EmptyCursor: VirtualTableCursor {
    func filter(indexNumber: Int32, indexString: String?, arguments: [Binding?]) throws {}
    func next() throws {}
    var eof: Bool { true }
    func column(_ index: Int32) -> Binding? { nil }
    var rowid: Int64 { 0 }
    func close() {}
}

// MARK: - Test Virtual Table Module: Error Handling

/// A virtual table that throws errors during iteration
final class ErrorModule: VirtualTableModule {
    static let moduleName = "test_error"
    
    var columns: [VirtualTableColumn] {
        [VirtualTableColumn(name: "value", type: .integer)]
    }
    
    required init(arguments: [String]) throws {
        // Check for "fail_init" argument
        if arguments.contains(where: { $0.contains("fail_init") }) {
            throw VirtualTableError.initializationFailed("Intentional init failure")
        }
    }
    
    func openCursor() throws -> ErrorCursor {
        ErrorCursor()
    }
}

final class ErrorCursor: VirtualTableCursor {
    var callCount = 0
    
    func filter(indexNumber: Int32, indexString: String?, arguments: [Binding?]) throws {
        callCount = 0
    }
    
    func next() throws {
        callCount += 1
        if callCount > 3 {
            throw VirtualTableError.iterationFailed("Intentional iteration failure")
        }
    }
    
    var eof: Bool { callCount >= 5 }
    
    func column(_ index: Int32) -> Binding? { Int64(callCount) }
    var rowid: Int64 { Int64(callCount) }
    func close() {}
}

// MARK: - Test Cases

class VirtualTableTests: SQLiteTestCase {
    
    // MARK: - Module Registration Tests
    
    func testModuleRegistration() throws {
        try db.createModule(SequenceModule.self)
        // Module should be registered successfully - no error thrown
    }
    
    func testMultipleModuleRegistration() throws {
        try db.createModule(SequenceModule.self)
        try db.createModule(StaticDataModule.self)
        try db.createModule(EmptyModule.self)
        // All modules should be registered successfully
    }
    
    // MARK: - Virtual Table Creation Tests
    
    func testCreateVirtualTable() throws {
        try db.createModule(SequenceModule.self)
        try db.execute("CREATE VIRTUAL TABLE nums USING test_sequence(1, 10)")
        // Table should be created successfully
    }
    
    func testCreateVirtualTableWithInvalidArgs() throws {
        try db.createModule(SequenceModule.self)
        
        XCTAssertThrowsError(try db.execute("CREATE VIRTUAL TABLE nums USING test_sequence()")) { error in
            // Should fail due to missing arguments
            XCTAssertTrue(error.localizedDescription.contains("Virtual table creation failed") ||
                         error.localizedDescription.contains("invalid") ||
                         error is Result)
        }
    }
    
    // MARK: - Query Tests
    
    func testSelectFromVirtualTable() throws {
        try db.createModule(SequenceModule.self)
        try db.execute("CREATE VIRTUAL TABLE nums USING test_sequence(1, 5)")
        
        var values: [Int64] = []
        for row in try db.prepare("SELECT value FROM nums") {
            if let value = row[0] as? Int64 {
                values.append(value)
            }
        }
        
        XCTAssertEqual(values, [1, 2, 3, 4, 5])
    }
    
    func testSelectAllColumnsFromVirtualTable() throws {
        try db.createModule(SequenceModule.self)
        try db.execute("CREATE VIRTUAL TABLE nums USING test_sequence(1, 3)")
        
        var results: [(Int64, Int64)] = []
        for row in try db.prepare("SELECT value, ordinal FROM nums") {
            if let value = row[0] as? Int64, let ordinal = row[1] as? Int64 {
                results.append((value, ordinal))
            }
        }
        
        XCTAssertEqual(results.count, 3)
        XCTAssertEqual(results[0].0, 1)  // value
        XCTAssertEqual(results[0].1, 0)  // ordinal
        XCTAssertEqual(results[1].0, 2)
        XCTAssertEqual(results[1].1, 1)
        XCTAssertEqual(results[2].0, 3)
        XCTAssertEqual(results[2].1, 2)
    }
    
    func testSelectWithWhereClause() throws {
        try db.createModule(SequenceModule.self)
        try db.execute("CREATE VIRTUAL TABLE nums USING test_sequence(1, 100)")
        
        var values: [Int64] = []
        for row in try db.prepare("SELECT value FROM nums WHERE value <= 5") {
            if let value = row[0] as? Int64 {
                values.append(value)
            }
        }
        
        XCTAssertEqual(values, [1, 2, 3, 4, 5])
    }
    
    func testSelectWithLimit() throws {
        try db.createModule(SequenceModule.self)
        try db.execute("CREATE VIRTUAL TABLE nums USING test_sequence(1, 100)")
        
        var values: [Int64] = []
        for row in try db.prepare("SELECT value FROM nums LIMIT 3") {
            if let value = row[0] as? Int64 {
                values.append(value)
            }
        }
        
        XCTAssertEqual(values.count, 3)
    }
    
    func testScalarFromVirtualTable() throws {
        try db.createModule(SequenceModule.self)
        try db.execute("CREATE VIRTUAL TABLE nums USING test_sequence(1, 10)")
        
        let sum = try db.scalar("SELECT SUM(value) FROM nums") as? Int64
        XCTAssertEqual(sum, 55)  // 1+2+3+4+5+6+7+8+9+10
    }
    
    func testCountFromVirtualTable() throws {
        try db.createModule(SequenceModule.self)
        try db.execute("CREATE VIRTUAL TABLE nums USING test_sequence(1, 10)")
        
        let count = try db.scalar("SELECT COUNT(*) FROM nums") as? Int64
        XCTAssertEqual(count, 10)
    }
    
    // MARK: - Multiple Column Types Tests
    
    func testMultipleColumnTypes() throws {
        try db.createModule(StaticDataModule.self)
        try db.execute("CREATE VIRTUAL TABLE people USING test_static()")
        
        var results: [(Int64, String, Double, Blob?)] = []
        for row in try db.prepare("SELECT id, name, score, data FROM people") {
            let id = row[0] as! Int64
            let name = row[1] as! String
            let score = row[2] as! Double
            let data = row[3] as? Blob
            results.append((id, name, score, data))
        }
        
        XCTAssertEqual(results.count, 3)
        
        XCTAssertEqual(results[0].0, 1)
        XCTAssertEqual(results[0].1, "Alice")
        XCTAssertEqual(results[0].2, 95.5)
        XCTAssertNotNil(results[0].3)
        XCTAssertEqual(results[0].3?.bytes, [0x01, 0x02, 0x03])
        
        XCTAssertEqual(results[1].0, 2)
        XCTAssertEqual(results[1].1, "Bob")
        XCTAssertEqual(results[1].2, 87.3)
        XCTAssertNil(results[1].3)
        
        XCTAssertEqual(results[2].0, 3)
        XCTAssertEqual(results[2].1, "Carol")
        XCTAssertEqual(results[2].2, 92.1)
    }
    
    func testAggregateOnMultipleTypes() throws {
        try db.createModule(StaticDataModule.self)
        try db.execute("CREATE VIRTUAL TABLE people USING test_static()")
        
        let avgScore = try db.scalar("SELECT AVG(score) FROM people") as? Double
        XCTAssertNotNil(avgScore)
        XCTAssertEqual(avgScore!, (95.5 + 87.3 + 92.1) / 3.0, accuracy: 0.001)
    }
    
    // MARK: - Empty Table Tests
    
    func testEmptyVirtualTable() throws {
        try db.createModule(EmptyModule.self)
        try db.execute("CREATE VIRTUAL TABLE empty USING test_empty()")
        
        var count = 0
        for _ in try db.prepare("SELECT * FROM empty") {
            count += 1
        }
        
        XCTAssertEqual(count, 0)
    }
    
    func testCountEmptyVirtualTable() throws {
        try db.createModule(EmptyModule.self)
        try db.execute("CREATE VIRTUAL TABLE empty USING test_empty()")
        
        let count = try db.scalar("SELECT COUNT(*) FROM empty") as? Int64
        XCTAssertEqual(count, 0)
    }
    
    // MARK: - Error Handling Tests
    
    func testVirtualTableInitError() throws {
        try db.createModule(ErrorModule.self)
        
        XCTAssertThrowsError(try db.execute("CREATE VIRTUAL TABLE err USING test_error(fail_init)")) { error in
            // Should fail during initialization
            XCTAssertTrue(error.localizedDescription.contains("Virtual table creation failed") ||
                         error.localizedDescription.contains("init") ||
                         error is Result)
        }
    }
    
    // MARK: - Drop Table Tests
    
    func testDropVirtualTable() throws {
        try db.createModule(SequenceModule.self)
        try db.execute("CREATE VIRTUAL TABLE nums USING test_sequence(1, 10)")
        
        // Verify table exists
        let beforeCount = try db.scalar("SELECT COUNT(*) FROM nums") as? Int64
        XCTAssertEqual(beforeCount, 10)
        
        // Drop table
        try db.execute("DROP TABLE nums")
        
        // Verify table is gone
        XCTAssertThrowsError(try db.scalar("SELECT COUNT(*) FROM nums"))
    }
    
    // MARK: - Multiple Cursors Tests
    
    func testMultipleCursors() throws {
        try db.createModule(SequenceModule.self)
        try db.execute("CREATE VIRTUAL TABLE nums USING test_sequence(1, 5)")
        
        // Run multiple queries in sequence
        var sum1: Int64 = 0
        for row in try db.prepare("SELECT value FROM nums") {
            if let value = row[0] as? Int64 {
                sum1 += value
            }
        }
        
        var sum2: Int64 = 0
        for row in try db.prepare("SELECT value FROM nums") {
            if let value = row[0] as? Int64 {
                sum2 += value
            }
        }
        
        XCTAssertEqual(sum1, 15)
        XCTAssertEqual(sum2, 15)
    }
    
    // MARK: - Join Tests
    
    func testJoinWithVirtualTable() throws {
        try createUsersTable()
        try insertUser("alice", age: 30)
        try insertUser("bob", age: 25)
        
        try db.createModule(SequenceModule.self)
        try db.execute("CREATE VIRTUAL TABLE ages USING test_sequence(20, 35)")
        
        // Join real table with virtual table
        var matches: [(String, Int64)] = []
        for row in try db.prepare("""
            SELECT u.email, a.value
            FROM users u
            JOIN ages a ON u.age = a.value
            ORDER BY u.email
        """) {
            if let email = row[0] as? String, let age = row[1] as? Int64 {
                matches.append((email, age))
            }
        }
        
        XCTAssertEqual(matches.count, 2)
        XCTAssertEqual(matches[0].0, "alice@example.com")
        XCTAssertEqual(matches[0].1, 30)
        XCTAssertEqual(matches[1].0, "bob@example.com")
        XCTAssertEqual(matches[1].1, 25)
    }
    
    // MARK: - Subquery Tests
    
    func testSubqueryWithVirtualTable() throws {
        try db.createModule(SequenceModule.self)
        try db.execute("CREATE VIRTUAL TABLE nums USING test_sequence(1, 10)")
        
        let count = try db.scalar("""
            SELECT COUNT(*) FROM (
                SELECT value FROM nums WHERE value > 5
            )
        """) as? Int64
        
        XCTAssertEqual(count, 5)  // 6, 7, 8, 9, 10
    }
}
