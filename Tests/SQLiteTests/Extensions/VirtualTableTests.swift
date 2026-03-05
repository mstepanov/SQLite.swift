//
// VirtualTableTests.swift
// SQLite.swift
//

import XCTest
import SQLite3
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

// MARK: - Test Virtual Table Module: BLOB Match (KNN-style)

/// A virtual table that supports MATCH with BLOB arguments.
/// Mimics the KNN pattern: WHERE col MATCH blob AND k = N
/// Used to verify BLOB values survive the UDF → constraint evaluator → xFilter path.
final class BlobMatchModule: VirtualTableModule {
    static let moduleName = "test_blob_match"
    
    struct StoredRow {
        let rowid: Int64
        let data: [UInt8]
    }
    
    var storedRows: [StoredRow] = []
    
    var columns: [VirtualTableColumn] {
        [
            VirtualTableColumn(name: "payload", type: .blob),
            VirtualTableColumn(name: "distance", type: .real),
            VirtualTableColumn(name: "k", type: .integer, hidden: true)
        ]
    }
    
    required init(arguments: [String]) throws {
        // Pre-populate with some test data
        // Each row has a 4-byte BLOB (one little-endian float32)
        storedRows = [
            StoredRow(rowid: 1, data: _floatToBytes(0.0)),
            StoredRow(rowid: 2, data: _floatToBytes(1.0)),
            StoredRow(rowid: 3, data: _floatToBytes(5.0)),
            StoredRow(rowid: 4, data: _floatToBytes(10.0)),
        ]
    }
    
    func openCursor() throws -> BlobMatchCursor {
        BlobMatchCursor(module: self)
    }
    
    func bestIndex(_ indexInfo: inout VirtualTableIndexInfo) -> Int32 {
        var matchIdx: Int? = nil
        var kIdx: Int? = nil
        
        let kColumnIdx = Int32(1 + 1) // distance=1, k=2
        
        for (i, constraint) in indexInfo.constraints.enumerated() {
            guard constraint.usable else { continue }
            
            if constraint.op == .match && constraint.column == 0 {
                matchIdx = i
            }
            
            if constraint.op == .eq && constraint.column == kColumnIdx {
                kIdx = i
            }
        }
        
        // Always assign MATCH → argvIndex=1, k → argvIndex=2
        // so filter() receives arguments in a fixed order regardless
        // of which order SQLite presents the constraints.
        if let mi = matchIdx {
            indexInfo.constraints[mi].argvIndex = 1
            indexInfo.constraints[mi].omit = true
        }
        if let ki = kIdx {
            indexInfo.constraints[ki].argvIndex = 2
            indexInfo.constraints[ki].omit = true
        }
        
        let hasMatch = matchIdx != nil
        
        if hasMatch {
            indexInfo.indexNumber = 3 // KNN plan
            indexInfo.estimatedCost = 25.0
            indexInfo.estimatedRows = 10
            indexInfo.orderByConsumed = true
        } else {
            indexInfo.indexNumber = 1 // full scan
            indexInfo.estimatedCost = 1000.0
            indexInfo.estimatedRows = Int64(storedRows.count)
        }
        
        return SQLITE_OK
    }
    
    private static func floatToBytes(_ value: Float) -> [UInt8] {
        withUnsafeBytes(of: value.bitPattern.littleEndian) { Array($0) }
    }
}

/// Free function versions accessible from required init and cursors
private func _floatToBytes(_ value: Float) -> [UInt8] {
    withUnsafeBytes(of: value.bitPattern.littleEndian) { Array($0) }
}

private func _bytesToFloat(_ bytes: [UInt8]) -> Float {
    guard bytes.count >= 4 else { return 0 }
    let bits = UInt32(bytes[0]) | UInt32(bytes[1]) << 8 | UInt32(bytes[2]) << 16 | UInt32(bytes[3]) << 24
    return Float(bitPattern: bits)
}

final class BlobMatchCursor: VirtualTableCursor {
    let module: BlobMatchModule
    
    // For KNN: sorted results
    private var results: [(rowIndex: Int, distance: Float)] = []
    private var currentIndex: Int = 0
    private var isKNN: Bool = false
    
    // Diagnostic: records the actual type received for the MATCH argument
    var matchArgumentType: String = "none"
    var matchArgumentWasBlob: Bool = false
    var matchArgumentByteCount: Int = 0
    
    init(module: BlobMatchModule) {
        self.module = module
    }
    
    func filter(indexNumber: Int32, indexString: String?, arguments: [Binding?]) throws {
        results.removeAll()
        currentIndex = 0
        
        if indexNumber == 3 {
            // KNN plan — extract BLOB from MATCH argument
            isKNN = true
            
            guard arguments.count > 0, let binding = arguments[0] else {
                matchArgumentType = "nil_or_empty"
                return
            }
            
            matchArgumentType = String(describing: type(of: binding))
            
            guard let blob = binding as? Blob else {
                // This is the failure case we're testing for
                return
            }
            
            matchArgumentWasBlob = true
            matchArgumentByteCount = blob.bytes.count
            
            let queryValue = _bytesToFloat(blob.bytes)
            
            let k: Int
            if arguments.count > 1, let kVal = arguments[1] as? Int64 {
                k = Int(kVal)
            } else {
                k = 10
            }
            
            // Compute "distance" as absolute difference
            var distances: [(rowIndex: Int, distance: Float)] = []
            for (i, row) in module.storedRows.enumerated() {
                let storedValue = _bytesToFloat(row.data)
                let dist = abs(queryValue - storedValue)
                distances.append((i, dist))
            }
            
            distances.sort { $0.distance < $1.distance }
            results = Array(distances.prefix(k))
            
        } else {
            // Full scan
            isKNN = false
            results = module.storedRows.enumerated().map { ($0.offset, Float(0)) }
        }
    }
    
    func next() throws {
        currentIndex += 1
    }
    
    var eof: Bool {
        currentIndex >= results.count
    }
    
    func column(_ index: Int32) -> Binding? {
        guard currentIndex < results.count else { return nil }
        let rowIndex = results[currentIndex].rowIndex
        
        switch index {
        case 0: // payload column
            return Blob(bytes: module.storedRows[rowIndex].data)
        case 1: // distance column
            return Double(results[currentIndex].distance)
        case 2: // k column (hidden)
            return nil
        default:
            return nil
        }
    }
    
    var rowid: Int64 {
        guard currentIndex < results.count else { return 0 }
        return module.storedRows[results[currentIndex].rowIndex].rowid
    }
    
    func close() {
        results.removeAll()
    }
}

// MARK: - Test Virtual Table Module: Writable with BLOB

/// A writable virtual table that stores BLOBs via xUpdate, then supports MATCH queries.
/// Tests the full INSERT-via-UDF + MATCH-via-UDF roundtrip.
final class WritableBlobModule: VirtualTableModule {
    static let moduleName = "test_writable_blob"
    
    var storedBlobs: [(rowid: Int64, data: [UInt8])] = []
    private var nextRowid: Int64 = 1
    
    var columns: [VirtualTableColumn] {
        [
            VirtualTableColumn(name: "vec", type: .blob),
            VirtualTableColumn(name: "distance", type: .real),
            VirtualTableColumn(name: "k", type: .integer, hidden: true)
        ]
    }
    
    required init(arguments: [String]) throws {}
    
    func openCursor() throws -> WritableBlobCursor {
        WritableBlobCursor(module: self)
    }
    
    func bestIndex(_ indexInfo: inout VirtualTableIndexInfo) -> Int32 {
        var matchIdx: Int? = nil
        var kIdx: Int? = nil
        let kColumnIdx = Int32(2) // vec=0, distance=1, k=2
        
        for (i, constraint) in indexInfo.constraints.enumerated() {
            guard constraint.usable else { continue }
            
            if constraint.op == .match && constraint.column == 0 {
                matchIdx = i
            }
            if constraint.op == .eq && constraint.column == kColumnIdx {
                kIdx = i
            }
        }
        
        // Always assign MATCH → argvIndex=1, k → argvIndex=2
        if let mi = matchIdx {
            indexInfo.constraints[mi].argvIndex = 1
            indexInfo.constraints[mi].omit = true
        }
        if let ki = kIdx {
            indexInfo.constraints[ki].argvIndex = 2
            indexInfo.constraints[ki].omit = true
        }
        
        let hasMatch = matchIdx != nil
        if hasMatch {
            indexInfo.indexNumber = 3
            indexInfo.estimatedCost = 25.0
            indexInfo.estimatedRows = 10
            indexInfo.orderByConsumed = true
        } else {
            indexInfo.indexNumber = 1
            indexInfo.estimatedCost = 1000.0
            indexInfo.estimatedRows = Int64(storedBlobs.count)
        }
        
        return SQLITE_OK
    }
    
    func update(_ arguments: [Binding?]) throws -> Int64 {
        if arguments.count == 1 {
            // DELETE
            if let binding = arguments[0], let rid = binding as? Int64 {
                storedBlobs.removeAll { $0.rowid == rid }
            }
            return 0
        } else if arguments.count > 1 && arguments[0] == nil {
            // INSERT
            let rid: Int64
            if let rowidBinding = arguments[1], let explicitRid = rowidBinding as? Int64 {
                rid = explicitRid
            } else {
                rid = nextRowid
            }
            if rid >= nextRowid { nextRowid = rid + 1 }
            
            // arguments[2] = vec BLOB
            if let blob = arguments[2] as? Blob {
                storedBlobs.append((rowid: rid, data: blob.bytes))
            }
            return rid
        }
        return 0
    }
}

final class WritableBlobCursor: VirtualTableCursor {
    let module: WritableBlobModule
    private var results: [(index: Int, distance: Float)] = []
    private var currentIndex: Int = 0
    private var isKNN: Bool = false
    
    init(module: WritableBlobModule) {
        self.module = module
    }
    
    func filter(indexNumber: Int32, indexString: String?, arguments: [Binding?]) throws {
        results.removeAll()
        currentIndex = 0
        
        if indexNumber == 3 {
            isKNN = true
            guard arguments.count > 0, let binding = arguments[0], let blob = binding as? Blob else {
                return
            }
            
            let queryValue = _bytesToFloat(blob.bytes)
            let k: Int = (arguments.count > 1 ? (arguments[1] as? Int64).map { Int($0) } : nil) ?? 10
            
            var distances: [(index: Int, distance: Float)] = []
            for (i, row) in module.storedBlobs.enumerated() {
                let storedValue = _bytesToFloat(row.data)
                distances.append((i, abs(queryValue - storedValue)))
            }
            distances.sort { $0.distance < $1.distance }
            results = Array(distances.prefix(k))
        } else {
            isKNN = false
            results = module.storedBlobs.indices.map { ($0, Float(0)) }
        }
    }
    
    func next() throws { currentIndex += 1 }
    var eof: Bool { currentIndex >= results.count }
    
    func column(_ index: Int32) -> Binding? {
        guard currentIndex < results.count else { return nil }
        let ri = results[currentIndex].index
        switch index {
        case 0: return Blob(bytes: module.storedBlobs[ri].data)
        case 1: return Double(results[currentIndex].distance)
        case 2: return nil
        default: return nil
        }
    }
    
    var rowid: Int64 {
        guard currentIndex < results.count else { return 0 }
        return module.storedBlobs[results[currentIndex].index].rowid
    }
    
    func close() { results.removeAll() }
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
    
    // MARK: - BLOB MATCH Tests (UDF → constraint evaluator → xFilter path)
    
    /// Verifies that a BLOB literal passed via MATCH reaches xFilter intact.
    /// This is the simplest case: a raw X'...' hex literal, no UDF involved.
    func testBlobMatchWithLiteral() throws {
        try db.createModule(BlobMatchModule.self)
        try db.execute("CREATE VIRTUAL TABLE vecs USING test_blob_match()")
        
        // X'00000000' is float32 0.0 in little-endian
        var rowids: [Int64] = []
        var distances: [Double] = []
        for row in try db.prepare("""
            SELECT rowid, distance FROM vecs
            WHERE payload MATCH X'00000000' AND k = 2
        """) {
            if let rid = row[0] as? Int64 { rowids.append(rid) }
            if let dist = row[1] as? Double { distances.append(dist) }
        }
        
        // Should get 2 results: rowid 1 (value=0.0, dist=0) and rowid 2 (value=1.0, dist=1)
        XCTAssertEqual(rowids.count, 2, "MATCH with BLOB literal should return k=2 results")
        XCTAssertEqual(rowids[0], 1, "Closest row should be rowid 1 (exact match)")
        XCTAssertEqual(distances[0], 0.0, accuracy: 0.001, "Distance to exact match should be 0")
        XCTAssertTrue(distances[0] <= distances[1], "Results should be sorted by distance ascending")
    }
    
    /// Verifies that a BLOB produced by a UDF survives the constraint evaluator
    /// and reaches xFilter as a Blob binding. This is the critical path that
    /// was broken when sqlite3_result_blob used nil instead of SQLITE_TRANSIENT.
    func testBlobMatchWithUDFResult() throws {
        // Register a UDF that produces a BLOB (simulating vec_f32)
        db.createFunction("make_blob", argumentCount: 1, deterministic: true) { args -> Binding? in
            guard let text = args[0] as? String else { return nil }
            // Parse a single float from text and return as 4-byte BLOB
            guard let value = Float(text) else { return nil }
            let bytes = withUnsafeBytes(of: value.bitPattern.littleEndian) { Array($0) }
            return Blob(bytes: bytes)
        }
        
        try db.createModule(BlobMatchModule.self)
        try db.execute("CREATE VIRTUAL TABLE vecs USING test_blob_match()")
        
        // Use UDF to produce the MATCH argument — this exercises the full
        // UDF result → sqlite3_result_blob → constraint evaluator → xFilter path
        var rowids: [Int64] = []
        var distances: [Double] = []
        for row in try db.prepare("""
            SELECT rowid, distance FROM vecs
            WHERE payload MATCH make_blob('0.0') AND k = 2
        """) {
            if let rid = row[0] as? Int64 { rowids.append(rid) }
            if let dist = row[1] as? Double { distances.append(dist) }
        }
        
        XCTAssertEqual(rowids.count, 2, "MATCH with UDF BLOB should return k=2 results")
        XCTAssertEqual(rowids[0], 1, "Closest row should be rowid 1 (exact match at 0.0)")
        XCTAssertEqual(distances[0], 0.0, accuracy: 0.001, "Distance to exact match should be 0")
        XCTAssertTrue(distances[0] <= distances[1], "Results should be sorted by distance ascending")
    }
    
    /// Verifies the full roundtrip: INSERT via UDF (xUpdate receives BLOB),
    /// then MATCH via UDF (xFilter receives BLOB), and correct results are returned.
    func testWritableVtabBlobInsertThenMatch() throws {
        // UDF: converts text like "3.14" to a 4-byte float32 BLOB
        db.createFunction("to_f32", argumentCount: 1, deterministic: true) { args -> Binding? in
            guard let text = args[0] as? String, let value = Float(text) else { return nil }
            let bytes = withUnsafeBytes(of: value.bitPattern.littleEndian) { Array($0) }
            return Blob(bytes: bytes)
        }
        
        try db.createModule(WritableBlobModule.self)
        try db.execute("CREATE VIRTUAL TABLE wvecs USING test_writable_blob()")
        
        // INSERT rows using UDF to produce BLOBs
        try db.execute("INSERT INTO wvecs(rowid, vec) VALUES (1, to_f32('0.0'))")
        try db.execute("INSERT INTO wvecs(rowid, vec) VALUES (2, to_f32('1.0'))")
        try db.execute("INSERT INTO wvecs(rowid, vec) VALUES (3, to_f32('5.0'))")
        try db.execute("INSERT INTO wvecs(rowid, vec) VALUES (4, to_f32('10.0'))")
        
        // Verify full scan returns all 4 rows
        let fullScanCount = try db.scalar("SELECT COUNT(*) FROM wvecs") as? Int64
        XCTAssertEqual(fullScanCount, 4, "Full scan should return 4 rows after INSERTs")
        
        // MATCH query using UDF for the query vector
        var rowids: [Int64] = []
        var distances: [Double] = []
        for row in try db.prepare("""
            SELECT rowid, distance FROM wvecs
            WHERE vec MATCH to_f32('0.0') AND k = 2
        """) {
            if let rid = row[0] as? Int64 { rowids.append(rid) }
            if let dist = row[1] as? Double { distances.append(dist) }
        }
        
        XCTAssertEqual(rowids.count, 2, "KNN MATCH should return k=2 results")
        XCTAssertEqual(rowids[0], 1, "Nearest to 0.0 should be rowid 1")
        XCTAssertEqual(rowids[1], 2, "Second nearest to 0.0 should be rowid 2 (value=1.0)")
        XCTAssertEqual(distances[0], 0.0, accuracy: 0.001)
        XCTAssertEqual(distances[1], 1.0, accuracy: 0.001)
    }
    
    /// Tests that a large BLOB (simulating a high-dimensional vector) survives
    /// the UDF → constraint evaluator → xFilter roundtrip without corruption.
    func testBlobMatchWithLargeBlob() throws {
        // Create a UDF that produces a large BLOB (768 floats = 3072 bytes)
        db.createFunction("make_large_blob", argumentCount: 0, deterministic: true) { _ -> Binding? in
            // Create a 3072-byte blob (768 float32s, all zeros)
            let bytes = [UInt8](repeating: 0, count: 3072)
            return Blob(bytes: bytes)
        }
        
        try db.createModule(BlobMatchModule.self)
        try db.execute("CREATE VIRTUAL TABLE vecs USING test_blob_match()")
        
        // The module has 4-byte stored rows, but the MATCH argument is 3072 bytes.
        // The important thing is that the BLOB arrives intact at xFilter.
        // The distance computation will produce 0 for the first float comparison.
        var count = 0
        for _ in try db.prepare("""
            SELECT rowid FROM vecs
            WHERE payload MATCH make_large_blob() AND k = 2
        """) {
            count += 1
        }
        
        // Should return results (the exact count depends on the module logic,
        // but the key assertion is no crash and the BLOB was received)
        XCTAssertEqual(count, 2, "Large BLOB should survive UDF → xFilter roundtrip")
    }
    
    /// Verifies that multiple sequential MATCH queries with different UDF-produced
    /// BLOBs all return correct results (no stale/cached BLOB data).
    func testBlobMatchMultipleSequentialQueries() throws {
        db.createFunction("to_f32", argumentCount: 1, deterministic: true) { args -> Binding? in
            guard let text = args[0] as? String, let value = Float(text) else { return nil }
            let bytes = withUnsafeBytes(of: value.bitPattern.littleEndian) { Array($0) }
            return Blob(bytes: bytes)
        }
        
        try db.createModule(BlobMatchModule.self)
        try db.execute("CREATE VIRTUAL TABLE vecs USING test_blob_match()")
        
        // Query 1: nearest to 0.0 → should get rowid 1 first
        var rowids1: [Int64] = []
        for row in try db.prepare("SELECT rowid FROM vecs WHERE payload MATCH to_f32('0.0') AND k = 1") {
            if let rid = row[0] as? Int64 { rowids1.append(rid) }
        }
        XCTAssertEqual(rowids1, [1], "Nearest to 0.0 should be rowid 1")
        
        // Query 2: nearest to 10.0 → should get rowid 4 first
        var rowids2: [Int64] = []
        for row in try db.prepare("SELECT rowid FROM vecs WHERE payload MATCH to_f32('10.0') AND k = 1") {
            if let rid = row[0] as? Int64 { rowids2.append(rid) }
        }
        XCTAssertEqual(rowids2, [4], "Nearest to 10.0 should be rowid 4")
        
        // Query 3: nearest to 5.0 → should get rowid 3 first
        var rowids3: [Int64] = []
        for row in try db.prepare("SELECT rowid FROM vecs WHERE payload MATCH to_f32('5.0') AND k = 1") {
            if let rid = row[0] as? Int64 { rowids3.append(rid) }
        }
        XCTAssertEqual(rowids3, [3], "Nearest to 5.0 should be rowid 3")
    }
    
    /// Regression test: UDF BLOB result must use SQLITE_TRANSIENT so SQLite copies
    /// the data before the Swift array is freed. Without SQLITE_TRANSIENT, the BLOB
    /// data may be corrupted by the time xFilter reads it.
    func testUDFBlobResultSurvivesConstraintEvaluation() throws {
        // Register a UDF that returns a known BLOB pattern
        let expectedBytes: [UInt8] = [0xDE, 0xAD, 0xBE, 0xEF]
        db.createFunction("known_blob", argumentCount: 0, deterministic: true) { _ -> Binding? in
            return Blob(bytes: expectedBytes)
        }
        
        try db.createModule(BlobMatchModule.self)
        try db.execute("CREATE VIRTUAL TABLE vecs USING test_blob_match()")
        
        // If SQLITE_TRANSIENT is not used, this BLOB may be corrupted
        // when it reaches xFilter. The test verifies we get results at all.
        var count = 0
        for _ in try db.prepare("SELECT rowid FROM vecs WHERE payload MATCH known_blob() AND k = 4") {
            count += 1
        }
        
        // The known_blob bytes don't represent a valid float32 for distance computation,
        // but the key is that xFilter receives a Blob binding (not nil/corrupted).
        // BlobMatchModule returns results based on distance comparison.
        XCTAssertEqual(count, 4, "UDF BLOB should survive constraint evaluation and reach xFilter")
    }
}
