//
// VirtualTable.swift
// SQLite.swift
//
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

import Foundation

#if SQLITE_SWIFT_STANDALONE
import sqlite3
#elseif SQLITE_SWIFT_SQLCIPHER
import SQLCipher
#elseif os(Linux)
import CSQLite
#else
import SQLite3
#endif

// MARK: - VirtualTableModule Protocol

/// Protocol for implementing custom SQLite virtual table modules.
/// Conforming types can be registered via `Connection.createModule()`.
///
/// Virtual tables allow you to create table-like objects that are backed by
/// custom Swift code rather than actual database tables. This is useful for:
/// - Streaming data sources
/// - Computed/derived data
/// - External data integration
///
/// Reference: https://sqlite.org/vtab.html
///
/// Example:
/// ```swift
/// final class MyVirtualTable: VirtualTableModule {
///     static let moduleName = "mytable"
///     var columns: [VirtualTableColumn] { ... }
///     required init(arguments: [String]) throws { ... }
///     func openCursor() throws -> MyCursor { ... }
/// }
///
/// try connection.createModule(MyVirtualTable.self)
/// try connection.execute("CREATE VIRTUAL TABLE t USING mytable(arg1, arg2)")
/// ```
public protocol VirtualTableModule: AnyObject {
    associatedtype Cursor: VirtualTableCursor
    
    /// Module name used in `CREATE VIRTUAL TABLE ... USING <name>`
    static var moduleName: String { get }
    
    /// Column definitions for this virtual table.
    /// Called after initialization to declare the table schema.
    var columns: [VirtualTableColumn] { get }
    
    /// Called when virtual table is created via `CREATE VIRTUAL TABLE`.
    ///
    /// - Parameter arguments: Arguments passed after module name.
    ///   The first three arguments are always:
    ///   - `argv[0]`: Module name
    ///   - `argv[1]`: Database name (e.g., "main", "temp")
    ///   - `argv[2]`: Table name being created
    ///   - `argv[3...]`: User-provided arguments
    ///
    /// - Throws: If creation fails
    init(arguments: [String]) throws
    
    /// Called when virtual table is destroyed via `DROP TABLE`.
    /// Default implementation does nothing.
    func destroy() throws
    
    /// Create a new cursor for iterating rows.
    /// Each SELECT query opens a new cursor.
    func openCursor() throws -> Cursor
    
    /// Estimate cost for query plan (optional optimization).
    /// Default implementation returns `SQLITE_OK` with no hints.
    ///
    /// - Parameter indexInfo: Mutable index info to fill with optimization hints
    /// - Returns: SQLite result code
    func bestIndex(_ indexInfo: inout VirtualTableIndexInfo) -> Int32
    
    /// Handle INSERT, DELETE, and UPDATE operations on the virtual table.
    ///
    /// This method is called by SQLite's xUpdate callback. The semantics depend on the arguments:
    ///
    /// - **DELETE**: `arguments.count == 1`, `arguments[0]` is the rowid to delete
    /// - **INSERT with auto rowid**: `arguments.count == nCol+2`, `arguments[0]` is nil (no old rowid),
    ///   `arguments[1]` is nil (auto-assign rowid), `arguments[2...]` are column values
    /// - **INSERT with explicit rowid**: Same as above but `arguments[1]` is the explicit rowid
    /// - **UPDATE**: `arguments.count == nCol+2`, `arguments[0]` is old rowid (non-nil),
    ///   `arguments[1]` is new rowid, `arguments[2...]` are new column values
    ///
    /// - Parameter arguments: Values as described above
    /// - Returns: The rowid of the inserted row (for INSERT), or 0 for DELETE/UPDATE
    /// - Throws: If the operation fails
    func update(_ arguments: [Binding?]) throws -> Int64
}

// Default implementations
public extension VirtualTableModule {
    func destroy() throws {
        // Default: no cleanup needed
    }
    
    func bestIndex(_ indexInfo: inout VirtualTableIndexInfo) -> Int32 {
        // Default: no optimization, full table scan
        indexInfo.estimatedCost = 1_000_000.0
        return SQLITE_OK
    }
    
    func update(_ arguments: [Binding?]) throws -> Int64 {
        // Default: read-only, reject all modifications
        throw VirtualTableError.invalidArgument("Virtual table is read-only")
    }
}

// MARK: - VirtualTableCursor Protocol

/// Protocol for virtual table cursor that iterates rows.
/// Each SELECT query creates a new cursor instance.
public protocol VirtualTableCursor: AnyObject {
    /// Begin iteration with optional filter constraints.
    ///
    /// - Parameters:
    ///   - indexNumber: Index number from `bestIndex()` (for query optimization)
    ///   - indexString: Index string from `bestIndex()` (optional)
    ///   - arguments: Constraint argument values from query WHERE clause
    ///
    /// - Throws: If filter fails
    func filter(indexNumber: Int32, indexString: String?, arguments: [Binding?]) throws
    
    /// Advance to next row.
    /// - Throws: If advancement fails
    func next() throws
    
    /// Check if cursor is past last row.
    /// Returns `true` when there are no more rows to read.
    var eof: Bool { get }
    
    /// Get value for column at index.
    ///
    /// - Parameter index: Zero-based column index
    /// - Returns: Column value, or `nil` for NULL
    func column(_ index: Int32) -> Binding?
    
    /// Get rowid for current row.
    /// Every virtual table row must have a unique 64-bit rowid.
    var rowid: Int64 { get }
    
    /// Close cursor and release resources.
    /// Called when query is complete or cancelled.
    func close()
}

// MARK: - VirtualTableColumn

/// Column definition for virtual table.
public struct VirtualTableColumn: Sendable {
    /// Column name
    public let name: String
    
    /// Column type (INTEGER, REAL, TEXT, BLOB, or empty for any)
    public let type: VirtualTableColumnType
    
    /// Whether this column is hidden from `SELECT *`
    public let hidden: Bool
    
    /// Create a column definition.
    ///
    /// - Parameters:
    ///   - name: Column name
    ///   - type: Column type
    ///   - hidden: Whether column is hidden from `SELECT *`
    public init(name: String, type: VirtualTableColumnType = .any, hidden: Bool = false) {
        self.name = name
        self.type = type
        self.hidden = hidden
    }
    
    /// SQL declaration for this column (e.g., "name TEXT HIDDEN")
    var declaration: String {
        // Quote column name for SQL (use Character version to avoid ambiguity)
        var parts = ["\"\(name.replacingOccurrences(of: "\"", with: "\"\""))\""]
        if type != .any {
            parts.append(type.rawValue)
        }
        if hidden {
            parts.append("HIDDEN")
        }
        return parts.joined(separator: " ")
    }
}

// MARK: - VirtualTableColumnType

/// Column type for virtual table columns.
public enum VirtualTableColumnType: String, Sendable {
    case integer = "INTEGER"
    case real = "REAL"
    case text = "TEXT"
    case blob = "BLOB"
    case any = ""
}

// MARK: - VirtualTableIndexInfo

/// Index info for query optimization.
/// Used in `bestIndex()` to communicate with SQLite's query planner.
public struct VirtualTableIndexInfo {
    /// Constraints from WHERE clause
    public var constraints: [Constraint]
    
    /// ORDER BY columns
    public var orderBy: [OrderBy]
    
    /// Estimated cost (lower = better)
    public var estimatedCost: Double
    
    /// Estimated number of rows
    public var estimatedRows: Int64
    
    /// Index number passed to `filter()`
    public var indexNumber: Int32
    
    /// Optional index string passed to `filter()`
    public var indexString: String?
    
    /// Whether output is already ordered
    public var orderByConsumed: Bool
    
    /// Constraint from WHERE clause
    public struct Constraint {
        /// Column index (-1 for rowid)
        public let column: Int32
        
        /// Constraint operator
        public let op: ConstraintOp
        
        /// Whether this constraint is usable
        public var usable: Bool
        
        /// Argument index for `filter()` (1-based, 0 = unused)
        public var argvIndex: Int32
        
        /// Whether to omit this constraint from SQLite's check
        public var omit: Bool
        
        public init(column: Int32, op: ConstraintOp, usable: Bool) {
            self.column = column
            self.op = op
            self.usable = usable
            self.argvIndex = 0
            self.omit = false
        }
    }
    
    /// Constraint operator types
    public enum ConstraintOp: UInt8 {
        case eq = 2          // =
        case gt = 4          // >
        case le = 8          // <=
        case lt = 16         // <
        case ge = 32         // >=
        case match = 64      // MATCH
        case like = 65       // LIKE
        case glob = 66       // GLOB
        case regexp = 67     // REGEXP
        case ne = 68         // != or <>
        case isNot = 69      // IS NOT
        case isNotNull = 70  // IS NOT NULL
        case isNull = 71     // IS NULL
        case `is` = 72       // IS
        case limit = 73      // LIMIT
        case offset = 74     // OFFSET
        case function = 150  // Function constraint
    }
    
    /// ORDER BY specification
    public struct OrderBy {
        /// Column index
        public let column: Int32
        
        /// Whether descending order
        public let desc: Bool
        
        public init(column: Int32, desc: Bool) {
            self.column = column
            self.desc = desc
        }
    }
    
    /// Create empty index info
    public init() {
        self.constraints = []
        self.orderBy = []
        self.estimatedCost = 1_000_000.0
        self.estimatedRows = 1_000_000
        self.indexNumber = 0
        self.indexString = nil
        self.orderByConsumed = false
    }
}

// MARK: - VirtualTableError

/// Errors that can occur during virtual table operations.
public enum VirtualTableError: Error, LocalizedError {
    case invalidArgument(String)
    case initializationFailed(String)
    case iterationFailed(String)
    case columnAccessFailed(String)
    
    public var errorDescription: String? {
        switch self {
        case .invalidArgument(let msg):
            return "Virtual table invalid argument: \(msg)"
        case .initializationFailed(let msg):
            return "Virtual table initialization failed: \(msg)"
        case .iterationFailed(let msg):
            return "Virtual table iteration failed: \(msg)"
        case .columnAccessFailed(let msg):
            return "Virtual table column access failed: \(msg)"
        }
    }
}

