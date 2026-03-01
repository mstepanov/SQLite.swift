//
// Connection+VirtualTable.swift
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

// MARK: - Connection Extension for Virtual Tables

extension Connection {
    
    /// Register a custom virtual table module.
    ///
    /// After registration, virtual tables can be created using:
    /// ```sql
    /// CREATE VIRTUAL TABLE name USING module_name(args...)
    /// ```
    ///
    /// - Parameter moduleType: Type conforming to `VirtualTableModule`
    /// - Throws: `Result.Error` if registration fails
    ///
    /// Example:
    /// ```swift
    /// try connection.createModule(MyVirtualTable.self)
    /// try connection.execute("CREATE VIRTUAL TABLE t USING mytable(arg)")
    /// for row in try connection.prepare("SELECT * FROM t") {
    ///     print(row)
    /// }
    /// ```
    public func createModule<M: VirtualTableModule>(_ moduleType: M.Type) throws {
        let moduleName = M.moduleName
        
        // Create factory function for this module type
        let factory: VirtualTableFactory = { arguments in
            try M(arguments: arguments)
        }
        
        // Create context holding factory
        let context = VirtualTableModuleContext(
            moduleName: moduleName,
            factory: factory,
            cursorFactory: { module in
                guard let typedModule = module as? M else {
                    throw VirtualTableError.initializationFailed("Module type mismatch")
                }
                return try typedModule.openCursor()
            },
            getColumns: { module in
                guard let typedModule = module as? M else {
                    return []
                }
                return typedModule.columns
            },
            bestIndex: { module, indexInfo in
                guard let typedModule = module as? M else {
                    return SQLITE_ERROR
                }
                return typedModule.bestIndex(&indexInfo)
            },
            destroy: { module in
                guard let typedModule = module as? M else {
                    return
                }
                try typedModule.destroy()
            }
        )
        let contextPtr = Unmanaged.passRetained(context).toOpaque()
        
        // Allocate and initialize sqlite3_module structure
        let module = UnsafeMutablePointer<sqlite3_module>.allocate(capacity: 1)
        
        // Initialize with zeros first, then set required fields
        module.pointee = sqlite3_module()
        module.pointee.iVersion = 2
        module.pointee.xCreate = vtabCreateCallback
        module.pointee.xConnect = vtabConnectCallback
        module.pointee.xBestIndex = vtabBestIndexCallback
        module.pointee.xDisconnect = vtabDisconnectCallback
        module.pointee.xDestroy = vtabDestroyCallback
        module.pointee.xOpen = vtabOpenCallback
        module.pointee.xClose = vtabCloseCallback
        module.pointee.xFilter = vtabFilterCallback
        module.pointee.xNext = vtabNextCallback
        module.pointee.xEof = vtabEofCallback
        module.pointee.xColumn = vtabColumnCallback
        module.pointee.xRowid = vtabRowidCallback
        // Read-only table: xUpdate, xBegin, xSync, xCommit, xRollback are nil (default)
        // Optional callbacks: xFindFunction, xRename, xSavepoint, xRelease, xRollbackTo, xShadowName are nil (default)
        
        // Register module with SQLite
        let result = sqlite3_create_module_v2(
            handle,
            moduleName,
            module,
            contextPtr,
            { ptr in
                // Destructor: release context
                if let ptr = ptr {
                    Unmanaged<VirtualTableModuleContext>.fromOpaque(ptr).release()
                }
            }
        )
        
        try check(result)
        
        // Store reference to module structure to prevent deallocation
        virtualTableModulesStorage[moduleName] = module
    }
    
    // Storage for registered modules (prevents deallocation)
    private var virtualTableModulesStorage: [String: UnsafeMutablePointer<sqlite3_module>] {
        get {
            objc_getAssociatedObject(self, &virtualTableModulesKey)
                as? [String: UnsafeMutablePointer<sqlite3_module>] ?? [:]
        }
        set {
            objc_setAssociatedObject(
                self,
                &virtualTableModulesKey,
                newValue,
                .OBJC_ASSOCIATION_RETAIN_NONATOMIC
            )
        }
    }
}

// Associated object key
private var virtualTableModulesKey: UInt8 = 0

// MARK: - Type-Erased Factory Types

/// Factory function for creating module instances
typealias VirtualTableFactory = ([String]) throws -> AnyObject

/// Factory function for creating cursor instances
typealias VirtualTableCursorFactory = (AnyObject) throws -> AnyObject

/// Function to get columns from module
typealias VirtualTableColumnsGetter = (AnyObject) -> [VirtualTableColumn]

/// Function to call bestIndex on module
typealias VirtualTableBestIndexFunc = (AnyObject, inout VirtualTableIndexInfo) -> Int32

/// Function to destroy module
typealias VirtualTableDestroyFunc = (AnyObject) throws -> Void

// MARK: - Context Types

/// Context for module registration (type-erased)
final class VirtualTableModuleContext {
    let moduleName: String
    let factory: VirtualTableFactory
    let cursorFactory: VirtualTableCursorFactory
    let getColumns: VirtualTableColumnsGetter
    let bestIndex: VirtualTableBestIndexFunc
    let destroy: VirtualTableDestroyFunc
    
    init(
        moduleName: String,
        factory: @escaping VirtualTableFactory,
        cursorFactory: @escaping VirtualTableCursorFactory,
        getColumns: @escaping VirtualTableColumnsGetter,
        bestIndex: @escaping VirtualTableBestIndexFunc,
        destroy: @escaping VirtualTableDestroyFunc
    ) {
        self.moduleName = moduleName
        self.factory = factory
        self.cursorFactory = cursorFactory
        self.getColumns = getColumns
        self.bestIndex = bestIndex
        self.destroy = destroy
    }
}

/// Context for virtual table instance
final class VirtualTableInstanceContext {
    let moduleContext: VirtualTableModuleContext
    let instance: AnyObject
    
    init(moduleContext: VirtualTableModuleContext, instance: AnyObject) {
        self.moduleContext = moduleContext
        self.instance = instance
    }
}

/// Context for cursor instance
final class VirtualTableCursorContext {
    let tableContext: VirtualTableInstanceContext
    let cursor: AnyObject
    
    // Type-erased cursor operations
    var filterFunc: ((Int32, String?, [Binding?]) throws -> Void)?
    var nextFunc: (() throws -> Void)?
    var eofFunc: (() -> Bool)?
    var columnFunc: ((Int32) -> Binding?)?
    var rowidFunc: (() -> Int64)?
    var closeFunc: (() -> Void)?
    
    init(tableContext: VirtualTableInstanceContext, cursor: AnyObject) {
        self.tableContext = tableContext
        self.cursor = cursor
    }
}

// MARK: - Virtual Table Structures (extends sqlite3_vtab and sqlite3_vtab_cursor)

/// Extended virtual table structure with Swift context pointer.
/// Must have sqlite3_vtab as first member for C compatibility.
struct SwiftVirtualTable {
    var base: sqlite3_vtab      // Must be first!
    var contextPtr: UnsafeMutableRawPointer
}

/// Extended cursor structure with Swift context pointer.
/// Must have sqlite3_vtab_cursor as first member for C compatibility.
struct SwiftVirtualTableCursor {
    var base: sqlite3_vtab_cursor  // Must be first!
    var contextPtr: UnsafeMutableRawPointer
}

// MARK: - sqlite3_module Callback Implementations (C-compatible)

/// Called when CREATE VIRTUAL TABLE is executed
private let vtabCreateCallback: @convention(c) (
    OpaquePointer?,
    UnsafeMutableRawPointer?,
    Int32,
    UnsafePointer<UnsafePointer<CChar>?>?,
    UnsafeMutablePointer<UnsafeMutablePointer<sqlite3_vtab>?>?,
    UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
) -> Int32 = { db, pAux, argc, argv, ppVTab, pzErr in
    // xCreate and xConnect are identical for read-only virtual tables
    return vtabConnectCallback(db, pAux, argc, argv, ppVTab, pzErr)
}

/// Called when connecting to existing virtual table
private let vtabConnectCallback: @convention(c) (
    OpaquePointer?,
    UnsafeMutableRawPointer?,
    Int32,
    UnsafePointer<UnsafePointer<CChar>?>?,
    UnsafeMutablePointer<UnsafeMutablePointer<sqlite3_vtab>?>?,
    UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
) -> Int32 = { db, pAux, argc, argv, ppVTab, pzErr in
    guard let db = db, let pAux = pAux, let ppVTab = ppVTab else {
        return SQLITE_ERROR
    }
    
    // Parse arguments
    var arguments: [String] = []
    if let argv = argv {
        for i in 0..<Int(argc) {
            if let arg = argv[i] {
                arguments.append(String(cString: arg))
            }
        }
    }
    
    // Get module context
    let moduleContext = Unmanaged<VirtualTableModuleContext>.fromOpaque(pAux).takeUnretainedValue()
    
    do {
        // Create module instance
        let instance = try moduleContext.factory(arguments)
        
        // Get columns for schema declaration
        let columns = moduleContext.getColumns(instance)
        
        // Build CREATE TABLE statement for schema
        let columnDefs = columns.map { $0.declaration }.joined(separator: ", ")
        let createSQL = "CREATE TABLE x(\(columnDefs))"
        
        // Declare schema to SQLite
        let declareResult = sqlite3_declare_vtab(db, createSQL)
        guard declareResult == SQLITE_OK else {
            setVtabError(pzErr, "Failed to declare virtual table schema: \(createSQL)")
            return declareResult
        }
        
        // Create table context
        let tableContext = VirtualTableInstanceContext(moduleContext: moduleContext, instance: instance)
        let tableContextPtr = Unmanaged.passRetained(tableContext).toOpaque()
        
        // Allocate SwiftVirtualTable structure
        let vtab = UnsafeMutablePointer<SwiftVirtualTable>.allocate(capacity: 1)
        vtab.pointee.base = sqlite3_vtab()
        vtab.pointee.contextPtr = tableContextPtr
        
        // Cast to sqlite3_vtab pointer and return
        ppVTab.pointee = UnsafeMutableRawPointer(vtab).assumingMemoryBound(to: sqlite3_vtab.self)
        
        return SQLITE_OK
    } catch {
        setVtabError(pzErr, "Virtual table creation failed: \(error.localizedDescription)")
        return SQLITE_ERROR
    }
}

/// Called to determine best query plan
private let vtabBestIndexCallback: @convention(c) (
    UnsafeMutablePointer<sqlite3_vtab>?,
    UnsafeMutablePointer<sqlite3_index_info>?
) -> Int32 = { pVTab, pIndexInfo in
    guard let pVTab = pVTab, let pIndexInfo = pIndexInfo else {
        return SQLITE_ERROR
    }
    
    // Cast to our extended structure
    let vtab = UnsafeMutableRawPointer(pVTab).assumingMemoryBound(to: SwiftVirtualTable.self)
    let tableContext = Unmanaged<VirtualTableInstanceContext>.fromOpaque(vtab.pointee.contextPtr).takeUnretainedValue()
    
    // Build VirtualTableIndexInfo from sqlite3_index_info
    var indexInfo = VirtualTableIndexInfo()
    
    // Parse constraints
    let nConstraint = Int(pIndexInfo.pointee.nConstraint)
    if nConstraint > 0, let constraints = pIndexInfo.pointee.aConstraint {
        for i in 0..<nConstraint {
            let c = constraints[i]
            if let op = VirtualTableIndexInfo.ConstraintOp(rawValue: c.op) {
                indexInfo.constraints.append(VirtualTableIndexInfo.Constraint(
                    column: c.iColumn,
                    op: op,
                    usable: c.usable != 0
                ))
            }
        }
    }
    
    // Parse ORDER BY
    let nOrderBy = Int(pIndexInfo.pointee.nOrderBy)
    if nOrderBy > 0, let orderBy = pIndexInfo.pointee.aOrderBy {
        for i in 0..<nOrderBy {
            let o = orderBy[i]
            indexInfo.orderBy.append(VirtualTableIndexInfo.OrderBy(
                column: o.iColumn,
                desc: o.desc != 0
            ))
        }
    }
    
    // Call module's bestIndex
    let result = tableContext.moduleContext.bestIndex(tableContext.instance, &indexInfo)
    
    // Copy results back to sqlite3_index_info
    pIndexInfo.pointee.estimatedCost = indexInfo.estimatedCost
    pIndexInfo.pointee.estimatedRows = indexInfo.estimatedRows
    pIndexInfo.pointee.idxNum = indexInfo.indexNumber
    pIndexInfo.pointee.orderByConsumed = indexInfo.orderByConsumed ? 1 : 0
    
    // Set constraint usage
    if let constraintUsage = pIndexInfo.pointee.aConstraintUsage {
        for (i, constraint) in indexInfo.constraints.enumerated() where i < nConstraint {
            constraintUsage[i].argvIndex = constraint.argvIndex
            constraintUsage[i].omit = constraint.omit ? 1 : 0
        }
    }
    
    // Set index string if provided
    if let idxStr = indexInfo.indexString {
        let cString = strdup(idxStr)
        pIndexInfo.pointee.idxStr = cString
        pIndexInfo.pointee.needToFreeIdxStr = 1
    }
    
    return result
}

/// Called when disconnecting from virtual table
private let vtabDisconnectCallback: @convention(c) (
    UnsafeMutablePointer<sqlite3_vtab>?
) -> Int32 = { pVTab in
    guard let pVTab = pVTab else {
        return SQLITE_ERROR
    }
    
    // Cast to our extended structure
    let vtab = UnsafeMutableRawPointer(pVTab).assumingMemoryBound(to: SwiftVirtualTable.self)
    
    // Release table context
    Unmanaged<VirtualTableInstanceContext>.fromOpaque(vtab.pointee.contextPtr).release()
    
    // Deallocate vtab structure
    vtab.deallocate()
    
    return SQLITE_OK
}

/// Called when DROP TABLE is executed
private let vtabDestroyCallback: @convention(c) (
    UnsafeMutablePointer<sqlite3_vtab>?
) -> Int32 = { pVTab in
    guard let pVTab = pVTab else {
        return SQLITE_ERROR
    }
    
    // Cast to our extended structure
    let vtab = UnsafeMutableRawPointer(pVTab).assumingMemoryBound(to: SwiftVirtualTable.self)
    let tableContext = Unmanaged<VirtualTableInstanceContext>.fromOpaque(vtab.pointee.contextPtr).takeUnretainedValue()
    
    // Call module's destroy
    do {
        try tableContext.moduleContext.destroy(tableContext.instance)
    } catch {
        // Log error but continue with cleanup
        print("Virtual table destroy error: \(error)")
    }
    
    // Disconnect handles deallocation
    return vtabDisconnectCallback(pVTab)
}

/// Called to open a new cursor
private let vtabOpenCallback: @convention(c) (
    UnsafeMutablePointer<sqlite3_vtab>?,
    UnsafeMutablePointer<UnsafeMutablePointer<sqlite3_vtab_cursor>?>?
) -> Int32 = { pVTab, ppCursor in
    guard let pVTab = pVTab, let ppCursor = ppCursor else {
        return SQLITE_ERROR
    }
    
    // Cast to our extended structure
    let vtab = UnsafeMutableRawPointer(pVTab).assumingMemoryBound(to: SwiftVirtualTable.self)
    let tableContext = Unmanaged<VirtualTableInstanceContext>.fromOpaque(vtab.pointee.contextPtr).takeUnretainedValue()
    
    do {
        // Create cursor instance
        let cursor = try tableContext.moduleContext.cursorFactory(tableContext.instance)
        
        // Create cursor context
        let cursorContext = VirtualTableCursorContext(tableContext: tableContext, cursor: cursor)
        
        // Set up type-erased cursor operations
        if let typedCursor = cursor as? any VirtualTableCursor {
            cursorContext.filterFunc = { indexNumber, indexString, arguments in
                try typedCursor.filter(indexNumber: indexNumber, indexString: indexString, arguments: arguments)
            }
            cursorContext.nextFunc = {
                try typedCursor.next()
            }
            cursorContext.eofFunc = {
                typedCursor.eof
            }
            cursorContext.columnFunc = { index in
                typedCursor.column(index)
            }
            cursorContext.rowidFunc = {
                typedCursor.rowid
            }
            cursorContext.closeFunc = {
                typedCursor.close()
            }
        }
        
        let cursorContextPtr = Unmanaged.passRetained(cursorContext).toOpaque()
        
        // Allocate SwiftVirtualTableCursor structure
        let vtabCursor = UnsafeMutablePointer<SwiftVirtualTableCursor>.allocate(capacity: 1)
        vtabCursor.pointee.base = sqlite3_vtab_cursor()
        vtabCursor.pointee.base.pVtab = pVTab
        vtabCursor.pointee.contextPtr = cursorContextPtr
        
        // Cast to sqlite3_vtab_cursor pointer and return
        ppCursor.pointee = UnsafeMutableRawPointer(vtabCursor).assumingMemoryBound(to: sqlite3_vtab_cursor.self)
        
        return SQLITE_OK
    } catch {
        return SQLITE_ERROR
    }
}

/// Called to close cursor
private let vtabCloseCallback: @convention(c) (
    UnsafeMutablePointer<sqlite3_vtab_cursor>?
) -> Int32 = { pCursor in
    guard let pCursor = pCursor else {
        return SQLITE_ERROR
    }
    
    // Cast to our extended structure
    let cursor = UnsafeMutableRawPointer(pCursor).assumingMemoryBound(to: SwiftVirtualTableCursor.self)
    let cursorContext = Unmanaged<VirtualTableCursorContext>.fromOpaque(cursor.pointee.contextPtr).takeUnretainedValue()
    
    // Call close on cursor
    cursorContext.closeFunc?()
    
    // Release cursor context
    Unmanaged<VirtualTableCursorContext>.fromOpaque(cursor.pointee.contextPtr).release()
    
    // Deallocate cursor structure
    cursor.deallocate()
    
    return SQLITE_OK
}

/// Called to begin iteration
private let vtabFilterCallback: @convention(c) (
    UnsafeMutablePointer<sqlite3_vtab_cursor>?,
    Int32,
    UnsafePointer<CChar>?,
    Int32,
    UnsafeMutablePointer<OpaquePointer?>?
) -> Int32 = { pCursor, idxNum, idxStr, argc, argv in
    guard let pCursor = pCursor else {
        return SQLITE_ERROR
    }
    
    // Cast to our extended structure
    let cursor = UnsafeMutableRawPointer(pCursor).assumingMemoryBound(to: SwiftVirtualTableCursor.self)
    let cursorContext = Unmanaged<VirtualTableCursorContext>.fromOpaque(cursor.pointee.contextPtr).takeUnretainedValue()
    
    // Parse index string
    let indexString: String? = idxStr.map { String(cString: $0) }
    
    // Parse arguments
    var arguments: [Binding?] = []
    if let argv = argv {
        for i in 0..<Int(argc) {
            if let value = argv[i] {
                arguments.append(extractBindingFromValue(value))
            } else {
                arguments.append(nil)
            }
        }
    }
    
    // Call filter on cursor
    do {
        try cursorContext.filterFunc?(idxNum, indexString, arguments)
        return SQLITE_OK
    } catch {
        return SQLITE_ERROR
    }
}

/// Called to advance cursor
private let vtabNextCallback: @convention(c) (
    UnsafeMutablePointer<sqlite3_vtab_cursor>?
) -> Int32 = { pCursor in
    guard let pCursor = pCursor else {
        return SQLITE_ERROR
    }
    
    // Cast to our extended structure
    let cursor = UnsafeMutableRawPointer(pCursor).assumingMemoryBound(to: SwiftVirtualTableCursor.self)
    let cursorContext = Unmanaged<VirtualTableCursorContext>.fromOpaque(cursor.pointee.contextPtr).takeUnretainedValue()
    
    // Call next on cursor
    do {
        try cursorContext.nextFunc?()
        return SQLITE_OK
    } catch {
        return SQLITE_ERROR
    }
}

/// Called to check if cursor past end
private let vtabEofCallback: @convention(c) (
    UnsafeMutablePointer<sqlite3_vtab_cursor>?
) -> Int32 = { pCursor in
    guard let pCursor = pCursor else {
        return 1 // EOF
    }
    
    // Cast to our extended structure
    let cursor = UnsafeMutableRawPointer(pCursor).assumingMemoryBound(to: SwiftVirtualTableCursor.self)
    let cursorContext = Unmanaged<VirtualTableCursorContext>.fromOpaque(cursor.pointee.contextPtr).takeUnretainedValue()
    
    // Call eof on cursor
    let isEof = cursorContext.eofFunc?() ?? true
    return isEof ? 1 : 0
}

/// Called to get column value
private let vtabColumnCallback: @convention(c) (
    UnsafeMutablePointer<sqlite3_vtab_cursor>?,
    OpaquePointer?,
    Int32
) -> Int32 = { pCursor, pContext, n in
    guard let pCursor = pCursor, let pContext = pContext else {
        return SQLITE_ERROR
    }
    
    // Cast to our extended structure
    let cursor = UnsafeMutableRawPointer(pCursor).assumingMemoryBound(to: SwiftVirtualTableCursor.self)
    let cursorContext = Unmanaged<VirtualTableCursorContext>.fromOpaque(cursor.pointee.contextPtr).takeUnretainedValue()
    
    // Get column value
    let value = cursorContext.columnFunc?(n)
    
    // Set result based on value type
    setVtabResult(pContext, value)
    
    return SQLITE_OK
}

/// Called to get rowid
private let vtabRowidCallback: @convention(c) (
    UnsafeMutablePointer<sqlite3_vtab_cursor>?,
    UnsafeMutablePointer<sqlite3_int64>?
) -> Int32 = { pCursor, pRowid in
    guard let pCursor = pCursor, let pRowid = pRowid else {
        return SQLITE_ERROR
    }
    
    // Cast to our extended structure
    let cursor = UnsafeMutableRawPointer(pCursor).assumingMemoryBound(to: SwiftVirtualTableCursor.self)
    let cursorContext = Unmanaged<VirtualTableCursorContext>.fromOpaque(cursor.pointee.contextPtr).takeUnretainedValue()
    
    // Get rowid
    let rowid = cursorContext.rowidFunc?() ?? 0
    pRowid.pointee = rowid
    
    return SQLITE_OK
}

// MARK: - Helper Functions

/// Set error message for virtual table operation
private func setVtabError(_ pzErr: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?, _ message: String) {
    guard let pzErr = pzErr else { return }
    pzErr.pointee = strdup(message)
}

/// Extract Binding value from sqlite3_value
private func extractBindingFromValue(_ value: OpaquePointer) -> Binding? {
    switch sqlite3_value_type(value) {
    case SQLITE_INTEGER:
        return sqlite3_value_int64(value)
    case SQLITE_FLOAT:
        return sqlite3_value_double(value)
    case SQLITE_TEXT:
        if let text = sqlite3_value_text(value) {
            return String(cString: text)
        }
        return nil
    case SQLITE_BLOB:
        let length = Int(sqlite3_value_bytes(value))
        if let bytes = sqlite3_value_blob(value) {
            return Blob(bytes: bytes, length: length)
        }
        return nil
    case SQLITE_NULL:
        return nil
    default:
        return nil
    }
}

/// Set result value for column
private func setVtabResult(_ context: OpaquePointer, _ value: Binding?) {
    switch value {
    case let int as Int64:
        sqlite3_result_int64(context, int)
    case let int as Int:
        sqlite3_result_int64(context, Int64(int))
    case let double as Double:
        sqlite3_result_double(context, double)
    case let string as String:
        sqlite3_result_text(context, string, Int32(string.utf8.count), SQLITE_TRANSIENT)
    case let blob as Blob:
        sqlite3_result_blob(context, blob.bytes, Int32(blob.bytes.count), SQLITE_TRANSIENT)
    case .none:
        sqlite3_result_null(context)
    default:
        sqlite3_result_null(context)
    }
}
