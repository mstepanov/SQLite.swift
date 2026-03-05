//
// sqlite_vtab_helper.h
// SQLite.swift
//
// C shim for initializing sqlite3_module structures.
//
// Rationale: When Swift imports the C struct `sqlite3_module` and sets fields
// individually (e.g., `module.pointee.xUpdate = callback`), there is a risk of
// struct field offset mismatches between Swift's view of the struct and the
// actual C layout. This causes SQLite to read NULL from the xUpdate slot even
// though Swift wrote a valid function pointer—resulting in the runtime error
// "table may not be modified" (SQLITE_READONLY).
//
// By populating the sqlite3_module struct entirely in C code, where the
// compiler guarantees correct field offsets, we eliminate this class of bug.
//

#ifndef SQLITE_VTAB_HELPER_H
#define SQLITE_VTAB_HELPER_H

#if defined(SQLITE_HAS_CODEC)
#include <sqlcipher/sqlite3.h>
#else
#include <sqlite3.h>
#endif

/// Callback type aliases matching sqlite3_module function pointer signatures.
/// These are identical to the sqlite3_module fields but defined explicitly
/// so Swift can pass @convention(c) closures without ambiguity.

typedef int (*svh_xCreate_func)(sqlite3*, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVTab, char**);

typedef int (*svh_xConnect_func)(sqlite3*, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVTab, char**);

typedef int (*svh_xBestIndex_func)(sqlite3_vtab *pVTab, sqlite3_index_info*);

typedef int (*svh_xDisconnect_func)(sqlite3_vtab *pVTab);

typedef int (*svh_xDestroy_func)(sqlite3_vtab *pVTab);

typedef int (*svh_xOpen_func)(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor);

typedef int (*svh_xClose_func)(sqlite3_vtab_cursor*);

typedef int (*svh_xFilter_func)(sqlite3_vtab_cursor*, int idxNum,
    const char *idxStr, int argc, sqlite3_value **argv);

typedef int (*svh_xNext_func)(sqlite3_vtab_cursor*);

typedef int (*svh_xEof_func)(sqlite3_vtab_cursor*);

typedef int (*svh_xColumn_func)(sqlite3_vtab_cursor*, sqlite3_context*, int);

typedef int (*svh_xRowid_func)(sqlite3_vtab_cursor*, sqlite3_int64 *pRowid);

typedef int (*svh_xUpdate_func)(sqlite3_vtab *, int, sqlite3_value **, sqlite3_int64 *);

/// Allocate and initialize a sqlite3_module structure entirely in C.
///
/// All fields are set using C struct assignment, which guarantees correct
/// field offsets. The caller is responsible for freeing the returned pointer
/// with sqlite_vtab_free_module() or free().
///
/// Fields not provided (xBegin, xSync, xCommit, xRollback, xFindFunction,
/// xRename, xSavepoint, xRelease, xRollbackTo) are set to NULL.
///
/// @param iVersion     Module version (typically 2)
/// @param xCreate      CREATE VIRTUAL TABLE callback
/// @param xConnect     Connect to existing virtual table callback
/// @param xBestIndex   Query planner callback
/// @param xDisconnect  Disconnect callback
/// @param xDestroy     DROP TABLE callback
/// @param xOpen        Open cursor callback
/// @param xClose       Close cursor callback
/// @param xFilter      Begin iteration callback
/// @param xNext        Advance cursor callback
/// @param xEof         Check end-of-data callback
/// @param xColumn      Get column value callback
/// @param xRowid       Get rowid callback
/// @param xUpdate      INSERT/DELETE/UPDATE callback (may be NULL for read-only)
/// @return Pointer to heap-allocated sqlite3_module, or NULL on allocation failure
sqlite3_module *sqlite_vtab_create_module(
    int iVersion,
    svh_xCreate_func xCreate,
    svh_xConnect_func xConnect,
    svh_xBestIndex_func xBestIndex,
    svh_xDisconnect_func xDisconnect,
    svh_xDestroy_func xDestroy,
    svh_xOpen_func xOpen,
    svh_xClose_func xClose,
    svh_xFilter_func xFilter,
    svh_xNext_func xNext,
    svh_xEof_func xEof,
    svh_xColumn_func xColumn,
    svh_xRowid_func xRowid,
    svh_xUpdate_func xUpdate
);

/// Free a sqlite3_module structure allocated by sqlite_vtab_create_module().
///
/// @param module Pointer returned by sqlite_vtab_create_module(), or NULL (no-op)
void sqlite_vtab_free_module(sqlite3_module *module);

#endif /* SQLITE_VTAB_HELPER_H */
