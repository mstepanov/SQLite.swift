//
// sqlite_vtab_helper.c
// SQLite.swift
//
// C shim for initializing sqlite3_module structures with guaranteed-correct
// field offsets. See sqlite_vtab_helper.h for rationale.
//

#include "include/sqlite_vtab_helper.h"
#include <stdlib.h>
#include <string.h>

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
) {
    sqlite3_module *mod = (sqlite3_module *)calloc(1, sizeof(sqlite3_module));
    if (!mod) return NULL;

    // Set all fields using C struct member access — offsets are guaranteed
    // correct by the C compiler.
    mod->iVersion     = iVersion;
    mod->xCreate      = xCreate;
    mod->xConnect     = xConnect;
    mod->xBestIndex   = xBestIndex;
    mod->xDisconnect  = xDisconnect;
    mod->xDestroy     = xDestroy;
    mod->xOpen        = xOpen;
    mod->xClose       = xClose;
    mod->xFilter      = xFilter;
    mod->xNext        = xNext;
    mod->xEof         = xEof;
    mod->xColumn      = xColumn;
    mod->xRowid       = xRowid;
    mod->xUpdate      = xUpdate;

    // v2 fields (xSavepoint, xRelease, xRollbackTo) are left NULL from calloc
    // v1 optional fields (xBegin, xSync, xCommit, xRollback, xFindFunction,
    //   xRename) are also left NULL from calloc

    return mod;
}

void sqlite_vtab_free_module(sqlite3_module *module) {
    free(module);
}
