// buffer_mgr.c — Buffer Manager implementation for CS525 Assign 2
// Implements FIFO and LRU; integrates with the provided Storage Manager.
// Author: (your name)

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "dt.h"

// -----------------------------
// Internal data structures
// -----------------------------

typedef struct Frame {
    PageNumber pageNum;     // disk page number loaded in this frame (NO_PAGE if empty)
    char *data;             // pointer to backing store for page data; index 1..PAGE_SIZE used
    bool dirty;             // dirty flag
    int  fixCount;          // number of pins
    unsigned long long seq; // sequence number (FIFO arrival order)
    unsigned long long lru; // last-used tick (LRU)
} Frame;

typedef struct PoolMgmt {
    SM_FileHandle fh;            // open file handle for the page file
    Frame *frames;               // array of frames of length bm->numPages
    int capacity;                // == bm->numPages (cached)

    // statistics
    int numReadIO;               // pages read from disk since init
    int numWriteIO;              // pages written to disk since init

    // replacement helpers
    unsigned long long tick;     // global increasing counter for FIFO/LRU
} PoolMgmt;

// -----------------------------
// Local helpers (prototypes)
// -----------------------------
static PoolMgmt *mgmt(BM_BufferPool *const bm);
static int findFrameIndexByPage(PoolMgmt *pm, PageNumber p);
static int findEmptyFrameIndex(PoolMgmt *pm);
static int pickVictim(PoolMgmt *pm, ReplacementStrategy strat);
static RC evictIfNeededAndLoad(PoolMgmt *pm, int fidx, PageNumber pageNum);
static RC flushFrameIfDirty(PoolMgmt *pm, Frame *fr);
static void touchForLRU(PoolMgmt *pm, Frame *fr);

// -----------------------------
// Helper implementations
// -----------------------------
static PoolMgmt *mgmt(BM_BufferPool *const bm) {
    return (PoolMgmt*)bm->mgmtData;
}

static int findFrameIndexByPage(PoolMgmt *pm, PageNumber p) {
    for (int i = 0; i < pm->capacity; i++) {
        if (pm->frames[i].pageNum == p) return i;
    }
    return -1;
}

static int findEmptyFrameIndex(PoolMgmt *pm) {
    for (int i = 0; i < pm->capacity; i++) {
        if (pm->frames[i].pageNum == NO_PAGE) return i;
    }
    return -1;
}

// Choose a victim frame index with fixCount==0 according to strategy; return -1 if none.
static int pickVictim(PoolMgmt *pm, ReplacementStrategy strat) {
    int victim = -1;
    unsigned long long best = ULLONG_MAX;

    for (int i = 0; i < pm->capacity; i++) {
        Frame *fr = &pm->frames[i];
        if (fr->fixCount != 0) continue; // only evict unpinned

        unsigned long long key;
        switch (strat) {
            case RS_FIFO:
                key = fr->seq; // smaller seq => older arrival
                break;
            case RS_LRU:
                key = fr->lru; // smaller lru => least recently used
                break;
            default:
                // Optional strategies not implemented; fall back to LRU semantics
                key = fr->lru;
                break;
        }
        if (key < best) { best = key; victim = i; }
    }
    return victim;
}

static RC flushFrameIfDirty(PoolMgmt *pm, Frame *fr) {
    if (fr->pageNum == NO_PAGE) return RC_OK; // nothing to do
    if (!fr->dirty) return RC_OK;
    RC rc = writeBlock(fr->pageNum, &pm->fh, fr->data + 1);
    if (rc != RC_OK) return rc;
    pm->numWriteIO += 1;
    fr->dirty = FALSE;
    return RC_OK;
}

static RC evictIfNeededAndLoad(PoolMgmt *pm, int fidx, PageNumber pageNum) {
    Frame *fr = &pm->frames[fidx];

    // If this frame currently holds a page, potentially flush if dirty
    if (fr->pageNum != NO_PAGE) {
        RC rc = flushFrameIfDirty(pm, fr);
        if (rc != RC_OK) return rc;
    }

    // Ensure file has enough pages
    RC rc = RC_OK;
    if (pageNum >= pm->fh.totalNumPages) {
        rc = ensureCapacity(pageNum + 1, &pm->fh);
        if (rc != RC_OK) return rc;
    }
    // Load requested page into the frame
    rc = readBlock(pageNum, &pm->fh, fr->data + 1);
    if (rc != RC_OK) return rc;
    pm->numReadIO += 1;

    fr->pageNum = pageNum;
    fr->dirty = FALSE;
    fr->fixCount = 0; // pinPage will set to 1 after this call
    fr->seq = ++pm->tick; // arrival sequence for FIFO
    fr->lru = ++pm->tick; // treat load as a use for LRU
    return RC_OK;
}

static void touchForLRU(PoolMgmt *pm, Frame *fr) {
    fr->lru = ++pm->tick;
}

// -----------------------------
// Public Buffer Pool API
// -----------------------------

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData) {
    (void)stratData; // not used for FIFO/LRU
    if (bm == NULL || pageFileName == NULL || numPages <= 0) return RC_FILE_HANDLE_NOT_INIT;

    PoolMgmt *pm = (PoolMgmt*)calloc(1, sizeof(PoolMgmt));
    if (!pm) return RC_FILE_HANDLE_NOT_INIT;

    RC rc = openPageFile((char*)pageFileName, &pm->fh);
    if (rc != RC_OK) { free(pm); return rc; }

    pm->capacity = numPages;
    pm->frames = (Frame*)calloc(numPages, sizeof(Frame));
    if (!pm->frames) { closePageFile(&pm->fh); free(pm); return RC_FILE_HANDLE_NOT_INIT; }

    // allocate backing storage for each frame; use 1-based addressing to match provided printers
    for (int i = 0; i < numPages; i++) {
        pm->frames[i].data = (char*)malloc(PAGE_SIZE + 1);
        if (!pm->frames[i].data) {
            // clean up partial allocation
            for (int j = 0; j < i; j++) free(pm->frames[j].data);
            free(pm->frames);
            closePageFile(&pm->fh);
            free(pm);
            return RC_FILE_HANDLE_NOT_INIT;
        }
        pm->frames[i].pageNum = NO_PAGE;
        pm->frames[i].dirty = FALSE;
        pm->frames[i].fixCount = 0;
        pm->frames[i].seq = 0;
        pm->frames[i].lru = 0;
    }

    pm->numReadIO = 0;
    pm->numWriteIO = 0;
    pm->tick = 0ULL;

    bm->pageFile = (char*)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = pm;

    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm) {
    if (!bm || !bm->mgmtData) return RC_FILE_HANDLE_NOT_INIT;
    PoolMgmt *pm = mgmt(bm);

    // Flush only unpinned dirty frames; allow shutdown even if some pages remain pinned
    for (int i = 0; i < pm->capacity; i++) {
        Frame *fr = &pm->frames[i];
        if (fr->pageNum != NO_PAGE && fr->dirty && fr->fixCount == 0) {
            RC rc = flushFrameIfDirty(pm, fr);
            if (rc != RC_OK) return rc;
        }
    }

    // free memory
    for (int i = 0; i < pm->capacity; i++) {
        free(pm->frames[i].data);
        pm->frames[i].data = NULL;
    }
    free(pm->frames);
    pm->frames = NULL;

    RC rc = closePageFile(&pm->fh);
    free(pm);
    bm->mgmtData = NULL;

    return rc;
}

RC forceFlushPool(BM_BufferPool *const bm) {
    if (!bm || !bm->mgmtData) return RC_FILE_HANDLE_NOT_INIT;
    PoolMgmt *pm = mgmt(bm);

    for (int i = 0; i < pm->capacity; i++) {
        Frame *fr = &pm->frames[i];
        if (fr->pageNum != NO_PAGE && fr->dirty && fr->fixCount == 0) {
            RC rc = flushFrameIfDirty(pm, fr);
            if (rc != RC_OK) return rc;
        }
    }
    return RC_OK;
}

// -----------------------------
// Page Access API
// -----------------------------

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (!bm || !bm->mgmtData || !page) return RC_FILE_HANDLE_NOT_INIT;
    PoolMgmt *pm = mgmt(bm);
    int idx = findFrameIndexByPage(pm, page->pageNum);
    if (idx < 0) return RC_READ_NON_EXISTING_PAGE;
    pm->frames[idx].dirty = TRUE;
    return RC_OK;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (!bm || !bm->mgmtData || !page) return RC_FILE_HANDLE_NOT_INIT;
    PoolMgmt *pm = mgmt(bm);
    int idx = findFrameIndexByPage(pm, page->pageNum);
    if (idx < 0) return RC_READ_NON_EXISTING_PAGE;
    if (pm->frames[idx].fixCount > 0) pm->frames[idx].fixCount -= 1;
    return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (!bm || !bm->mgmtData || !page) return RC_FILE_HANDLE_NOT_INIT;
    PoolMgmt *pm = mgmt(bm);
    int idx = findFrameIndexByPage(pm, page->pageNum);
    if (idx < 0) return RC_READ_NON_EXISTING_PAGE;
    RC rc = flushFrameIfDirty(pm, &pm->frames[idx]);
    return rc;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
           const PageNumber pageNum) {
    if (!bm || !bm->mgmtData || !page) return RC_FILE_HANDLE_NOT_INIT;
    PoolMgmt *pm = mgmt(bm);

    if (pageNum < 0) return RC_READ_NON_EXISTING_PAGE;

    // If already cached, bump fixCount and return
    int idx = findFrameIndexByPage(pm, pageNum);
    if (idx >= 0) {
        Frame *fr = &pm->frames[idx];
        fr->fixCount += 1;
        touchForLRU(pm, fr);
        page->pageNum = pageNum;
        page->data = fr->data + 1;
        return RC_OK;
    }

    // Not cached: try to find an empty frame first
    idx = findEmptyFrameIndex(pm);

    // If no empty frame, select a victim according to strategy
    if (idx < 0) {
        idx = pickVictim(pm, bm->strategy);
        if (idx < 0) {
            // No evictable frame (all pinned)
            return RC_WRITE_FAILED; // reuse error code to signal inability to pin
        }
    }

    // Evict if needed and load requested page
    RC rc = evictIfNeededAndLoad(pm, idx, pageNum);
    if (rc != RC_OK) return rc;

    // Pin and return handle
    Frame *fr = &pm->frames[idx];
    fr->fixCount = 1;
    touchForLRU(pm, fr);

    page->pageNum = pageNum;
    page->data = fr->data + 1;
    return RC_OK;
}

// -----------------------------
// Statistics API
// -----------------------------

PageNumber *getFrameContents(BM_BufferPool *const bm) {
    if (!bm || !bm->mgmtData) return NULL;
    PoolMgmt *pm = mgmt(bm);
    PageNumber *arr = (PageNumber*)malloc(sizeof(PageNumber) * pm->capacity);
    for (int i = 0; i < pm->capacity; i++) arr[i] = pm->frames[i].pageNum;
    return arr;
}

bool *getDirtyFlags(BM_BufferPool *const bm) {
    if (!bm || !bm->mgmtData) return NULL;
    PoolMgmt *pm = mgmt(bm);
    bool *arr = (bool*)malloc(sizeof(bool) * pm->capacity);
    for (int i = 0; i < pm->capacity; i++) arr[i] = pm->frames[i].dirty ? TRUE : FALSE;
    return arr;
}

int *getFixCounts(BM_BufferPool *const bm) {
    if (!bm || !bm->mgmtData) return NULL;
    PoolMgmt *pm = mgmt(bm);
    int *arr = (int*)malloc(sizeof(int) * pm->capacity);
    for (int i = 0; i < pm->capacity; i++) arr[i] = pm->frames[i].fixCount;
    return arr;
}

int getNumReadIO(BM_BufferPool *const bm) {
    if (!bm || !bm->mgmtData) return -1;
    return mgmt(bm)->numReadIO;
}

int getNumWriteIO(BM_BufferPool *const bm) {
    if (!bm || !bm->mgmtData) return -1;
    return mgmt(bm)->numWriteIO;
}
