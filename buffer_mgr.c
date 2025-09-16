#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "dt.h"
typedef struct Frame { // It temprorarily holds the page data in the buffer pool from the disk
    PageNumber pageNum;     
    char *data;            
    bool dirty;           
    int  fixCount;        
    unsigned long long seq; 
    unsigned long long lru; 
} Frame;

typedef struct PoolMgmt { // It tracks the file,frame,capacity and I/O results
    SM_FileHandle fh;            
    Frame *frames;              
    int capacity;             
    int numReadIO;               
    int numWriteIO;             
    unsigned long long tick;     
} PoolMgmt;

static PoolMgmt *mgmt(BM_BufferPool *const bm);// get PoolMgmt struct from BM_BufferPool
static int findFrameIndexByPage(PoolMgmt *pm, PageNumber p);//  find the index of a frame that holds the given page
static int findEmptyFrameIndex(PoolMgmt *pm);//find an unused (empty) frame index
static int pickVictim(PoolMgmt *pm, ReplacementStrategy strat); //choose a frame to remove based on FIFO/LRU
static RC evictIfNeededAndLoad(PoolMgmt *pm, int fidx, PageNumber pageNum);//remove old page (if needed) and load a new one into frame
static RC flushFrameIfDirty(PoolMgmt *pm, Frame *fr);// write frame back to disk if it’s dirty
static void touchForLRU(PoolMgmt *pm, Frame *fr);//update LRU timestamp 

static PoolMgmt *mgmt(BM_BufferPool *const bm) {
    return (PoolMgmt*)bm->mgmtData;
}

static int findFrameIndexByPage(PoolMgmt *pm, PageNumber p) {
    // simple linear scan
    for (int i = 0; i < pm->capacity; i++) {
        if (pm->frames[i].pageNum == p) {
            return i;
        }
    }
    return -1;
}

static int findEmptyFrameIndex(PoolMgmt *pm) {
    // first slot whose pageNum is NO_PAGE
    for (int i = 0; i < pm->capacity; i++) {
        if (pm->frames[i].pageNum == NO_PAGE) {
            return i;
        }
    }
    return -1;
}

static int pickVictim(PoolMgmt *pm, ReplacementStrategy strat) {
    int firstFree = -1;

    for (int i = 0; i < pm->capacity; i++) {
        if (pm->frames[i].fixCount == 0 && pm->frames[i].pageNum != NO_PAGE) {
            firstFree = i;
            break;
        }
    }
    if (firstFree < 0) return -1;

    int victim = firstFree;
    unsigned long long bestKey = ULLONG_MAX;

    for (int i = 0; i < pm->capacity; i++) {
        Frame *fr = &pm->frames[i];
        if (fr->fixCount != 0 || fr->pageNum == NO_PAGE) continue;

        unsigned long long key;
        switch (strat) {
            case RS_FIFO:
                key = fr->seq; 
                break;
            case RS_LRU:
                key = fr->lru; 
                break;
            default:
             
                key = fr->lru;
                break;
        }

        if (key < bestKey) {
            bestKey = key;
            victim = i;
        }
    }
    return victim;
}

static RC flushFrameIfDirty(PoolMgmt *pm, Frame *fr) {
    if (fr->pageNum == NO_PAGE) return RC_OK; 
    if (!fr->dirty) return RC_OK;

    // write the page back
    RC rc = writeBlock(fr->pageNum, &pm->fh, fr->data + 1);
    if (rc != RC_OK) return rc;

    pm->numWriteIO += 1;
    fr->dirty = FALSE;
    return RC_OK;
}

static RC evictIfNeededAndLoad(PoolMgmt *pm, int fidx, PageNumber pageNum) {
    Frame *fr = &pm->frames[fidx];
    if (fr->pageNum != NO_PAGE) {
        RC rcFlush = flushFrameIfDirty(pm, fr);
        if (rcFlush != RC_OK) return rcFlush;
    }
    if (pageNum >= pm->fh.totalNumPages) {
        RC rcCap = ensureCapacity(pageNum + 1, &pm->fh);
        if (rcCap != RC_OK) return rcCap;
    }
    RC rcRead = readBlock(pageNum, &pm->fh, fr->data + 1);
    if (rcRead != RC_OK) return rcRead;

    pm->numReadIO += 1;

    // reset frame metadata in a simple way
    fr->pageNum = pageNum;
    fr->dirty = FALSE;
    fr->fixCount = 0;
    pm->tick += 1;
    fr->seq = pm->tick;   // when it was loaded
    fr->lru = pm->tick;   // most recent "use" time
    return RC_OK;
}

static void touchForLRU(PoolMgmt *pm, Frame *fr) {
    pm->tick += 1;
    fr->lru = pm->tick;
}

// Public Buffer Pool API
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData) {
    (void)stratData; // not used for FIFO/LRU
    if (bm == NULL || pageFileName == NULL || numPages <= 0) return RC_FILE_HANDLE_NOT_INIT;

    PoolMgmt *pm = (PoolMgmt*)calloc(1, sizeof(PoolMgmt));
    if (pm == NULL) return RC_FILE_HANDLE_NOT_INIT;

    RC rcOpen = openPageFile((char*)pageFileName, &pm->fh);
    if (rcOpen != RC_OK) {
        free(pm);
        return rcOpen;
    }

    pm->capacity = numPages;
    pm->frames = (Frame*)calloc(numPages, sizeof(Frame));
    if (pm->frames == NULL) {
        closePageFile(&pm->fh);
        free(pm);
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // allocate backing storage for each frame; use 1-based addressing to match provided printers
    for (int i = 0; i < numPages; i++) {
        pm->frames[i].data = (char*)malloc(PAGE_SIZE + 1);
        if (pm->frames[i].data == NULL) {
            for (int j = 0; j < i; j++) {
                free(pm->frames[j].data);
            }
            free(pm->frames);
            closePageFile(&pm->fh);
            free(pm);
            return RC_FILE_HANDLE_NOT_INIT;
        }
        pm->frames[i].pageNum  = NO_PAGE;
        pm->frames[i].dirty    = FALSE;
        pm->frames[i].fixCount = 0;
        pm->frames[i].seq      = 0;
        pm->frames[i].lru      = 0;
    }

    pm->numReadIO  = 0;
    pm->numWriteIO = 0;
    pm->tick       = 0ULL;

    bm->pageFile = (char*)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = pm;

    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return RC_FILE_HANDLE_NOT_INIT;
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
        if (pm->frames[i].data) {
            free(pm->frames[i].data);
            pm->frames[i].data = NULL;
        }
    }
    free(pm->frames);
    pm->frames = NULL;

    RC rcClose = closePageFile(&pm->fh);
    free(pm);
    bm->mgmtData = NULL;

    return rcClose;
}

RC forceFlushPool(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return RC_FILE_HANDLE_NOT_INIT;
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

// Page Access API

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm == NULL || bm->mgmtData == NULL || page == NULL) return RC_FILE_HANDLE_NOT_INIT;
    PoolMgmt *pm = mgmt(bm);
    int idx = findFrameIndexByPage(pm, page->pageNum);
    if (idx < 0) return RC_READ_NON_EXISTING_PAGE;
    pm->frames[idx].dirty = TRUE;
    return RC_OK;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm == NULL || bm->mgmtData == NULL || page == NULL) return RC_FILE_HANDLE_NOT_INIT;
    PoolMgmt *pm = mgmt(bm);
    int idx = findFrameIndexByPage(pm, page->pageNum);
    if (idx < 0) return RC_READ_NON_EXISTING_PAGE;

    // student: just decrement if positive
    if (pm->frames[idx].fixCount > 0) {
        pm->frames[idx].fixCount -= 1;
    }
    // touching LRU on unpin is optional; keep it simple and don't bump here
    return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm == NULL || bm->mgmtData == NULL || page == NULL) return RC_FILE_HANDLE_NOT_INIT;
    PoolMgmt *pm = mgmt(bm);
    int idx = findFrameIndexByPage(pm, page->pageNum);
    if (idx < 0) return RC_READ_NON_EXISTING_PAGE;
    return flushFrameIfDirty(pm, &pm->frames[idx]);
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
           const PageNumber pageNum) {
    if (bm == NULL || bm->mgmtData == NULL || page == NULL) return RC_FILE_HANDLE_NOT_INIT;
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
    RC rcLoad = evictIfNeededAndLoad(pm, idx, pageNum);
    if (rcLoad != RC_OK) return rcLoad;

    // Pin and return handle
    Frame *fr = &pm->frames[idx];
    fr->fixCount = 1;
    touchForLRU(pm, fr);

    page->pageNum = pageNum;
    page->data = fr->data + 1;
    return RC_OK;
}

// Statistics API

PageNumber *getFrameContents(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return NULL;
    PoolMgmt *pm = mgmt(bm);
    PageNumber *arr = (PageNumber*)malloc(sizeof(PageNumber) * pm->capacity);
    if (arr == NULL) return NULL;
    for (int i = 0; i < pm->capacity; i++) {
        arr[i] = pm->frames[i].pageNum;
    }
    return arr;
}

bool *getDirtyFlags(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return NULL;
    PoolMgmt *pm = mgmt(bm);
    bool *arr = (bool*)malloc(sizeof(bool) * pm->capacity);
    if (arr == NULL) return NULL;
    for (int i = 0; i < pm->capacity; i++) {
        arr[i] = pm->frames[i].dirty ? TRUE : FALSE;
    }
    return arr;
}

int *getFixCounts(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return NULL;
    PoolMgmt *pm = mgmt(bm);
    int *arr = (int*)malloc(sizeof(int) * pm->capacity);
    if (arr == NULL) return NULL;
    for (int i = 0; i < pm->capacity; i++) {
        arr[i] = pm->frames[i].fixCount;
    }
    return arr;
}

int getNumReadIO(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return -1;
    return mgmt(bm)->numReadIO;
}

int getNumWriteIO(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return -1;
    return mgmt(bm)->numWriteIO;
}
