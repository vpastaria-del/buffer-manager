#include <fcntl.h>    
#include <unistd.h>     
#include <sys/stat.h>  
#include <stdlib.h>    
#include <string.h>     
#include "storage_mgr.h"
#include "dberror.h"

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

// // Struct used to store the fHandle->mgmtInfo; holds the OS file descriptor (int)
typedef struct InternalFileHandle {
    int fd;
} InternalFileHandle;
// Struct to get the Linked list for all the open files
typedef struct OpenNode {
    char *name;
    int   fd;
    struct OpenNode *next;
} OpenNode;

static OpenNode *g_open_files = NULL;

static void addOpenFile(const char *name, int fd) {//Function to append the Linked list for opened files
    OpenNode *n = (OpenNode*)malloc(sizeof(OpenNode));
    if (n == NULL) return;
    n->name = (char*)malloc((name ? strlen(name) : 0) + 1);
    if (n->name == NULL) {
        free(n);
        return;
    }
    if (name) strcpy(n->name, name); else n->name[0] = '\0';
    n->fd = fd;
    n->next = g_open_files;
    g_open_files = n;
}

static void removeOpenFile(int fd) {//If any file is closed, remove it from the opened files linked list
    OpenNode *prev = NULL;
    OpenNode *cur = g_open_files;
    while (cur != NULL) {
        if (cur->fd == fd) {
            if (prev == NULL) g_open_files = cur->next;
            else prev->next = cur->next;
            free(cur->name);
            free(cur);
            return;
        }
        prev = cur;
        cur = cur->next;
    }
}

static int getFdByName(const char *name) {//Function to find the file in the openNode Struct
    OpenNode *p = g_open_files;
    while (p != NULL) {
        if (strcmp(p->name, name) == 0) return p->fd;
        p = p->next;
    }
    return -1;
}

// Helper Functions 

static off_t page_offset(int pageIndex) {
    return (off_t)pageIndex * (off_t)PAGE_SIZE;
}

static int get_fd(const SM_FileHandle *fh) {
    if (fh == NULL || fh->mgmtInfo == NULL) return -1;
    const InternalFileHandle *box = (const InternalFileHandle*)fh->mgmtInfo;
    return box->fd;
}

static RC updatePageCount(int fd, SM_FileHandle *fh) {
    if (fh == NULL) return RC_FILE_HANDLE_NOT_INIT;
    off_t cur = lseek(fd, 0, SEEK_CUR);
    if (cur == (off_t)-1) return RC_FILE_NOT_FOUND;

    off_t end = lseek(fd, 0, SEEK_END);
    if (end == (off_t)-1) return RC_FILE_NOT_FOUND;
    lseek(fd, cur, SEEK_SET);
    if (end <= 0) {
        fh->totalNumPages = 0;
    } else {
        off_t pages = end / PAGE_SIZE;
        if ((end % PAGE_SIZE) != 0) pages += 1;
        fh->totalNumPages = (int)pages;
    }
    return RC_OK;
}

static RC write_zero_page_fd(int fd) {
    char *zeros = (char*)malloc(PAGE_SIZE);
    if (zeros == NULL) return RC_WRITE_FAILED;
    memset(zeros, 0, PAGE_SIZE);

    // loop until PAGE_SIZE bytes written
    ssize_t total = 0;
    while (total < PAGE_SIZE) {
        ssize_t w = write(fd, zeros + total, PAGE_SIZE - total);
        if (w <= 0) { free(zeros); return RC_WRITE_FAILED; }
        total += w;
    }

    free(zeros);
    return RC_OK;
}

// Storage Manager API 

void initStorageManager(void) {
    printf("Storage Manager has been initialized.");
}

RC createPageFile(char *fileName) {
    int fd = open(fileName, O_CREAT | O_TRUNC | O_RDWR
#ifdef _WIN32
        | O_BINARY
#endif
        , 0644);
    if (fd < 0) return RC_WRITE_FAILED;

    RC rc = write_zero_page_fd(fd);
    close(fd);
    return rc;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    if (fHandle == NULL || fileName == NULL) return RC_FILE_HANDLE_NOT_INIT;

    int fd = open(fileName, O_RDWR
#ifdef _WIN32
        | O_BINARY
#endif
    );
    if (fd < 0) return RC_FILE_NOT_FOUND;

    InternalFileHandle *box = (InternalFileHandle*)malloc(sizeof(InternalFileHandle));
    if (box == NULL) { close(fd); return RC_FILE_HANDLE_NOT_INIT; }
    box->fd = fd;

    fHandle->fileName   = fileName;
    fHandle->mgmtInfo   = box;
    fHandle->curPagePos = 0;

    RC rc = updatePageCount(fd, fHandle);
    if (rc != RC_OK) {
        close(fd);
        free(box);
        fHandle->mgmtInfo = NULL;
        return rc;
    }
    if (fHandle->totalNumPages == 0) fHandle->totalNumPages = 1;

    addOpenFile(fileName, fd);
    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) return RC_FILE_HANDLE_NOT_INIT;
    int fd = get_fd(fHandle);
    if (fd >= 0) {
        close(fd);
        removeOpenFile(fd);
    }
    free(fHandle->mgmtInfo);
    fHandle->mgmtInfo = NULL;
    return RC_OK;
}

RC destroyPageFile(char *fileName) {
    if (fileName == NULL) return RC_FILE_NOT_FOUND;
    int fd_open = getFdByName(fileName);
    if (fd_open >= 0) {
        close(fd_open);
        removeOpenFile(fd_open);
    }
    return (remove(fileName) == 0) ? RC_OK : RC_FILE_NOT_FOUND;
}

// Reading File Operations 

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || memPage == NULL) return RC_FILE_HANDLE_NOT_INIT;
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) return RC_READ_NON_EXISTING_PAGE;

    int fd = get_fd(fHandle);
    off_t off = page_offset(pageNum);
    if (lseek(fd, off, SEEK_SET) == (off_t)-1) return RC_READ_NON_EXISTING_PAGE;
    ssize_t total = 0;
    while (total < PAGE_SIZE) {
        ssize_t r = read(fd, memPage + total, PAGE_SIZE - total);
        if (r < 0) return RC_READ_NON_EXISTING_PAGE;
        if (r == 0) { 
            memset(memPage + total, 0, PAGE_SIZE - total);
            break;
        }
        total += r;
    }

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

int getBlockPos(SM_FileHandle *fHandle) {
    if (fHandle == NULL) return -1;
    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

// Writing page Operations 

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || memPage == NULL) return RC_FILE_HANDLE_NOT_INIT;
    if (pageNum < 0) return RC_WRITE_FAILED;

    int fd = get_fd(fHandle);

    // If we writing beyond current capacity, grow first
    if (fHandle->totalNumPages <= pageNum) {
        RC rc = ensureCapacity(pageNum + 1, fHandle);
        if (rc != RC_OK) return rc;
    }

    if (lseek(fd, page_offset(pageNum), SEEK_SET) == (off_t)-1) return RC_WRITE_FAILED;
    ssize_t total = 0;
    while (total < PAGE_SIZE) {
        ssize_t w = write(fd, memPage + total, PAGE_SIZE - total);
        if (w <= 0) return RC_WRITE_FAILED;
        total += w;
    }

    fHandle->curPagePos = pageNum;
    (void)updatePageCount(fd, fHandle);
    if (fHandle->totalNumPages <= pageNum)
        fHandle->totalNumPages = pageNum + 1;

    return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) return RC_FILE_HANDLE_NOT_INIT;
    int fd = get_fd(fHandle);

    if (lseek(fd, 0, SEEK_END) == (off_t)-1) return RC_WRITE_FAILED;
    RC rc = write_zero_page_fd(fd);
    if (rc != RC_OK) return rc;

    return updatePageCount(fd, fHandle);
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) return RC_FILE_HANDLE_NOT_INIT;
    int fd = get_fd(fHandle);

    RC rc = updatePageCount(fd, fHandle);
    if (rc != RC_OK) return rc;
    if (numberOfPages > fHandle->totalNumPages) {
        int missing = numberOfPages - fHandle->totalNumPages;
        int i;
        for (i = 0; i < missing; i++) {
            rc = appendEmptyBlock(fHandle);
            if (rc != RC_OK) return rc;
        }
    }
    return RC_OK;
}
