/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG


namespace badgerdb
{

    // -----------------------------------------------------------------------------
    // BTreeIndex::BTreeIndex -- Constructor
    // -----------------------------------------------------------------------------

    BTreeIndex::BTreeIndex(const std::string & relationName, std::string & outIndexName, BufMgr *bufMgrIn,
            const int attrByteOffset, const Datatype attrType) {
        // bufferMgr
        bufMgr = bufMgrIn;
        leafOccupancy = INTARRAYLEAFSIZE;
        nodeOccupancy = INTARRAYNONLEAFSIZE;
        scanExecuting = false;

        // retrieve index name
        std::ostringstream idxStr;
        idxStr << relationName << "." << attrByteOffset;
        outIndexName = idxStr.str();

        // loading file
        try {
            file = new BlobFile(outIndexName, false);
            // read metadata
            Page *headerPage;
            headerPageNum = file->getFirstPageNo();
            bufMgr->readPage(file, headerPageNum, headerPage);
            IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
            rootPageNum = meta->rootPageNo;

            //check if index info matches
            if (meta->relationName != relationName || meta-> attrType != attrType || meta->attrByteOffset != attrByteOffset)
                throw BadIndexInfoException(outIndexName);
            bufMgr->unPinPage(file, headerPageNum, false);
        } catch(FileNotFoundException e) { // if blob file does not exist
            // create a new blob file
            file = new BlobFile(outIndexName, true);
            //allocate header and root page
            Page *rootPage;
            Page *headerPage;
            bufMgr->allocPage(file, headerPageNum, headerPage);
            bufMgr->allocPage(file, rootPageNum, rootPage);

            //write metadata
            IndexMetaInfo *metadata = (IndexMetaInfo *)headerPage;
            metadata->attrType = attrType;
            metadata->attrByteOffset = attrByteOffset;
            metadata->rootPageNo = rootPageNum;
            strncpy((char *)(&(metadata->relationName)), relationName.c_str(), 20);
            metadata->relationName[19] = 0;
            initRootPageNo = rootPageNum;

            // initialize root node
            LeafNodeInt *root = (LeafNodeInt *)rootPage;
            root->rightSibPageNo = 0;
            bufMgr->unPinPage(file, rootPageNum, true);
            bufMgr->unPinPage(file, headerPageNum, true);

            //write to blob file
            FileScan fc(relationName, bufMgr);
            RecordId rid;
            try {
                while(true) {
                    fc.scanNext(rid);
                    std::string record = fc.getRecord();
                    insertEntry(record.c_str() + attrByteOffset, rid);
                }
            } catch(EndOfFileException e) {
                // save b-tree index file to disk
                bufMgr->flushFile(file);
            }
        }
    }


    // -----------------------------------------------------------------------------
    // BTreeIndex::~BTreeIndex -- destructor
    // -----------------------------------------------------------------------------

    BTreeIndex::~BTreeIndex() {
        scanExecuting = false;
        bufMgr->flushFile(BTreeIndex::file);
        delete file;
        file = nullptr;
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::insertEntry
    // -----------------------------------------------------------------------------

    const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
        RIDKeyPair<int> dataEntry;
        dataEntry.set(rid, *((int *) key));
        Page* root; // root

        // pin root
        bufMgr->readPage(file, rootPageNum, root);
        PageKeyPair<int> *newChildEntry = nullptr;
        bool isInitRoot = rootPageNum == initRootPageNo? true: false;
        recInsert(root, rootPageNum, isInitRoot, dataEntry, newChildEntry);
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::startScan
    // -----------------------------------------------------------------------------
    const void BTreeIndex::startScan(const void* lowValParm, const Operator lowOpParm,
                                     const void* highValParm, const Operator highOpParm){
        lowValInt = *((int *)lowValParm);
        highValInt = *((int *)highValParm);
        // scan for illegal operators or parameters
        if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm!= LT && highOpParm != LTE))
            throw BadOpcodesException();
        if (lowValInt > highValInt)
            throw BadScanrangeException();
        // kill all existing scans
        if (scanExecuting)
            endScan();
        // init scanning
        currentPageNum = rootPageNum;
        bufMgr->readPage(file, currentPageNum, currentPageData); //FIXME page data defined?
        if (initRootPageNo != rootPageNum) { // if root is internal
            NonLeafNodeInt* curNode = (NonLeafNodeInt *) currentPageData;
            bool atLeafLevelNext = false;
            while (!atLeafLevelNext) {
                curNode = (NonLeafNodeInt *) currentPageData;
                if (curNode -> level == 1)
                    atLeafLevelNext = true;

                //looking for target subtree
                int i = nodeOccupancy;
                while (i >= 0 && curNode->pageNoArray[i] == 0) {
                    i--;
                }
                while (i > 0 && curNode->keyArray[i-1] >= lowValInt) { //FIXME double check here lowvalint
                    i--;
                }
                PageId nextPageNum = curNode->pageNoArray[i]; //target subtree
                bufMgr->unPinPage(file, currentPageNum, false);
                currentPageNum = nextPageNum;
                bufMgr->readPage(file, currentPageNum, currentPageData);
            }
        }

        // when target leaf node is found
        bool targetFound = false;
        while (!targetFound) {
            LeafNodeInt* curNode = (LeafNodeInt *) currentPageData;
            if (curNode->ridArray[0].page_number == 0) { //if leaf node is vacant
                bufMgr->unPinPage(file, currentPageNum, false);
                throw NoSuchKeyFoundException();
            } else { //FIXME made mod here
                //traverse through the leaf node
                // bool nullVal = false; //FIXME
                for (int i = 0; i < leafOccupancy; i++) {
                    int curKey = curNode->keyArray[i];
                    // prevent comparing with null
                    if (i < leafOccupancy - 1 && curNode->ridArray[i+1].page_number == 0) {
                        //target not found
                        // nullVal = true;
                        break;
                    } else if (keyIsInRange(lowValInt, lowOp, highValInt, highOp, curKey)) {
                        //if current key satisfies the query
                        targetFound = true;
                        nextEntry = i;
                        scanExecuting = true;
                        break;
                    } else if((highOp == LT && curKey >= highValInt) || (highOp == LTE) && curKey > highValInt) {
                        // if no matching key is in range
                        bufMgr->unPinPage(file, currentPageNum, false);
                        throw NoSuchKeyFoundException();
                    } else {
                        // if traversed through the leaf and still in range
                        // proceed to the right sibling
                        currentPageNum = curNode->rightSibPageNo;
                        bufMgr->readPage(file, currentPageNum, currentPageData);
                    }
                }
            }
        }
    }


    // -----------------------------------------------------------------------------
    // BTreeIndex::scanNext
    // -----------------------------------------------------------------------------

    const void BTreeIndex::scanNext(RecordId& outRid){
        if (!scanExecuting)
            throw ScanNotInitializedException();
        LeafNodeInt* curNode = (LeafNodeInt *) currentPageData;
        // check for scan completeness
        if (curNode->ridArray[nextEntry].page_number == 0 || nextEntry == leafOccupancy) { //fully scanned current node
            bufMgr->unPinPage(file, currentPageNum, false);
            if (curNode->rightSibPageNo == 0)
                throw IndexScanCompletedException(); // scan is complete
            else { // scan is incomplete
                // proceed to right sibling
                currentPageNum = curNode->rightSibPageNo;
                bufMgr->readPage(file, currentPageNum, currentPageData);
                nextEntry = 0; // reset
            }
        }
        // check for un-scanned keys in current leaf, if any
        int curKey = curNode->keyArray[nextEntry];
        if (keyIsInRange(lowValInt, lowOp, highValInt, highOp, curKey)) {
            // if key is in range
            outRid = curNode->ridArray[nextEntry];
            nextEntry++;
        } else {
            throw IndexScanCompletedException(); // scan complete
        }
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::endScan
    // -----------------------------------------------------------------------------
    //
    const void BTreeIndex::endScan() {
        if (!scanExecuting)
            throw ScanNotInitializedException();
        scanExecuting = false;
        //free buffer and reset variables
        bufMgr->unPinPage(file, currentPageNum, false);
        currentPageNum = static_cast<PageId>(-1);
        currentPageData = nullptr;
        nextEntry = -1;
    }

    const void BTreeIndex::recInsert(Page *curPage, PageId curPageNo, bool nodeIsLeaf,
            const RIDKeyPair<int> dataEntry, PageKeyPair<int> *&newChildEntry) {
        // if current node is internal
        if (!nodeIsLeaf) {
            NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
            // choose subtree
            Page *nextPage;
            PageId nextPageNo;
            findSubtree(curNode, nextPageNo, dataEntry.key);
            bufMgr->readPage(file, nextPageNo, nextPage);
            nodeIsLeaf = curNode->level == 1; //FIXME: assumeing <=3 layers?
            recInsert(nextPage, nextPageNo, nodeIsLeaf, dataEntry, newChildEntry);

            //splitting
            if (newChildEntry == nullptr) { //if no splitting occurred
                //unpin and return
                bufMgr->unPinPage(file, curPageNo, false);
                return; //FIXME: return here?
            } else { //if splitting occurred
                // if able to fill at the same level
                if (curNode->pageNoArray[nodeOccupancy] == 0) {
                    insertNonLeafNode(curNode, newChildEntry);
                    newChildEntry = nullptr;
                    bufMgr->unPinPage(file, curPageNo, true);
                } else { // if no more space at the same level
                    splitNonLeafNode(curNode, curPageNo, newChildEntry);
                }
            }
        } else { // if current node is a leaf
            LeafNodeInt *leaf = (LeafNodeInt *)curPage;
            if (leaf->ridArray[leafOccupancy - 1].page_number == 0) { //FIXME why -1, add comment
                //FIXME implement insert leaf node
                bufMgr->unPinPage(file, curPageNo, true);
                newChildEntry = nullptr;
            } else { // if leaf node is full
                //split such leaf
               splitLeafNode(leaf, curPageNo, newChildEntry, dataEntry);
            }
        }
    }

    const void BTreeIndex::insertNonLeafNode(NonLeafNodeInt *curNode, PageKeyPair<int> *newChildEntry) {
        int i = nodeOccupancy;
        while(i >= 0 && curNode->pageNoArray[i] == 0) {
            i--;
        }

        //shift FIXME
        while (i > 0 && (curNode->keyArray[i-1] > newChildEntry->key)) {
            curNode->keyArray[i] = curNode->keyArray[i-1]; //FIXME: should it be i+1?????
            curNode->pageNoArray[i+1] = curNode->pageNoArray[i]; //FIXME
            i--;
        }
        //insert new entry
        curNode->keyArray[i] = newChildEntry->key;
        curNode->pageNoArray[i+1] = newChildEntry->pageNo;
        return;
    }


    const void BTreeIndex::insertLeafNode(LeafNodeInt *leafNode, RIDKeyPair<int> dataEntry) {
        if (leafNode->ridArray[0].page_number == 0) { //if leaf is vacant
            leafNode->keyArray[0] = dataEntry.key;
            leafNode->ridArray[0] = dataEntry.rid;
        } else { //if leaf is occupied
            int i = leafOccupancy - 1;
            // locate slot to insert
            while (i >= 0 && leafNode->ridArray[i].page_number == 0) {
                i--;
            }
            //shift existing elements to the left
            while (i>=0 && leafNode->keyArray[i] > dataEntry.key) { //FIXME this is not right
                leafNode->keyArray[i+1] = leafNode->keyArray[i];
                leafNode->ridArray[i+1] = leafNode->ridArray[i];
            }
            //insert new entry
            leafNode->keyArray[i+1] = dataEntry.key;
            leafNode->ridArray[i+1] = dataEntry.rid;
        }
    }

    const void BTreeIndex::splitLeafNode(LeafNodeInt *leafNode, PageId leafPageNo, PageKeyPair<int> *&newChildEntry,
                                         const RIDKeyPair<int> dataEntry) {
        //allocating for a new leaf node
        Page *newPage;
        PageId newPageNo;
        bufMgr->allocPage(file, newPageNo, newPage);
        LeafNodeInt *newLeafNode = (LeafNodeInt *)newPage;

        //find midpoint
        int midPoint = leafOccupancy / 2;
        if (leafOccupancy %2 == 1 && leafNode-> keyArray[midPoint] < dataEntry.key) {
            midPoint = midPoint + 1;
        }

        //migrate right half of the node to new leaf
        for (int i = midPoint; i < leafOccupancy; i++) {
            newLeafNode->keyArray[i - midPoint] = leafNode->keyArray[i];
            newLeafNode->ridArray[i - midPoint] = leafNode->ridArray[i];
            leafNode->keyArray[i] = 0;
            leafNode->ridArray[i].page_number = 0;
        }
        //FIXME: why
        if (leafNode->keyArray[midPoint-1] < dataEntry.key) {
           insertLeafNode(newLeafNode, dataEntry);
        } else {
           insertLeafNode(leafNode, dataEntry);
        }

        //update pointers for siblings
        newLeafNode->rightSibPageNo = leafNode->rightSibPageNo;
        leafNode->rightSibPageNo = newPageNo;
        //return the left most key from right leaf node
        PageKeyPair<int> newKeyPair;
        newKeyPair.set(newPageNo, newLeafNode->keyArray[0]); //FIXME new page no?
        newChildEntry = new PageKeyPair<int>();
        newChildEntry = &newKeyPair;
        //free buffer
        bufMgr->unPinPage(file, newPageNo, true);
        bufMgr->unPinPage(file, leafPageNo, true);
        //special case as root
        if (rootPageNum == leafPageNo) {
            updateRoot(leafPageNo, newChildEntry);
        }
    }


    const void BTreeIndex::splitNonLeafNode(NonLeafNodeInt *prevNode, PageId prevPageNo, PageKeyPair<int> *&newChildEntry) {
        // allocating space for new node
        Page *newPage;
        PageId newPageNo;
        bufMgr->allocPage(file, newPageNo, newPage);
        NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;

        int midPoint = nodeOccupancy/2;
        int pushupIndex = midPoint;
        PageKeyPair<int> pushupEntry;
        if (nodeOccupancy % 2 == 0) { // even number of keys
            pushupIndex = (newChildEntry->key < prevNode->keyArray[midPoint])? midPoint-1 : midPoint;
        }
        pushupEntry.set(newPageNo, prevNode->keyArray[pushupIndex]);
        midPoint = pushupIndex + 1;
        // move right half of prevNode entries to new node
        for (int i = midPoint; i < nodeOccupancy; i++) {
            newNode->keyArray[i-midPoint] = prevNode->keyArray[i];
            newNode->pageNoArray[i-midPoint] = prevNode->pageNoArray[i+1];
            prevNode->pageNoArray[i+1] = (PageId) 0; //FIXME: cleraing old cell but is this valid?
            prevNode->keyArray[i+1] = 0;
        }
        // remove redundant copy of mid entry
        prevNode->pageNoArray[pushupIndex] = (PageId) 0;
        prevNode->keyArray[pushupIndex] = 0;
        newNode->level = prevNode->level;
        insertNonLeafNode(newChildEntry-> key < newNode->keyArray[0] ? prevNode: newNode, newChildEntry);
        newChildEntry = &pushupEntry;
        bufMgr->unPinPage(file, prevPageNo, true);
        bufMgr->unPinPage(file, newPageNo, true);

        //update root when prevNode is root
        if (prevPageNo == rootPageNum)
            updateRoot(prevPageNo, newChildEntry);
    }

    const void BTreeIndex::updateRoot(PageId prevPageId, PageKeyPair<int> *newChildEntry) {
        // allocate for a new root
        Page *newRoot;
        PageId newRootPageNo;
        bufMgr->allocPage(file, newRootPageNo, newRoot);
        NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;

        // update metadata
        newRootPage->level = initRootPageNo == rootPageNum? 1: 0;
        newRootPage->pageNoArray[0] = prevPageId;
        newRootPage->pageNoArray[1] = newChildEntry->pageNo; //FIXME why
        newRootPage->keyArray[0] = newChildEntry->key;

        Page *metadata;
        bufMgr->readPage(file, headerPageNum, metadata);
        IndexMetaInfo *metaPage = (IndexMetaInfo *)metadata;
        metaPage->rootPageNo = newRootPageNo;
        rootPageNum = newRootPageNo;
        //free buffer
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, newRootPageNo, true);
    }


    const void BTreeIndex::findSubtree(NonLeafNodeInt *curNode, PageId &nextNodeNo, int key) {
        int i = nodeOccupancy;
        while(i >= 0 && curNode->pageNoArray[i] == 0) { //FIXME: why >=
            i--;
        }
        while (i > 0 && curNode->keyArray[i-1] >= key ) {
            i--;
        }
        nextNodeNo = curNode->pageNoArray[i];
    }

    const bool BTreeIndex::keyIsInRange(int lowVal, const Operator LOp, int highVal, const Operator GOp,
                                        int curKey) {
        if (LOp == LT && GOp == GT) {
            return lowVal < curKey && curKey < highVal;
        } else if (LOp == LTE && GOp == GT) {
            return lowVal < curKey && curKey <= highVal;
        } if (LOp == LT && GOp == GTE) {
            return lowVal <= curKey && curKey < highVal;
        } else {
            return lowVal <= curKey && curKey <= highVal;
        }
    }
}
