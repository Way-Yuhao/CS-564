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

namespace badgerdb {
   /**
    * The constructor first checks if the specified index file exists. An index file name is constructed as:
    * concatenating the relation name with the offset of the attribute over which the index is built. The
    * general form of the index file name is "relName.attrOffset".
    * @param relationName the name of the raltion on which to build the index
    * @param outIndexName the name of the index file
    * @param bufMgrIn the instance of the global buffer manager
    * @param attrByteOffset the data type of the attribute indexing
    * @param attrType
    */
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

    /**
     *The destructor. Perform any cleanup that may be necessary, including clearing up any state variables,
     *unpinning any B+ tree pages that are pinned, and flushing the index file (by calling the function
     *bufMgr->flushFile()​). Note that this method does not delete the index file! But, deletion of the ​
     *file object is required, which will call the destructor of ​File​ class causing the index file to be closed.
     */
    BTreeIndex::~BTreeIndex()
    {
      scanExecuting = false;
      bufMgr->flushFile(BTreeIndex::file);
      delete file;
      file = nullptr;
    }

  /**
   * This method inserts a new entry into the index using the pair <key, rid>.
   * It calls recInsert() to recursively insert data.
   * @param key a pointer to the value to be inserted
   * @param rid the corresponding record id of the tuple in the base relation
   */
    const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
        RIDKeyPair<int> dataEntry;
        dataEntry.set(rid, *((int *) key));
        Page* root; // root
        // pin root
        bufMgr->readPage(file, rootPageNum, root);
        PageKeyPair<int> *newChildEntry = nullptr;
        bool isInitRoot = rootPageNum == initRootPageNo? true: false;
        recInsert(isInitRoot, root,rootPageNum, dataEntry, newChildEntry);
    }

    /**
     * This method is used to begin a “filtered scan” of the index.
     * For example, if the method is called using arguments ​(1, GT, 100, LTE)​,
     * then the scan should seek all entries greater than 1 and less than or equal to 100.
     * @param lowValParm value of lower bound
     * @param lowOpParm operator for the lower bound
     * @param highValParm value of the upper bound
     * @param highOpParm operator for the upper bound
     */
    const void BTreeIndex::startScan(const void* lowValParm, const Operator lowOpParm, const void* highValParm,
                                     const Operator highOpParm) {
        lowValInt = *((int *)lowValParm);
        highValInt = *((int *)highValParm);
        // scan for illegal operators or parameters
        if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm!= LT && highOpParm != LTE))
            throw BadOpcodesException();
        if (lowValInt > highValInt)
            throw BadScanrangeException();
        highOp = highOpParm;
        lowOp = lowOpParm;
        // kill all existing scans
        if (scanExecuting)
            endScan();
        // init scanning
        currentPageNum = rootPageNum;
        bufMgr->readPage(file, currentPageNum, currentPageData);
        if (initRootPageNo != rootPageNum) { // if root not a leaf
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
                while (i > 0 && curNode->keyArray[i-1] >= lowValInt) {
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
            } else {
                //traverse through the leaf node
                bool nullVal = false;
                for (int i = 0; i < leafOccupancy; i++) {
                    int curKey = curNode->keyArray[i];
                    // prevent comparing with null
                    if (i < leafOccupancy - 1 && curNode->ridArray[i+1].page_number == 0) {
                        //target not found
                        nullVal = true;
                    }
                    if (keyIsInRange(lowValInt, lowOp, highValInt, highOp, curKey)) {
                        //if current key satisfies the query
                        targetFound = true;
                        nextEntry = i;
                        scanExecuting = true;
                        break;
                    } else if((highOp == LT && curKey >= highValInt) || ((highOp == LTE) && curKey > highValInt)) {
                        // if no matching key is in range
                        bufMgr->unPinPage(file, currentPageNum, false);
                        throw NoSuchKeyFoundException();
                    }
                    if (leafOccupancy - 1 == i || nullVal) { //if traversal is complete for current node
                        // proceed to right sibling
                        bufMgr->unPinPage(file, currentPageNum, false);
                        if (curNode->rightSibPageNo == 0)
                            throw NoSuchKeyFoundException();
                        currentPageNum = curNode->rightSibPageNo;
                        bufMgr->readPage(file, currentPageNum, currentPageData);
                    }
                    if (nullVal) break;
                }
            }
        }
    }

    /**
     * This method fetches the record id of the next tuple that matches the scan criteria.
     * If the scan has reached the end, then it should throw the exception ​IndexScanCompletedException​.
     * For instance, if there are two data entries that need to be returned in a scan,
     * then the ​third call to ​scanNext must throw IndexScanCompletedException​.
     * A leaf page that has been read into the buffer pool for the purpose of scanning,
     * should not be unpinned from buffer pool unless all the records from it are read,
     * or the scan has reached its end. Use the right sibling page number value from
     * the current leaf to move to the next leaf which holds successive key values for the scan.
     * @param outRid record id of the next entry matching the can filter
     */
    const void BTreeIndex::scanNext(RecordId& outRid) {
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
                curNode = (LeafNodeInt *) currentPageData;
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

    /**
     * This method terminates the current scan and unpins all the pages that have been
     * pinned for the purpose of the scan. It throws ​ScanNotInitializedException​ when
     * called before a successful ​startScan​ call.
     */
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

   /**
    * Private helper function that recursively insert the given data entry to B+Tree
    * @param nodeIsLeaf true if node is a leaf
    * @param curPage pointer to current page object
    * @param curPageNo page id of the current page
    * @param dataEntry data to be inserted
    * @param newChildEntry entry to be pushed up
    */
    const void BTreeIndex::recInsert(bool nodeIsLeaf, Page *curPage, PageId curPageNo,
                                     RIDKeyPair<int> dataEntry, PageKeyPair<int> *&newChildEntry){
        // if current node is internal
        if (!nodeIsLeaf) {
            NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
            // choose subtree
            Page *nextPage;
            PageId nextPageNo;
            findSubTree(curNode, nextPageNo, dataEntry.key);
            bufMgr->readPage(file, nextPageNo, nextPage);
            nodeIsLeaf = curNode->level == 1;
            recInsert(nodeIsLeaf, nextPage, nextPageNo, dataEntry, newChildEntry);
            //splitting
            if (newChildEntry == nullptr) { //if no splitting occurred
                //unpin and return
                bufMgr->unPinPage(file, curPageNo, false);
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
            if (leaf->ridArray[leafOccupancy - 1].page_number == 0) {
                insertLeafNode(leaf, dataEntry);
                bufMgr->unPinPage(file, curPageNo, true);
                newChildEntry = nullptr;
            } else { // if leaf node is full
                //split such leaf
                splitLeafNode(leaf, curPageNo, newChildEntry, dataEntry);
            }
        }
    }

    /**
     * Helper method that inserts a new entry to a given non-leaf node.
     * @param curNode pointer to the current nonleaf node
     * @param newChildEntry entry to be pushed up
     */
    const void BTreeIndex::insertNonLeafNode(NonLeafNodeInt *curNode, PageKeyPair<int> *newChildEntry) {
        int i = nodeOccupancy;
        while(i >= 0 && curNode->pageNoArray[i] == 0) {
            i--;
        }
        while (i > 0 && (curNode->keyArray[i-1] > newChildEntry->key)) {
            curNode->keyArray[i] = curNode->keyArray[i-1];
            curNode->pageNoArray[i+1] = curNode->pageNoArray[i];
            i--;
        }
        //insert new entry
        curNode->keyArray[i] = newChildEntry->key;
        curNode->pageNoArray[i+1] = newChildEntry->pageNo;
    }

    /**
     * Helper method that inserts a data entry to a given leaf node.
     * @param leafNode pointer to the current leaf node
     * @param dataEntry entry to be pushed up
     */
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
            while (i>=0 && leafNode->keyArray[i] > dataEntry.key) {
                leafNode->keyArray[i+1] = leafNode->keyArray[i];
                leafNode->ridArray[i+1] = leafNode->ridArray[i];
                i--;
            }
            //insert new entry
            leafNode->keyArray[i+1] = dataEntry.key;
            leafNode->ridArray[i+1] = dataEntry.rid;
        }
    }

    /**
     * Helper method that splits a given non-leaf node into two parts. The function returns the entry
     * in the middle as newChildEntry. It also updates all relavent meta info about the nodes involved.
     * @param prevNode pointer to the old node before modification
     * @param prevPageNo page number of the old node
     * @param newChildEntry entry to be pushed up
     */
    const void BTreeIndex::splitNonLeafNode(NonLeafNodeInt *prevNode, PageId prevPageNo,
            PageKeyPair<int> *&newChildEntry) {
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
            prevNode->pageNoArray[i+1] = (PageId) 0;
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

   /**
    * Helper function that splits a given leaf node into two parts. It pushes up the middle entry as newChildEntry.
    * The function also updates the sibling pointer of the two nodes involved.
    * @param leafNode pointer to the current leaf node
    * @param leafPageNo page number of the current node
    * @param newChildEntry entry to be pushed up
    * @param dataEntry data to be inserted
    */
    const void BTreeIndex::splitLeafNode(LeafNodeInt *leafNode, PageId leafPageNo,
            PageKeyPair<int> *&newChildEntry, const RIDKeyPair<int> dataEntry) {
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
        newKeyPair.set(newPageNo, newLeafNode->keyArray[0]);
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

    /**
     * Helper function that finds the index of the subtree to search with a given key value.
     * @param curNode pointer to the current node
     * @param nextNodeNo page id of the root of the subtree to search next
     * @param key value to be searched
     */
    const void BTreeIndex::findSubTree(NonLeafNodeInt *curNode, PageId &nextNodeNo, int key) {
        int i = nodeOccupancy;
        while(i >= 0 && curNode->pageNoArray[i] == 0)
        {
            i--;
        }
        while(i > 0 && curNode->keyArray[i-1] >= key)
        {
            i--;
        }
        nextNodeNo = curNode->pageNoArray[i];
    }

   /**
    * Helper function that updates the date on the root node once a new root is present.
    * @param prevPageId page id of the old root
    * @param newChildEntry entry to be pushed up
    */
    const void BTreeIndex::updateRoot(PageId prevPageId, PageKeyPair<int> *newChildEntry) {
        // allocate for a new root
        Page *newRoot;
        PageId newRootPageNo;
        bufMgr->allocPage(file, newRootPageNo, newRoot);
        NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;

        // update metadata
        newRootPage->level = initRootPageNo == rootPageNum? 1: 0;
        newRootPage->pageNoArray[0] = prevPageId;
        newRootPage->pageNoArray[1] = newChildEntry->pageNo;
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

    /**
     * Helper function tha that checks if a given key satisfies the constraints of the given
     * low and upper bounds.
     * @param lowVal value of the lower bound
     * @param LOP operator on the lower bound
     * @param highVal value of the upper bound
     * @param GOP operator on the upper bound
     * @param key value to be searched
     * @return true if key is in range
     */
    const bool BTreeIndex::keyIsInRange(int lowVal, const Operator LOP, int highVal, const Operator GOP, int key) {
        if (LOP == GTE && GOP == LTE) {
            return key <= highVal && key >= lowVal;
        } else if (LOP == GT && GOP == LTE) {
            return key <= highVal && key > lowVal;
        } else if (LOP == GTE && GOP == LT) {
            return key < highVal && key >= lowVal;
        } else {
            return key < highVal && key > lowVal;
        }
    }
}
