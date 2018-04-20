// Be careful!
// Please include only once and only if you know exactly what you are doing.
// Because things are really getting complicated with template declarations,
// implementations in headers, I have taken no precautions to automatically
// resolving dependencies when including files.

// This file should be included and compiled only once (in AttributeEngine).
// This file should only be included from the one .cpp file that expands the
// templates within.

#include "AttributeEngine/Main/SingleAttributeFwd.h"
#include "AttributeEngine/Main/SinglePagedSpBaseDef.h"

// --------------------
// error codes

namespace TREX_ERROR {
USE_CODE AERC_FAILED;
USE_CODE AERC_NOT_IMPLEMENTED;
USE_CODE AERC_NO_SINGLE_VALUE;
USE_CODE AERC_SAVE_FAILED;
USE_CODE AERC_LOAD_FAILED;
USE_CODE AERC_LOCALE;
USE_CODE AERC_ILLEGAL_SORT;
USE_CODE AERC_ITERATION_IMPOSSIBLE;
USE_CODE AERC_DOCUMENT_NOT_FOUND;
USE_CODE AERC_WRONG_MAGIC_NUMBER;
USE_CODE AERC_UNSUPPORTED_FILE_VERSION;
USE_CODE AERC_NO_INDEX;
USE_CODE AERC_ATTRIBUTE_TYPE;
USE_CODE AERC_NO_EXTEND_RANGE;
USE_CODE AERC_VALUE_OUT_OF_RANGE;
USE_CODE AERC_GRANULARITY_NOT_SUPPORTED;
USE_CODE AERC_DATE_SYNTAX;
USE_CODE AERC_NO_UPDATED_VERSION;
USE_CODE AERC_ATTRIBUTE_CORRUPT;
USE_CODE OLAP_MULTI_VALUES_NOT_ALLOWED;
USE_CODE AERC_GRANULARITY_NOT_MONOTONE;
USE_CODE AERC_INDEX_INCONSISTENT;
USE_CODE AERC_INVALID_PARAMS;
USE_CODE AERC_MEMORY_ERROR;
} // namespace TREX_ERROR

// --------------------
// implementation for template class SingleAttribute

USE_TRACE(TRACE_AE_CASE_INSENSITIVE_SORT);
USE_TRACE(TRACE_SINGLEATTR);
USE_TRACE(TRACE_CREATE_INDEX);
USE_TRACE(TRACE_DATASTATS_AE);
USE_IMPORTED_TRACE(TRACE_AE_HEX);

// change #define <-> #undef as required:
#define _VERTICA_HACK_
#undef AE_USE_MERGE_DICT_GET_DOCIDS_INDEX_WITH_ADD_UNMATCHED_AS_NULL // NOTE def/undef needs to be done in SinglePagedUtils.h as well!

#define PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER (AttributeEngineConfig::getInstance()->getParallelMgetsearchChunksize())

// --------------------
// stuff copied from SingleIndex.cpp (XXX move this to some common header,
//                                    when we are allowed to check in again):

#define TOP_DOCUMENT_COUNT 10

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4355)
#endif

namespace AttributeEngine {

// --------------------
// class LtSingleResult:

class LtSingleResult
{
public:
    LtSingleResult(const TRexUtils::IndexVector& documents, bool ascending = true)
        : itsDocuments(documents)
        , itsAscending(ascending)
    {
    }
    bool operator()(DocumentId docid1, DocumentId docid2) const
    {
        // check if docids are within range,
        // if m_resultAndNot is set, the result of the query
        // will contain docids not in the column
        size_t n = itsDocuments.size();
        if ((size_t)docid1 < n && (size_t)docid2 < n) {
            int v1 = itsDocuments.get(docid1), v2 = itsDocuments.get(docid2);
            if (itsAscending)
                return v1 < v2 || (v1 == v2 && docid1 < docid2);
            else
                return v2 < v1 || (v2 == v1 && docid2 < docid1);
        } else
            return docid1 < docid2;
    }
    bool operator()(const DocidValue& dv1, const DocidValue& dv2) const
    {
        int vid1 = itsDocuments.get(dv1.docid);
        int vid2 = itsDocuments.get(dv2.docid);
        if (vid1 == vid2)
            if (itsAscending)
                return dv1.docid < dv2.docid;
            else
                return dv2.docid < dv1.docid;
        else if (itsAscending)
            return vid1 < vid2;
        else
            return vid2 < vid1;
    }
    bool operator()(const DocidAllocatedValue& dv1, const DocidAllocatedValue& dv2) const
    {
        int vid1 = itsDocuments.get(dv1.docid);
        int vid2 = itsDocuments.get(dv2.docid);
        if (vid1 == vid2)
            if (itsAscending)
                return dv1.docid < dv2.docid;
            else
                return dv2.docid < dv1.docid;
        else if (itsAscending)
            return vid1 < vid2;
        else
            return vid2 < vid1;
    }

private:
    const TRexUtils::IndexVector& itsDocuments;
    bool itsAscending;
};

// --------------------
// class LtSingleResultLocale:

template <class ValueType, class DictType>
class LtSingleResultLocale
{
public:
    LtSingleResultLocale(const DictType& dict, const TRexUtils::IndexVector& documents,
        bool ascending = true)
        : itsDict(dict)
        , itsDocuments(documents)
        , itsAscending(ascending)
    {
    }
    bool operator()(DocumentId docid1, DocumentId docid2) const
    {
        size_t n = itsDocuments.size();
        if ((size_t)docid1 < n && (size_t)docid2 < n) {
            int v1id = itsDocuments.get(docid1), v2id = itsDocuments.get(docid2);
            if (v1id == v2id) {
                if (itsAscending)
                    return docid1 < docid2;
                else
                    return docid2 < docid1;
            }
            const _STL::string& s1 = itsDict.getString(v1id, itsDictIt1, itsS1);
            const _STL::string& s2 = itsDict.getString(v2id, itsDictIt2, itsS2);
            int cmp = itsLocaleLt.cmp(s1, s2);
            if (itsAscending)
                return cmp < 0 || (cmp == 0 && docid1 < docid2);
            else
                return cmp > 0 || (cmp == 0 && docid2 < docid1);
        } else
            return docid1 < docid2;
    }
    bool operator()(const DocidValue& dv1, const DocidValue& dv2) const
    {
        //         return operator()(dv1.docid, dv2.docid);
        int cmp = itsLocaleLt.cmp(dv1.value, dv2.value);
        if (itsAscending)
            return cmp < 0 || (cmp == 0 && dv1.docid < dv2.docid);
        else
            return cmp > 0 || (cmp == 0 && dv2.docid < dv1.docid);
    }
    bool operator()(const DocidAllocatedValue& dv1, const DocidAllocatedValue& dv2) const
    {
        //         return operator()(dv1.docid, dv2.docid);
        const char *p1, *p2;
        size_t l1, l2;
        dv1.value.get(p1, l1);
        dv2.value.get(p2, l2);
        int cmp = itsLocaleLt.cmp(p1, p2, l1, l2);
        if (itsAscending)
            return cmp < 0 || (cmp == 0 && dv1.docid < dv2.docid);
        else
            return cmp > 0 || (cmp == 0 && dv2.docid < dv1.docid);
    }
    ERRCODE setLocale(const _STL::string& locale)
    {
        TREX_ERROR::TRexError error;
        if (!itsLocaleLt.setLocale(locale, true, error))
            return error.getCode();
        return AERC_OK;
    }

private:
    const DictType& itsDict;
    const TRexUtils::IndexVector& itsDocuments;
    bool itsAscending;
    mutable typename DictType::iterator itsDictIt1, itsDictIt2;
    mutable _STL::string itsS1, itsS2;
    TRexUtils::LocaleLt itsLocaleLt;
};

// --------------------
// class LtSingleResultIndirect:

class LtSingleResultIndirect
{
public:
    LtSingleResultIndirect(const _STL::vector<int>& indirect, const LtSingleResult& lt)
        : itsIndirect(indirect)
        , itsLt(lt)
    {
    }
    bool operator()(int index1, int index2) const
    {
        return itsLt.operator()(itsIndirect[index1], itsIndirect[index2]);
    }

private:
    const _STL::vector<int>& itsIndirect;
    const LtSingleResult& itsLt;
};

// --------------------
// class LtSingleResultLocaleIndirect:

template <class ValueType, class DictType>
class LtSingleResultLocaleIndirect
{
public:
    LtSingleResultLocaleIndirect(const _STL::vector<int>& indirect, const LtSingleResultLocale<ValueType, DictType>& lt)
        : itsIndirect(indirect)
        , itsLt(lt)
    {
    }
    bool operator()(int index1, int index2) const
    {
        return itsLt.operator()(itsIndirect[index1], itsIndirect[index2]);
    }

private:
    const _STL::vector<int>& itsIndirect;
    const LtSingleResultLocale<ValueType, DictType>& itsLt;
};

// --------------------
// class LtDocidSingle:

class LtDocidSingle
{
public:
    LtDocidSingle(
        const TRexUtils::IndexVector& documents,
        int nullValue,
        bool ascending,
        bool invertNullOrder)
        : itsDocuments(documents)
        , itsNullValueId(nullValue)
        , itsAscending(ascending)
        , itsInvertNullOrder(invertNullOrder)
    {
    }
    bool operator()(DocumentId docid1, DocumentId docid2) const
    {
        if (itsAscending)
            return cmp(docid1, docid2) < 0;
        else
            return cmp(docid2, docid1) < 0;
    }

private:
    const TRexUtils::IndexVector& itsDocuments;
    int itsNullValueId;
    bool itsAscending;
    bool itsInvertNullOrder;
    int cmp(DocumentId docid1, DocumentId docid2) const
    {
        int v1 = getValueId(docid1), v2 = getValueId(docid2);
        if (v1 == v2) {
            if (docid1 < docid2) {
                return -1;
            } else if (docid1 > docid2) {
                return 1;
            } else {
                return 0;
            }
        } else if (v1 == itsNullValueId) {
            return (!itsInvertNullOrder) ? -1 : 1;
        } else if (v2 == itsNullValueId) {
            return (!itsInvertNullOrder) ? 1 : -1;
        } else if (v1 < v2) {
            return -1;
        } else if (v1 > v2) {
            return 1;
        }
        return 0;
    }
    int getValueId(DocumentId docid) const
    {
        if (static_cast<size_t>(docid) >= itsDocuments.size())
            return itsNullValueId;
        else
            return itsDocuments.get(docid);
    }
};

// --------------------
// class LtDocidSingleLocale:

template <class ValueType, class DictType>
class LtDocidSingleLocale
{
public:
    LtDocidSingleLocale(const DictType& dict,
        const TRexUtils::IndexVector& documents,
        bool ascending,
        bool invertNullOrder)
        : itsDict(dict)
        , itsDocuments(documents)
        , itsAscending(ascending)
        , itsInvertNullOrder(invertNullOrder)
        , itsVidOrder(nullptr)
    {
    }
    ERRCODE setLocale(const _STL::string& locale)
    {
        TREX_ERROR::TRexError error;
        if (!itsLocaleLt.setLocale(locale, true, error))
            return error.getCode();
        return AERC_OK;
    }
    bool operator()(DocumentId docid1, DocumentId docid2) const
    {
        if (itsAscending)
            return cmp(docid1, docid2) < 0;
        else
            return cmp(docid2, docid1) < 0;
    }

private:
    const DictType& itsDict;
    const TRexUtils::IndexVector& itsDocuments;
    bool itsAscending;
    bool itsInvertNullOrder;
    // XXX these should not be mutable, make the compare methods non const
    // instead (trying to avoid trouble with the complex sorter template here)
    mutable typename DictType::iterator itsDictIt1, itsDictIt2;
    mutable _STL::string itsS1, itsS2;
    TRexUtils::LocaleLt itsLocaleLt;
    _STL::vector<int>* itsVidOrder;

    int getValueId(DocumentId docid) const
    {
        if (docid >= static_cast<DocumentId>(itsDocuments.size()))
            return itsDict.size();
        else
            return itsDocuments.get(docid);
    }
    int cmp(DocumentId docid1, DocumentId docid2) const
    {
        int valueid1 = getValueId(docid1), valueid2 = getValueId(docid2);
        if (valueid1 == valueid2) {
            if (docid1 < docid2) {
                return -1;
            } else if (docid1 > docid2) {
                return 1;
            } else {
                return 0;
            }
        } else if (valueid1 == itsDict.size()) {
            return (!itsInvertNullOrder) ? -1 : 1;
        } else if (valueid2 == itsDict.size()) {
            return (!itsInvertNullOrder) ? 1 : -1;
        }

        if (itsVidOrder) {
            // use precalculated sort order of all vids
            DEV_ASSERT_0(valueid1 < static_cast<int>(itsVidOrder->size()) && valueid2 < (int)itsVidOrder->size());
            int ord1 = (*itsVidOrder)[valueid1];
            int ord2 = (*itsVidOrder)[valueid2];
            return (ord1 < ord2) ? -1 : 1;
        }

        const _STL::string& s1 = itsDict.getString(valueid1, itsDictIt1, itsS1);
        const _STL::string& s2 = itsDict.getString(valueid2, itsDictIt2, itsS2);
        return itsLocaleLt.cmp(s1, s2);
    }

public:
    int cmpVids(int valueid1, int valueid2) const
    {
        if (valueid1 == valueid2) {
            return 0;
        } else if (valueid1 == itsDict.size()) {
            return (!itsInvertNullOrder) ? -1 : 1;
        } else if (valueid2 == itsDict.size()) {
            return (!itsInvertNullOrder) ? 1 : -1;
        }
        const _STL::string& s1 = itsDict.getString(valueid1, itsDictIt1, itsS1);
        const _STL::string& s2 = itsDict.getString(valueid2, itsDictIt2, itsS2);
        return itsLocaleLt.cmp(s1, s2);
    }

    // calculate sort order of all vids and use as cache
    void calcAndUseVidOrder(_STL::vector<int>& orderVec)
    {
        int maxVid = itsDict.size();
        _STL::vector<int> vidVec;
        vidVec.raw_resize(maxVid);
        for (int i = 0; i < maxVid; ++i)
            vidVec[i] = i;
        VidCmp<LtDocidSingleLocale<ValueType, DictType>> cmpVids(*this);

        if (TRexUtils::Parallel::mode == TRexUtils::Parallel::Force || (TRexUtils::Parallel::mode == TRexUtils::Parallel::Auto && maxVid > 1000000))
            TRexUtils::Parallel::psort(vidVec.begin(), vidVec.end(), cmpVids, false, -1);
        else
            _STL::sort(vidVec.begin(), vidVec.end(), cmpVids);

        // invert
        orderVec.raw_resize(maxVid);
        for (int i = 0; i < maxVid; ++i)
            orderVec[vidVec[i]] = i;
        itsVidOrder = &orderVec;
    }
};

// --------------------
// class EqSingleValues:

class EqSingleValues
{
public:
    EqSingleValues(const TRexUtils::IndexVector& documents, int nullValue)
        : itsDocuments(documents)
        , itsNullValue(nullValue)
    {
    }
    bool operator()(DocumentId doc1, DocumentId doc2) const { return doc1 == doc2 || getValueId(doc1) == getValueId(doc2); }
private:
    const TRexUtils::IndexVector& itsDocuments;
    int itsNullValue;
    int getValueId(DocumentId docid) const
    {
        return static_cast<size_t>(docid) < itsDocuments.size()
            ? itsDocuments.get(docid)
            : itsNullValue;
    }
};

// --------------------
// class DocidByDocidGenerator:

template <class ValueType, class DictType>
class DocidByDocidGenerator
{
public:
    DocidByDocidGenerator(SingleQueryPredicate<ValueType, DictType>& qp)
        : itsQp(qp)
    {
    }
    bool getFirst(DocumentId& docid)
    {
        if (!itsQp.getFirst())
            return false;
        docid = itsQp.getDocid();
        return true;
    }
    bool getNext(DocumentId& docid)
    {
        if (!itsQp.getNext())
            return false;
        docid = itsQp.getDocid();
        return true;
    }

private:
    SingleQueryPredicate<ValueType, DictType>& itsQp;
};

// --------------------
// class DocidByDocidMgetGenerator:

template <class ValueType, class DictType>
class DocidByDocidMgetGenerator
{
public:
    DocidByDocidMgetGenerator(SingleQueryPredicate<ValueType, DictType>& qp)
        : itsQp(qp)
    {
    }
    bool getFirst(DocumentId& docid)
    {
        itsBuffer.clear();
        itsIt = itsBuffer.end();
        itsDocid = 0;
        itsQp.optimizeForValueTest();
        return getNext(docid);
    }
    bool getNext(DocumentId& docid)
    {
        if (itsIt != itsBuffer.end()) {
            docid = *itsIt;
            ++itsIt;
            return true;
        }

        int attributeVersion = -1; // -1: disable GC processing; <=0: enable GC processing

        itsBuffer.clear();
        while (itsBuffer.empty()) {
            const TRexUtils::IndexVector& docs = itsQp.getValues().itsDocuments;
            int count = docs.size() - itsDocid;
            if (count <= 0) {
                itsIt = itsBuffer.end();
                return false;
            }
            if (count > 2048)
                count = 2048;

            if (itsQp.includeDocids(docs, itsDocid, count, itsBuffer, attributeVersion) != AERC_OK)
                return false;

            itsDocid += count; // standard processing retrieves only "count" values
        }
        docid = itsBuffer.front();
        itsIt = itsBuffer.begin() + 1;
        return true;
    }

private:
    SingleQueryPredicate<ValueType, DictType>& itsQp;
    _STL::vector<int> itsBuffer;
    _STL::vector<int>::iterator itsIt;
    DocumentId itsDocid;
};

// --------------------
// class DocidByDocidAndNotGenerator:

template <class ValueType, class DictType>
class DocidByDocidAndNotGenerator
{
public:
    DocidByDocidAndNotGenerator(SingleQueryPredicate<ValueType, DictType>& qp)
        : itsQp(qp)
    {
    }
    bool getFirst(DocumentId& docid)
    {
        if (!itsQp.getFirstAndNot())
            return false;
        docid = itsQp.getDocid();
        return true;
    }
    bool getNext(DocumentId& docid)
    {
        if (!itsQp.getNextAndNot())
            return false;
        docid = itsQp.getDocid();
        return true;
    }

private:
    SingleQueryPredicate<ValueType, DictType>& itsQp;
};

// --------------------
// class SingleAttribute:

template <class ValueType, class DictType>
SingleAttribute<ValueType, DictType>::SingleAttribute(
    const AttributeStore& attributeStore,
    AttributeId attributeId,
    const _STL::string& name,
    const AttributeDefinition& definition)
    : Parent::SinglePagedSpBase(attributeStore, attributeId, name, definition)
    , m_values(m_definition, this)
    , m_newValues(nullptr)
    , m_updates(m_definition)
    , m_sparseWriteFlags(0)
    , m_memoryInfo()
    , m_allocatedMemoryInfo()
    , m_allocatedNewMemoryInfo()
{
    itsIndexState = itsNewIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
    itsDataStatsState = itsNewDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
    cacheMemoryInfo();
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::reserveUpdates(size_t num)
{
    Synchronization::UncheckedMutexScope guard(m_writeLock);
    m_updates.reserve(num);
    cacheMemoryInfo();
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::takeUpdates(UpdateContainer& updates)
{
    Synchronization::UncheckedMutexScope guard(m_writeLock);
    ERRCODE ret = moveToSingleUpdates(updates, m_updates,
        m_attributeStore.getIndexId(), m_name,
        m_definition);
    cacheMemoryInfo();
    fixAvc(true);
    return ret;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::searchDocuments(
    const AttributeQuery& query, ltt::vector<DocumentId>& result,
    ltt::vector<float>* ranks, QueryData& queryData, QueryStats& queryStats)
{
    AE_TEST_RET(lazyLoad());
    return _searchDocuments(query, result, ranks, queryData, queryStats);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::_searchDocuments(
    const AttributeQuery& query,
    ltt::vector<DocumentId>& result,
    ltt::vector<float>* ranks,
    QueryData& queryData,
    QueryStats& queryStats)
{
    InvertedIndexInfoMustBeSetInThisScope invertedIndexScope(queryStats);

    ERRCODE ret = AERC_OK;

    SingleQueryPredicate<ValueType, DictType>& qp = initQueryData(queryData, query, true, ret);
    if (ret != AERC_OK) {
        queryStats.setInvertedIndexAccess(InvertedIndexAccess::SKIPPED_EMPTY);
        return clearQueryData(queryData, ret);
    }

    queryStats.setInvertedIndexAccess(isIndexDefined() ? InvertedIndexAccess::NO : InvertedIndexAccess::NOT_EXISTING);
    ITERATION_METHOD im = query.getIterationMethod();

    if (im == ITERATE_AUTO) {
        unsigned docEstimation = 0;
        {
            bool dummyExactResult;
            ret = getResultSizeEstimation(m_values, query, docEstimation, dummyExactResult, qp);
            if (ret != AERC_OK)
                return ret;
        }
        if (!ranks) {
            if (isIndexed()) {
                int resultCount = 0;
                if (query.hasResultDocs())
                    resultCount = query.countResultDocs();
                if (docEstimation < 100 || resultCount == 0) {
                    int documents = m_values.countDocuments();
                    int queryValues = qp.countValues();
                    int divisor = 6;
                    if (queryValues > 100000 && documents / queryValues < 3)
                        divisor = 8;
                    if (resultCount == 0 && (int)docEstimation > documents / divisor)
                        im = ITERATE_DOCIDS;
                    else
                        im = ITERATE_VALUES;
                } else if ((int)docEstimation > resultCount)
                    im = ITERATE_DOCIDS;
                else
                    im = ITERATE_VALUES;
                if (im == ITERATE_VALUES)
                    im = ITERATE_VALUES_MERGE;
            } else
                im = ITERATE_DOCIDS;
        } else {
            if (!isIndexed()) // no index flag is set
                im = ITERATE_DOCIDS;
            else
                im = ITERATE_VALUES;
        }
        int reserveCount = docEstimation;
        int rangeLimit = 0;
        if (im == ITERATE_DOCIDS
            && resultRangeFromQuery(query).getLimit(rangeLimit))
            switch (query.getSorting()) {
                case DONT_SORT:
                case SORT_BY_DOCID:
                    if (rangeLimit < reserveCount)
                        reserveCount = rangeLimit;
                // no break
                default:;
            }
        //result.reserve(reserveCount);
        if (ranks)
            ranks->reserve(reserveCount);
    }

    switch (im) {
        case ITERATE_DOCIDS:
        case ITERATE_RESULTDOCS:
            if (query.hasResultDocs() && query.getResultNot()) {
                DocidByDocidAndNotGenerator<ValueType, DictType> gen(qp);
                return searchDocumentsIterateDocids(query, result, ranks, qp, gen);
            } else if (!qp.getQueryHasResultDocs()) {
                DocidByDocidMgetGenerator<ValueType, DictType> gen(qp);
                return searchDocumentsIterateDocids(query, result, ranks, qp, gen);
            } else {
                DocidByDocidGenerator<ValueType, DictType> gen(qp);
                return searchDocumentsIterateDocids(query, result, ranks, qp, gen);
            }
        case ITERATE_VALUES:
            return searchDocumentsIterateValues(m_values, query, result, ranks, qp, queryStats);
        case ITERATE_VALUES_MERGE:
            return searchDocumentsIterateValuesMerge(m_values, query, queryData, result, ranks, qp, queryStats);
        default:
            return AE_TRACE_ERROR(TREX_ERROR::AERC_ITERATION_IMPOSSIBLE, "searchDocuments");
    }
}

template <class ValueType, class DictType>
template <class Generator>
ERRCODE SingleAttribute<ValueType, DictType>::searchDocumentsIterateDocids(
    const AttributeQuery& query,
    ltt::vector<DocumentId>& result,
    ltt::vector<float>* ranks,
    SingleQueryPredicate<ValueType, DictType>& qp,
    Generator& gen)
{
    // skip scan if we have no rows as result and do an AND
    const int numValuesToScan = qp.countValues();
    if (numValuesToScan == 0 && !query.getResultNot())
        return AERC_OK;

    ERRCODE ret = AERC_OK;
    ResultRange range = resultRangeFromQuery(query);

    switch (query.getSorting()) {
        case DONT_SORT:
        case SORT_BY_DOCID: {
            // parallel mget
            // small prevResult -> use cutSorted, iterate prevResult
            const int n = (m_values.getMaxDocid() + 1);
            if (!TRexUtils::Parallel::Context::isJob()
                && (TRexUtils::Parallel::mode == TRexUtils::Parallel::Force || TRexUtils::Parallel::mode == TRexUtils::Parallel::Auto)
                && static_cast<size_t>(n) > PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER
                && range.includesEverything(0)
                && (!qp.getQueryHasResultDocs() || (qp.getQueryResultDocs().isBitVector() && query.countResultDocs() * 16 > n))
                && !query.getResultNot()) {
                ret = searchDocumentsIterateDocidsParallel(m_values.itsDocuments, m_values.itsDict.size(), query, nullptr, &result, qp, AttributeValueContainer::getAttributeLocation());
                break;
            }
            cutSorted(gen, result, EqDocids(), range, false);

            break;
        }
        default:
            ret = AE_TRACE_ERROR(TREX_ERROR::AERC_ILLEGAL_SORT, "searchDocumentsIterateDocids");
    }

    if (ranks) {
        ranks->resize(result.size());
        _STL::vector<DocumentId>::const_iterator it = result.begin();
        const _STL::vector<DocumentId>::const_iterator itEnd = result.end();
        _STL::vector<float>::iterator ranksIt = ranks->begin();
        float rank;
        for (; it != itEnd; ++it, ++ranksIt) {
            if (qp.getRank(m_values.itsDocuments.get(*it), rank))
                *ranksIt = rank;
        }
    }

    return ret;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::searchDocuments(
    const AttributeQuery& query,
    TRexUtils::BitVector& result,
    ltt::vector<float>* ranks,
    BIT_OPERATOR op,
    QueryData& queryData,
    QueryStats& queryStats,
    int resultBitsSet)
{
    AE_TEST_RET(lazyLoad());
    InvertedIndexInfoMustBeSetInThisScope invertedIndexScope(queryStats);

    ERRCODE ret = AERC_OK;

    SingleQueryPredicate<ValueType, DictType>& qp = initQueryData(queryData, query, false, ret);
    if (ret != AERC_OK) {
        queryStats.setInvertedIndexAccess(InvertedIndexAccess::SKIPPED_EMPTY);
        return clearQueryData(queryData, ret);
    }
    queryStats.setInvertedIndexAccess(isIndexDefined() ? InvertedIndexAccess::NO : InvertedIndexAccess::NOT_EXISTING);

    ITERATION_METHOD im = query.getIterationMethod();
    if (im == ITERATE_AUTO) {
        unsigned docEstimation = 0;
        {
            bool dummyExactResult;
            ret = getResultSizeEstimation(m_values, query, docEstimation, dummyExactResult, qp);
            if (ret != AERC_OK)
                return ret;
        }
        if (op == BIT_OPERATOR_AND || op == BIT_OPERATOR_AND_NOT) {
            if (resultBitsSet <= 0)
                resultBitsSet = (int)result.numSet();
            if ((int)docEstimation > resultBitsSet * 2)
                im = ITERATE_RESULTDOCS;
        }
        if (im == ITERATE_AUTO) {
            if (isIndexed()) {
                int documents = m_values.countDocuments();
                int queryValues = qp.countValues();
                int divisor = 2;
                if (queryValues > 2)
                    divisor = 4;
                if (queryValues > 100000 && documents / queryValues < 3)
                    divisor = 5;

                // iterate docids (mgetsearch) is parallelized, itervalues is not
                // -> favour parallel iterdocids
                if (TRexUtils::Parallel::mode == TRexUtils::Parallel::Force || (TRexUtils::Parallel::mode == TRexUtils::Parallel::Auto && static_cast<size_t>(documents) > PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER))
                    divisor *= Execution::JobExecutor::getInstance().concurrencyHint();

                if ((int)docEstimation > documents / divisor) {
                    im = ITERATE_DOCIDS;
                } else {
                    im = ITERATE_VALUES;
                }
            } else {
                im = ITERATE_DOCIDS;
            }
        }
        //         if (im == ITERATE_DOCIDS
        //             && (op == BIT_OPERATOR_AND || op == BIT_OPERATOR_AND_NOT)) {
        //             if (resultBitsSet <= 0)
        //                 resultBitsSet = (int)result.numSet();
        //             if ((int)docEstimation > resultBitsSet)
        //                 im = ITERATE_RESULTDOCS;
        //         }
    }

    switch (im) {
        case ITERATE_DOCIDS:
            return searchDocumentsIterateDocids(query, result, ranks, op, resultBitsSet, qp);
        case ITERATE_RESULTDOCS:
            return searchDocumentsIterateResultdocs(query, result, ranks, op, resultBitsSet, qp);
        case ITERATE_VALUES:
            return searchDocumentsIterateValues(m_values, query, result, ranks, op, resultBitsSet, qp, queryStats);
        default:
            return AE_TRACE_ERROR(TREX_ERROR::AERC_ITERATION_IMPOSSIBLE, "searchDocuments");
    }
}

template <class ValueType, class DictType>
ERRCODE searchDocumentsIterateDocidResultParallel(
    TRexUtils::IndexVector& documents,
    size_t dictSize,
    const AttributeQuery& query,
    TRexUtils::BitVector& bvResult,
    BIT_OPERATOR op,
    SingleQueryPredicate<ValueType, DictType>& qp,
    const AttributeLocation& location)
{
    size_t numDocs = bvResult.size();
    if (numDocs > documents.size())
        numDocs = documents.size();
    size_t numJobs = (numDocs / PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER) + 1;
    uint64_t numJobsHint = Execution::JobExecutor::getInstance().concurrencyHint();
    if (numJobs > numJobsHint) {
        numJobs = numJobsHint;
    }
    size_t chunkSize = (numDocs / numJobs) + 1;
    // rounds up (align) chunks to bitvector words
    // parallel jobs must work on disjunct result bitvector areas
    size_t align = 8 * sizeof(TRexUtils::BitVector::ElemType);
    chunkSize = ((((long)chunkSize) + align - 1) & ~(align - 1));

    TRexUtils::BitVector bvTmp1;
    const TRexUtils::BitVector* bvValids = nullptr;
    const _STL::vector<int>* vecValIds = nullptr;
    const _STL::vector<int>* vecValIdRanges = nullptr;
    int valRangeFrom = 0, valRangeTo = 0;
    if (qp.isRangeOnlyQuery())
        qp.getValueRange(valRangeFrom, valRangeTo);
    else if (qp.valuesAreRanges()) {
        qp.optimizeForValueTest();
        if (!qp.getBitVector().empty()) {
            bvValids = &qp.getBitVector();
        } else {
            vecValIdRanges = &qp.getPredicateValues();
        }
    } else {
        // use either bitvector or intvector for valueid input
        const _STL::vector<int>& valVec = qp.getPredicateValues();
        size_t maxVal = dictSize;
        if (valVec.size() > 8000 /*(maxVal >> 5)*/) {
            bvValids = &bvTmp1;
            bvTmp1.resize(maxVal + 1, false);
            _STL::vector<int>::const_iterator it = valVec.begin(), itEnd = valVec.end();
            for (; it != itEnd; ++it)
                bvTmp1.set_unchecked(*it);
        } else
            vecValIds = &valVec;
    }
    bool bOpAnd = (op == BIT_OPERATOR_AND); // else BIT_OPERATOR_NOT_AND
    if (bOpAnd)
        bvResult.setRange(numDocs, bvResult.size(), false);

    if (chunkSize < numDocs) {
        TRexUtils::Parallel::Context context;
        size_t docRangeFrom = 0;
        while (docRangeFrom < numDocs) {
            if ((docRangeFrom + chunkSize) >= numDocs)
                chunkSize = numDocs - docRangeFrom; // adapt last chunk
            if (chunkSize == 0)
                break;
            if (bvValids)
                context.pushJob(new TRexUtils::JobParallelResultDocSearch(documents, bvResult, bOpAnd, docRangeFrom, chunkSize, bvValids, location));
            else if (vecValIds)
                context.pushJob(new TRexUtils::JobParallelResultDocSearch(documents, bvResult, bOpAnd, docRangeFrom, chunkSize, vecValIds, location));
            else if (vecValIdRanges)
                context.pushJob(new TRexUtils::JobParallelResultDocSearch(documents, bvResult, bOpAnd, docRangeFrom, chunkSize, TRexUtils::JobParallelResultDocSearch::useValueIdRangesVec, vecValIdRanges, location));
            else
                context.pushJob(new TRexUtils::JobParallelResultDocSearch(documents, bvResult, bOpAnd, docRangeFrom, chunkSize, valRangeFrom, valRangeTo, location));
            docRangeFrom += chunkSize;
        }

        if (!context.run())
            return context.getErrors().front().getCode();
    } else {
        TRexUtils::Parallel::JobResult jobRes;
        if (bvValids) {
            TRexUtils::JobParallelResultDocSearch so(documents, bvResult, bOpAnd, 0, numDocs, bvValids, location);
            while ((jobRes = so.run()) == TRexUtils::Parallel::Keep) {
            }
            if (jobRes != TRexUtils::Parallel::Done) {
                return so.getError().getCode();
            }
        } else if (vecValIds) {
            TRexUtils::JobParallelResultDocSearch so(documents, bvResult, bOpAnd, 0, numDocs, vecValIds, location);
            while ((jobRes = so.run()) == TRexUtils::Parallel::Keep) {
            }
            if (jobRes != TRexUtils::Parallel::Done) {
                return so.getError().getCode();
            }
        } else if (vecValIdRanges) {
            TRexUtils::JobParallelResultDocSearch so(documents, bvResult, bOpAnd, 0, numDocs, TRexUtils::JobParallelResultDocSearch::useValueIdRangesVec, vecValIdRanges, location);
            while ((jobRes = so.run()) == TRexUtils::Parallel::Keep) {
            }
            if (jobRes != TRexUtils::Parallel::Done) {
                return so.getError().getCode();
            }
        } else {
            TRexUtils::JobParallelResultDocSearch so(documents, bvResult, bOpAnd, 0, numDocs, valRangeFrom, valRangeTo, location);
            while ((jobRes = so.run()) == TRexUtils::Parallel::Keep) {
            }
            if (jobRes != TRexUtils::Parallel::Done) {
                return so.getError().getCode();
            }
        }
    }
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE searchDocumentsIterateDocidsParallel(
    TRexUtils::IndexVector& documents,
    size_t dictSize,
    const AttributeQuery& query,
    TRexUtils::BitVector* bvResult,
    ltt::vector<DocumentId>* vecResult,
    SingleQueryPredicate<ValueType, DictType>& qp,
    const AttributeLocation& location)
{
    const size_t nDocs = documents.size();
    const size_t oldBvSize = bvResult != nullptr ? bvResult->size() : 0;
    if (bvResult != nullptr && bvResult->size() < nDocs)
        bvResult->resize(nDocs);
    const TRexUtils::BitVector* prevResult = nullptr;
    if (qp.getQueryHasResultDocs())
        prevResult = qp.getQueryResultDocs().getResultDocBits();
    size_t numJobs = (nDocs / PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER) + 1;
    uint64_t numJobsHint = Execution::JobExecutor::getInstance().concurrencyHint();
    // Bug44832: TODO replace by max_concurrency_hint
    DTEST_EXEC_WHEN_REACHED(
        "DTEST_AE_SA_SEARCHDOCUMENTSITERATEDOCIDSPARALLEL_SINGLECORE", "", { numJobsHint = 1; });
    if (numJobs > numJobsHint)
        numJobs = numJobsHint;
    size_t chunkSize = (nDocs / numJobs) + 1;

    size_t align = 1024;
    chunkSize = ((((long)chunkSize) + align - 1) & ~(align - 1));
    // parallel jobs must work on disjunct result bitvector areas
    DEV_ASSERT_0((chunkSize % sizeof(TRexUtils::BitVector::ElemType)) == 0);
    TRexUtils::BitVector bvTmp1;

    // use either bitvector or intvector as result
    _STL::vector<_STL::vector<unsigned>> vecVecResult;
    if (vecResult) {
        vecVecResult.resize(numJobs);
        for (size_t i = 0; i < vecVecResult.size(); ++i)
            vecVecResult[i].reserve(vecResult->capacity() / numJobs);
    }
    const TRexUtils::BitVector* bvValids = nullptr;
    const _STL::vector<int>* vecValIds = nullptr;
    const _STL::vector<int>* vecValIdRanges = nullptr;
    int valRangeFrom = 0, valRangeTo = 0;
    if (qp.isRangeOnlyQuery())
        qp.getValueRange(valRangeFrom, valRangeTo);
    else if (qp.valuesAreRanges()) {
        qp.optimizeForValueTest();
        if (!qp.getBitVector().empty()) {
            bvValids = &qp.getBitVector();
        } else {
            vecValIdRanges = &qp.getPredicateValues();
        }
    } else {
        // use either bitvector or intvector for valueid input
        const _STL::vector<int>& valVec = qp.getPredicateValues();
        size_t maxVal = dictSize;
        if (valVec.empty()) {
            bvValids = &bvTmp1;
            bvTmp1.resize(maxVal + 1, false);
        } else {
            if (((valVec.back() / 32) < (int)valVec.size()) || (nDocs > 32000)) {
                bvValids = &bvTmp1;
                bvTmp1.resize(maxVal + 1, false);
                _STL::vector<int>::const_iterator it = valVec.begin(), itEnd = valVec.end();
                for (; it != itEnd; ++it)
                    bvTmp1.set_unchecked(*it);
            } else {
                vecValIds = &valVec;
            }
        }
    }

    ltt::AtomicI64 maxBit;
    maxBit.set(static_cast<int64_t>(oldBvSize));
    if (chunkSize < nDocs) {
        TRexUtils::Parallel::Context context;
        size_t docRangeFrom = 0, k = 0;
        while (docRangeFrom < nDocs) {
            if ((docRangeFrom + chunkSize) >= nDocs)
                chunkSize = nDocs - docRangeFrom; // adapt last chunk
            if (chunkSize == 0)
                break;
            if (bvValids)
                context.pushJob(new TRexUtils::JobParallelMgetSearch(documents, docRangeFrom, chunkSize, prevResult,
                    bvValids, bvResult, bvResult ? nullptr : &vecVecResult[k], location, maxBit));
            else if (vecValIds)
                context.pushJob(new TRexUtils::JobParallelMgetSearch(documents, docRangeFrom, chunkSize, prevResult,
                    vecValIds, bvResult, bvResult ? nullptr : &vecVecResult[k], location, maxBit));
            else if (vecValIdRanges)
                context.pushJob(new TRexUtils::JobParallelMgetSearch(documents, docRangeFrom, chunkSize, prevResult,
                    TRexUtils::JobParallelMgetSearch::useValueIdRangesVec, vecValIdRanges, bvResult, bvResult ? nullptr : &vecVecResult[k], location, maxBit));
            else
                context.pushJob(new TRexUtils::JobParallelMgetSearch(documents, docRangeFrom, chunkSize, prevResult,
                    valRangeFrom, valRangeTo, bvResult, bvResult ? nullptr : &vecVecResult[k], location, maxBit));
            docRangeFrom += chunkSize;
            ++k;
        }
        if (!context.run())
            return context.getErrors().front().getCode();
    } else {
        TRexUtils::Parallel::JobResult jobRes;
        if (bvValids) {
            TRexUtils::JobParallelMgetSearch so(documents, 0, nDocs, prevResult,
                bvValids, bvResult, bvResult ? nullptr : &vecVecResult[0], location, maxBit);
            while ((jobRes = so.run()) == TRexUtils::Parallel::Keep) {
            } // Bug 44832
            if (jobRes != TRexUtils::Parallel::Done) {
                return so.getError().getCode();
            }
        } else if (vecValIds) {
            TRexUtils::JobParallelMgetSearch so(documents, 0, nDocs, prevResult,
                vecValIds, bvResult, bvResult ? nullptr : &vecVecResult[0], location, maxBit);
            while ((jobRes = so.run()) == TRexUtils::Parallel::Keep) {
            } // Bug 44832
            if (jobRes != TRexUtils::Parallel::Done) {
                return so.getError().getCode();
            }
        } else if (vecValIdRanges) {
            TRexUtils::JobParallelMgetSearch so(documents, 0, nDocs, prevResult,
                TRexUtils::JobParallelMgetSearch::useValueIdRangesVec, vecValIdRanges, bvResult, bvResult ? nullptr : &vecVecResult[0], location, maxBit);
            while ((jobRes = so.run()) == TRexUtils::Parallel::Keep) {
            } // Bug 44832
            if (jobRes != TRexUtils::Parallel::Done) {
                return so.getError().getCode();
            }
        } else {
            TRexUtils::JobParallelMgetSearch so(documents, 0, nDocs, prevResult,
                valRangeFrom, valRangeTo, bvResult, bvResult ? nullptr : &vecVecResult[0], location, maxBit);
            while ((jobRes = so.run()) == TRexUtils::Parallel::Keep) {
            } // Bug 44832
            if (jobRes != TRexUtils::Parallel::Done) {
                return so.getError().getCode();
            }
        }
    }

    if (bvResult) {
        AttributeEngineConfig* cfg = AttributeEngineConfig::getInstance();
        if (cfg && cfg->getShrinkResultBitvector() == true) {
            DEV_ASSERT_0(!bvResult->anySetInRange(maxBit + 1, bvResult->size()));
            bvResult->resize(maxBit + 1);
        }
        if (prevResult)
            bvResult->opAnd(*prevResult);
    } else {
        size_t numTotal = 0, i = 0;
        for (i = 0; i < vecVecResult.size(); ++i)
            numTotal += vecVecResult[i].size();
        (*vecResult).raw_resize(numTotal);
        if (numTotal) {
            int* ptr = &(*vecResult)[0];
            for (i = 0; i < vecVecResult.size(); ++i) {
                size_t size = vecVecResult[i].size();
                if (size) {
                    ::memcpy(ptr, &vecVecResult[i][0], size * sizeof(int));
                    ptr += size;
                }
            }
        }
    }
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::searchDocumentsIterateDocids(
    const AttributeQuery& query,
    TRexUtils::BitVector& result,
    ltt::vector<float>* ranks,
    BIT_OPERATOR op,
    int resultBitsSet,
    SingleQueryPredicate<ValueType, DictType>& qp)
{
    ERRCODE ret = AERC_OK;
    qp.setIgnoreResultDocs();
    int n = m_values.getMaxDocid() + 1;
    ResultRange resRange = resultRangeFromQuery(query);

    switch (op) {
        case BIT_OPERATOR_OR: {
            if (qp.isRangeOnlyQuery()) {
                if (!TRexUtils::Parallel::Context::isJob()
                    && (TRexUtils::Parallel::mode == TRexUtils::Parallel::Force || (TRexUtils::Parallel::mode == TRexUtils::Parallel::Auto && static_cast<size_t>(n) > PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER))
                    && static_cast<size_t>(n) > 1
                    && !query.getNeedRanks()
                    && resRange.includesEverything(0)
                    && (!qp.getQueryHasResultDocs() || qp.getQueryResultDocs().isBitVector())) {
                    ret = searchDocumentsIterateDocidsParallel(m_values.itsDocuments, m_values.itsDict.size(), query, &result, nullptr, qp, AttributeValueContainer::getAttributeLocation());
                    break;
                }
                if (n > static_cast<int>(result.size()))
                    result.resize(n);
                int rangeFrom, rangeTo;
                qp.getValueRange(rangeFrom, rangeTo);
                m_values.itsDocuments.mgetSearch(1, n - 1, rangeFrom, rangeTo, result);
                break;
            }
            if (!TRexUtils::Parallel::Context::isJob()
                && (TRexUtils::Parallel::mode == TRexUtils::Parallel::Force || (TRexUtils::Parallel::mode == TRexUtils::Parallel::Auto && static_cast<size_t>(n) > PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER))
                && static_cast<size_t>(n) > 1
                && !query.getNeedRanks()
                && resRange.includesEverything(0)
                && (!qp.getQueryHasResultDocs() || qp.getQueryResultDocs().isBitVector())) {
                ret = searchDocumentsIterateDocidsParallel(m_values.itsDocuments, m_values.itsDict.size(), query, &result, nullptr, qp, AttributeValueContainer::getAttributeLocation());
                break;
            }
            if (n > static_cast<int>(result.size()))
                result.resize(n);
            if (qp.getFirst()) {
                const TRexUtils::BitVector bv = qp.getBitVector();
                if (!bv.empty()) {
                    int prevValue = -1;
                    int noBitsSet = bv.numSet();
                    if (noBitsSet > 0)
                        prevValue = *(bv.begin()) - 1;
                    bool range = true;

                    for (TRexUtils::BitVector::iterator bIT = bv.begin(); bIT != bv.end(); ++bIT) {
                        if ((int)*bIT != prevValue + 1) {
                            range = false;
                            break;
                        }
                        prevValue = *bIT;
                    }
                    if (range && noBitsSet > 0 && !qp.getQueryHasResultDocs()) {
                        int rangeFrom, rangeTo;
                        rangeFrom = *(bv.begin());
                        rangeTo = prevValue + 1;
                        m_values.itsDocuments.mgetSearch(1, n - 1, rangeFrom, rangeTo, result);
                        break;
                    }

                    if (!range && noBitsSet > 0 && !qp.getQueryHasResultDocs()) {
                        m_values.itsDocuments.mgetSearch(1, n - 1, bv, result);
                        break;
                    }
                }

                do {
                    result.set_unchecked(qp.getDocid());
                } while (qp.getNext());
            }
            break;
        }
        case BIT_OPERATOR_AND: {
            if (qp.getQueryHasResultDocs()) {
                // small prevResult -> use cutSorted, iterate prevResult
                int numPrevResults = query.countResultDocs();
                if (numPrevResults < n)
                    n = numPrevResults;
            }
            TRexUtils::BitVector bv2;
            if (!TRexUtils::Parallel::Context::isJob()
                && (TRexUtils::Parallel::mode == TRexUtils::Parallel::Force || (TRexUtils::Parallel::mode == TRexUtils::Parallel::Auto && static_cast<size_t>(n) > PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER))
                && static_cast<size_t>(n) > 1
                && !query.getNeedRanks()
                && resRange.includesEverything(0)
                && (!qp.getQueryHasResultDocs() || qp.getQueryResultDocs().isBitVector())) {
                ret = searchDocumentsIterateDocidsParallel(m_values.itsDocuments, m_values.itsDict.size(), query, &bv2, nullptr, qp, AttributeValueContainer::getAttributeLocation());
                result.opAnd(bv2);
                break;
            }
            // nonparallel
            int resultSize = result.size();
            bv2.resize(resultSize, false);
            if (qp.getFirst())
                do {
                    int docid = qp.getDocid();
                    if (docid < resultSize)
                        bv2.set_unchecked(docid);
                } while (qp.getNext());
            result.opAnd(bv2);
            break;
        }
        case BIT_OPERATOR_AND_NOT:

            if (!TRexUtils::Parallel::Context::isJob()
                && (TRexUtils::Parallel::mode == TRexUtils::Parallel::Force || (TRexUtils::Parallel::mode == TRexUtils::Parallel::Auto && static_cast<size_t>(n) > PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER))
                && static_cast<size_t>(n) > 1
                && !query.getNeedRanks()
                && resRange.includesEverything(0)
                && (!qp.getQueryHasResultDocs() || qp.getQueryResultDocs().isBitVector())) {
                TRexUtils::BitVector bv2;
                ret = searchDocumentsIterateDocidsParallel(m_values.itsDocuments, m_values.itsDict.size(), query, &bv2, nullptr, qp, AttributeValueContainer::getAttributeLocation());
                result.opNotAnd(bv2);
                break;
            }

#ifdef _VERTICA_HACK_
            if (qp.getFirst() && qp.getBitVector().size() > 0) {
                TRexUtils::BitVector bv(m_values.itsDocuments.size());
                m_values.itsDocuments.mgetSearch(0, m_values.itsDocuments.size(), qp.getBitVector(), bv);
                for (TRexUtils::BitVector::iterator bvIt = bv.begin(); bvIt != bv.end(); ++bvIt) {
                    result.clear_unchecked(*bvIt);
                }
                break;
            }
#endif // _VERTICA_HACK_
            //else path for VERTICA_HACK and generic default
            if (qp.getFirst())
                do {
                    result.clear(qp.getDocid());
                } while (qp.getNext());
            break;
        default:
            ret = TREX_ERROR::AERC_NOT_IMPLEMENTED;
    }
    cutBitVector(result, resultRangeFromQuery(query));
    return ret;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::searchDocumentsIterateResultdocs(
    const AttributeQuery& query,
    TRexUtils::BitVector& result,
    ltt::vector<float>* ranks,
    BIT_OPERATOR op,
    int resultBitsSet,
    SingleQueryPredicate<ValueType, DictType>& qp)
{
    ERRCODE ret = AERC_OK;
    qp.setIgnoreResultDocs();
    qp.optimizeForValueTest();
    TRexUtils::BitVector::iterator bitit = result.begin();
    switch (op) {
        case BIT_OPERATOR_AND: {
            const unsigned countRows = (unsigned)m_values.itsDocuments.size();
            unsigned prevRow = (unsigned)*bitit;
            if (prevRow >= countRows) {
                result.clear();
                return AERC_OK; // no hits from previous column can be hit in the current column
            }
            int noRanges = 0;
            int maxRanges = resultBitsSet > 0 ? resultBitsSet : (int)result.numSet();
            maxRanges = maxRanges >> 13;
            _STL::vector<unsigned> ranges;
            ranges.reserve(600);
            ranges.push_back(prevRow);

            for (; !bitit.isEnd(); ++bitit) {
                const unsigned row = (unsigned)*bitit;
                if (row != prevRow) {
                    ++noRanges;
                    ranges.push_back(prevRow > countRows ? countRows : prevRow);
                    if (row >= countRows) {
                        ranges.push_back(countRows - 1); // insert an empty (dummy range) [countRows-1, countRows-1)
                        prevRow = countRows - 1;
                        break; // already at the end of the valid rows for this column
                    } else
                        ranges.push_back(row);
                    if (noRanges > maxRanges) {
                        ranges.clear();
                        break;
                    }
                }
                prevRow = row + 1;
            }
            ranges.push_back(prevRow > countRows ? countRows : prevRow);

            if (!qp.getQueryHasResultDocs() && qp.isRangeOnlyQuery()) {
                if (noRanges < maxRanges) {
                    int rangeFrom, rangeTo;
                    qp.getValueRange(rangeFrom, rangeTo);

#ifdef NORELEASE
                    for (auto rIt = ranges.begin(); rIt != ranges.end(); rIt += 2) {

                        if (*rIt < countRows || *(rIt + 1) <= countRows) {
                            Diagnose::TraceStream traceStream(TRACE_ATTR, Diagnose::Trace_Error, __FILE__, __LINE__);
                            traceStream << "TEMPORARY TRACING FOR BUG 136326, resultBitsSet=" << resultBitsSet
                                        << ", result.numSet()" << result.numSet()
                                        << ", countRows=" << countRows
                                        << ", noRanges=" << noRanges
                                        << ", maxRanges=" << maxRanges
                                        << ", itsDocuments.size()=" << m_values.itsDocuments.size() << ": ";
                            for (size_t i = 0; i < ranges.size(); ++i) {
                                traceStream << " " << ranges[i];
                            }
                        }

                        DEV_ASSERT_0(*rIt < countRows);
                        DEV_ASSERT_0(*(rIt + 1) <= countRows);
                    }
#endif // NORELEASE

                    TRexUtils::BitVector bv;
                    bv.resize(countRows);
                    m_values.itsDocuments.mgetSearch(ranges, rangeFrom, rangeTo, bv);
                    result.opAnd(bv);
                    break;
                }
            }

            size_t n = (size_t)(m_values.getMaxDocid() + 1);
            if (result.size() < n)
                n = result.size();
            ResultRange resRange = resultRangeFromQuery(query);
            if (!TRexUtils::Parallel::Context::isJob()
                && (TRexUtils::Parallel::mode == TRexUtils::Parallel::Force || (TRexUtils::Parallel::mode == TRexUtils::Parallel::Auto && n >= PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER))
                && n > 1
                && !query.getNeedRanks()
                && resRange.includesEverything(0)
                && (!qp.getQueryHasResultDocs() || qp.getQueryResultDocs().isBitVector())) {
                ret = searchDocumentsIterateDocidResultParallel(m_values.itsDocuments,
                    m_values.itsDict.size(), query, result, op, qp, AttributeValueContainer::getAttributeLocation());
                break;
            }

            bitit = result.begin();
            while (!bitit.isEnd()) {
                if (!qp.includeDocid(*bitit))
                    result.clear_unchecked(*bitit);
                ++bitit;
            }
            break;
        }

        case BIT_OPERATOR_AND_NOT: {
            size_t n = (size_t)(m_values.getMaxDocid() + 1);
            if (result.size() < n)
                n = result.size();
            ResultRange resRange = resultRangeFromQuery(query);
            if (!TRexUtils::Parallel::Context::isJob()
                && (TRexUtils::Parallel::mode == TRexUtils::Parallel::Force || (TRexUtils::Parallel::mode == TRexUtils::Parallel::Auto && n >= PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER))
                && n > 1
                && !query.getNeedRanks()
                && resRange.includesEverything(0)
                && (!qp.getQueryHasResultDocs() || qp.getQueryResultDocs().isBitVector())) {
                ret = searchDocumentsIterateDocidResultParallel(m_values.itsDocuments,
                    m_values.itsDict.size(), query, result, op, qp, AttributeValueContainer::getAttributeLocation());
                break;
            }
            if (!qp.getQueryHasResultDocs() && qp.isRangeOnlyQuery()) {
                int rangeFrom, rangeTo;
                qp.getValueRange(rangeFrom, rangeTo);
                const size_t startId = *bitit;
                const size_t endId = min(result.highestBitSet() + 1, (size_t)n);
                if (endId > startId && rangeTo > 0) // rangeTo == 0 if nothing to search
                {
                    TRexUtils::BitVector bv(max(result.size(), (size_t)n));
                    m_values.itsDocuments.mgetSearch(startId, endId - startId, rangeFrom, rangeTo, bv);
                    result.opNotAnd(bv);
                }
            } else {
                while (!bitit.isEnd()) {
                    if (qp.includeDocid(*bitit))
                        result.clear_unchecked(*bitit);
                    ++bitit;
                }
            }
            break;
        }
        default:
            ret = TREX_ERROR::AERC_NOT_IMPLEMENTED;
    }
    cutBitVector(result, resultRangeFromQuery(query));
    return ret;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getDocidForUniqueValue(const TRexCommonObjects::InputValueWrapper& values, TRexCommonObjects::VisibilityChecker& checker, QueryStats& queryStats, size_t from, size_t to, bool useLock)
{

    AE_TEST_RET(lazyLoad());
    IndexInfoMustBeSetInThisScope invertedIndexScope(queryStats);

    const size_t end = min(values.size(), to);
    const size_t start = from;

    if (start >= end) {
        queryStats.setInvertedIndexAccess(InvertedIndexAccess::SKIPPED_EMPTY);
        return AERC_OK;
    }

    if (!isIndexed()) {
        // this cannot happen when it is a primary key
        // only if there was an oom exception during index build up phase
        DEV_ASSERT_NOARG(!isIndexDefined(), "No index available but we are a primary key. There might be an oom during index build up."); // lets see if we can keep this assert
        return AttributeValueContainer::getDocidForUniqueValue(values, checker, queryStats, from, to, useLock);
    }
    queryStats.setInvertedIndexAccess(InvertedIndexAccess::YES);

    ERRCODE ret = AERC_OK;

    SingleIndexIterator iit(*m_values.itsIndex);
    ValueType value;
    _STL::string tempString;
    int valueId;
    DocumentId docid;
    for (size_t pos = start; pos < end; ++pos) // TODO: parallelize here for multiple values, but currently there is only one value
    {
        values.get(pos, tempString);
        ret = value.set(m_values.itsDict.getDefinition_const(), tempString);
        if (ret != AERC_OK)
            break;
        if (!m_values.itsDict.findValue(value, valueId, true)) {
            continue; // value not found in dict ???
        }

        if (!iit.findValue(valueId) || !iit.getFirstDocid(docid)) {
            // value not found in index
            continue;
        }

        if (checker.isVisible(pos, docid)) {
            // docid is visible, we're done here
            continue;
        }
        bool foundVisibleRow = false;
        while (iit.getNextDocid(docid)) {
            if (checker.isVisible(pos, docid)) {
                // docid is visible, we're done here
                foundVisibleRow = true;
                break; // exit inner loop
            }
        }
        if (foundVisibleRow)
            continue;
        checker.noVisibleDocIdFound(pos);
    }
    return ret;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getEstimatedUncompressedSize(size_t& size)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));

    TRexUtils::BitVector* bv = nullptr;
    _STL::vector<int> result;
    ERRCODE ret = this->bwGetValueIdCountsFromDocids(bv, result);
    if (result.size() != (size_t)m_values.itsDict.size()) {
        return AE_TRACE_NOT_IMPLEMENTED("getEstimatedUncompressedSize");
    }
    typename DictType::iterator dictIt;
    _STL::string stringBuffer;
    int n = m_values.itsDict.size();
    size = 0;
    for (int i = 0; i < n; ++i) {
        const _STL::string& valueString = m_values.getValueString(i, dictIt, stringBuffer);
        size += valueString.size() * result[i];
    }
    return ret;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::sortDocuments(
    const ltt::vector<DocumentId>& docids,
    const TRexUtils::BitVector* equalsIn,
    const _STL::string& locale,
    ltt::vector<int>& order,
    TRexUtils::BitVector* equalsOut,
    bool ascending,
    int maxResults, bool equalsExtendRange,
    ITERATION_METHOD im,
    bool invertNullOrder)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));

    if (ValueType::supportsLocale && !locale.empty() && (locale != "BINARY" || !m_values.itsDict.isInBinaryOrder()))
        return sortDocumentsLocale(
            docids, equalsIn, locale, order, equalsOut, ascending, invertNullOrder,
            maxResults, equalsExtendRange);

    // ------------------------------------------------------------------------
    // Case-insensitive sorting optimization

    TRACE_INFO(TRACE_AE_CASE_INSENSITIVE_SORT,
        "Input:"
            << ", supportsLocale=" << ValueType::supportsLocale
            << ", valueType=" << ValueType::valueClass
            << ", locale=" << locale
            << ", isIndexed=" << isIndexed()
            << ", hasEqualsIn=" << (equalsIn != nullptr)
            << ", hasEqualsOut=" << (equalsOut != nullptr)
            << ", enableInvertedIndexSort=" << AttributeEngineConfig::enableInvertedIndexSort
            << ", enableRadixSort=" << AttributeEngineConfig::enableRadixSort
            << ", isIndexed=" << isIndexed()
            << ", dictIsInBinaryOrder=" << m_values.itsDict.isInBinaryOrder()
            << ", dataVector.size()=" << getValues().itsDocuments.size()
            << ", docids.size()=" << docids.size()
            << ", nullValueId=" << getValues().itsDict.size()
            << ", nonNullRows=" << getValues().itsDocumentCount
            << ", maxResults=" << maxResults
            << ", invertNullOrder=" << invertNullOrder);

    // Case-insensitive inverted index sort
    if (locale.empty()
        && (ValueType::valueClass == VALUE_CLASS_STRING || ValueType::valueClass == VALUE_CLASS_FIXEDSTRING)
        && equalsIn == nullptr && equalsOut == nullptr // no multi-column sort for now ...
        && AttributeEngineConfig::enableInvertedIndexSort
        && isIndexed()
        && ascending == true && invertNullOrder == false
        // - since cpu time consumption is _much_ better than partial/total (aka. heap/quick) sort, we employ
        //   inverted index sort as a full replacement for normal sorter
        && maxResults > 0) {
        return InvertedIndexSorter::sort(docids, maxResults, getValues().itsDocuments,
            *(getValues().itsIndex), getValues().itsDict.size(), getValues().itsDocumentCount, AttributeValueContainer::getAttributeLocation(),
            order);
    }

    // Case-insensitive radix sort
    if (ValueType::supportsLocale && locale.empty()
        && (ValueType::valueClass == VALUE_CLASS_STRING || ValueType::valueClass == VALUE_CLASS_FIXEDSTRING)
        && equalsIn == nullptr && equalsOut == nullptr // no multi-column sort for now ...
        && AttributeEngineConfig::enableRadixSort
        && ascending == true && invertNullOrder == false
        // - cpu time consumption beats total sorter, but not partial sorter --> employ radix sort only as replacement
        //   for total sorting (i.e., for large maxResults)
        && maxResults > 0 && static_cast<size_t>(maxResults) > AttributeEngineConfig::radixSortMinDocids && static_cast<float>(maxResults) / docids.size() > AttributeEngineConfig::radixSortDocidRatio) {
        return RadixSorter::sort(docids, maxResults, getValues().itsDocuments, getValues().itsDict.size(), order);
    }

    // ------------------------------------------------------------------------

    LtDocidSingle isLess(m_values.itsDocuments, m_values.itsDict.size(), ascending, invertNullOrder);
    EqSingleValues isEqual(m_values.itsDocuments, m_values.itsDict.size());
    Sorter<int, LtDocidSingle, EqSingleValues> sorter(isLess, isEqual);
    return sorter.sort(docids, equalsIn, order, equalsOut, maxResults, equalsExtendRange);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::sortDocumentsLocale(
    const ltt::vector<DocumentId>& docids,
    const TRexUtils::BitVector* equalsIn,
    const _STL::string& locale,
    ltt::vector<int>& order,
    TRexUtils::BitVector* equalsOut,
    bool ascending,
    bool invertNullOrder,
    int maxResults,
    bool equalsExtendRange)
{
    return AE_TRACE_NOT_IMPLEMENTED("sortDocumentsLocale");
}

#define DEFINE_SPECIALIZED_METHOD(ValueType)                                          \
    template <>                                                                       \
    ERRCODE SingleAttribute<ValueType, ValueDict<ValueType>>::sortDocumentsLocale(    \
        const ltt::vector<DocumentId>& docids,                                        \
        const TRexUtils::BitVector* equalsIn,                                         \
        const _STL::string& locale,                                                   \
        ltt::vector<int>& order,                                                      \
        TRexUtils::BitVector* equalsOut,                                              \
        bool ascending,                                                               \
        bool invertNullOrder,                                                         \
        int maxResults,                                                               \
        bool equalsExtendRange)                                                       \
    {                                                                                 \
        LtDocidSingleLocale<ValueType,                                                \
            ValueDict<ValueType>>                                                     \
            isLess(                                                                   \
                m_values.itsDict, m_values.itsDocuments, ascending, invertNullOrder); \
        ERRCODE rc = isLess.setLocale(locale);                                        \
        if (rc != AERC_OK)                                                            \
            return rc;                                                                \
        _STL::vector<int> vidSortOrder;                                               \
        if (static_cast<size_t>(m_values.itsDict.size()) < (docids.size() / 2))       \
            isLess.calcAndUseVidOrder(vidSortOrder);                                  \
        EqSingleValues isEqual(m_values.itsDocuments,                                 \
            m_values.itsDict.size());                                                 \
        Sorter<int, LtDocidSingleLocale<ValueType,                                    \
                        ValueDict<ValueType>>,                                        \
            EqSingleValues>                                                           \
            sorter(isLess, isEqual);                                                  \
        return sorter.sort(docids, equalsIn, order, equalsOut,                        \
            maxResults, equalsExtendRange);                                           \
    }

#ifdef SINGLE_ATTRIBUTE_STRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::StringAttributeValue)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXEDSTRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedStringAttributeValue)
#endif
#ifdef SINGLE_ATTRIBUTE_ALPHANUM
DEFINE_SPECIALIZED_METHOD(TrexTypes::AlphanumAttributeValue)
#endif
#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::loadFileVersion(
    AttributeDeserializer& f,
    int fileversion,
    SingleValues<ValueType, DictType>& values,
    InvertedIndexState& indexState)
{
    ERRCODE rc;
    switch (fileversion) {
        case ATTRIBUTEFILE_SINGLE_V1:
            rc = loadFileSingleV1(f, values, false, indexState);
            break;
        case ATTRIBUTEFILE_SINGLE_V1C:
            rc = loadFileSingleV1(f, values, true, indexState);
            break;
        case ATTRIBUTEFILE_SINGLE_NVM_1:
            f.setUseNvmFormat(true);
            rc = loadFileSingleV1(f, values, true, indexState);
            break;
        case ATTRIBUTEFILE_ID_V1:
            if (m_definition.getAttributeFlags() & TRexEnums::ATTRIBUTE_FLAG_LOB) {
                rc = loadFileLobV1(f, values, indexState);
                break;
            }
        // intentional fallthrough
        default:
            rc = AE_TRACE_ERROR3(TREX_ERROR::AERC_UNSUPPORTED_FILE_VERSION, "loadFileVersion", m_attributeStore.getIndexId(), m_name, m_attributeId);
    }
#ifdef AE_DELTA_DICT
    if (values.itsDict.supportDeltaDict && values.itsDict.getDelta() != nullptr
        && deltaDictTrace.doesTrace(TrexTrace::TL_INFO)) {
        DEF_LOCAL_TRACE_STREAM(f, deltaDictTrace, Diagnose::Trace_Info, __FILE__, __LINE__);
        f << "loadFileVersion completed with delta dict status ";
        values.itsDict.getDelta()->status.trace(f);
        f << ", rc = " << rc << TrexTrace::endl;
    }
#endif // AE_DELTA_DICT
    return rc;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getMostFrequentValueInfos(
    MostFrequentValueInfos& mostFrequentValueInfos)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoadNoIndex(handle));

    DTEST_EXEC_WHEN_REACHED(
        "DTEST_AE_SA_GETMOSTFREQUENTVALUEINFOS", m_attributeStore.getIndexId().nameWithDBAndNamespace(),
        { return TREX_ERROR::AERC_FAILED; });
    // --> get number of distinct values including the undefined value (NULL)
    const size_t countDistinctValues = m_values.itsDict.size() + 1;
    // --> init value counts
    ltt::vector<DocumentId> valueCounts(mostFrequentValueInfos.get_allocator());
    valueCounts.raw_resize(countDistinctValues);
    memset(&valueCounts[0], 0, countDistinctValues * sizeof(DocumentId));
    // --> init mget stuff
    const size_t mgetBufferSize = 1024;
    TRexUtils::MgetStackBuffer<mgetBufferSize> mgetBuffer;
    // 1.determine the count of each value
    const size_t countRows = m_values.itsDocuments.size();
    size_t mgetSize = mgetBufferSize;
    for (size_t mgetIndex = 0; mgetIndex < countRows; mgetIndex += mgetSize) {
        if (mgetIndex + mgetSize > countRows)
            mgetSize = countRows - mgetIndex;
        m_values.itsDocuments.mget(mgetIndex, mgetSize, mgetBuffer);
        // do the work
        for (size_t i = 0; i < mgetSize; ++i)
            ++valueCounts[mgetBuffer[i]];
    }
    --valueCounts[countDistinctValues - 1]; // do not consider docid 0

    // 2.determine the most frequent value
    mostFrequentValueInfos.mostFrequentValueId = countDistinctValues;
    mostFrequentValueInfos.frequency = 0;
    for (size_t i = 0; i < countDistinctValues; ++i) {
        if (valueCounts[i] > (DocumentId)mostFrequentValueInfos.frequency) {
            mostFrequentValueInfos.frequency = valueCounts[i];
            mostFrequentValueInfos.mostFrequentValueId = i;
        }
    }

    // 3.fill a bitvector with the most frequent value positions
    mostFrequentValueInfos.frequentPositions.resize(countRows, false);
    DEV_ASSERT_NOARG(mostFrequentValueInfos.frequentPositions.numSet() == 0, "frequentPositions have to be empty");
    size_t numJobs = (TRexUtils::Parallel::mode == TRexUtils::Parallel::Off || TRexUtils::Parallel::Context::isJob())
        ? 1u
        : min(static_cast<size_t>(Execution::JobExecutor::getInstance().concurrencyHint()),
              countRows / PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER);
    if (numJobs > 1) {
        size_t chunkSize = (countRows / numJobs) & ~MASK; // see BitVector.h
        TRexUtils::Parallel::Context context;
        size_t index = 0;
        ltt::AtomicI64 dummy;
        dummy.set(ltt::numeric_limits<int64_t>::max());
        while (index < countRows) {
            if ((index + chunkSize) > countRows)
                chunkSize = countRows - index; // adapt last chunk
            context.pushJob(new TRexUtils::JobParallelMgetSearch(
                m_values.itsDocuments, index, chunkSize, nullptr,
                mostFrequentValueInfos.mostFrequentValueId, mostFrequentValueInfos.mostFrequentValueId + 1,
                &mostFrequentValueInfos.frequentPositions, nullptr, AttributeValueContainer::getAttributeLocation(), dummy));
            index += chunkSize;
        }
        if (!context.run())
            return context.getErrors().front().getCode();
    } else {
        m_values.itsDocuments.mgetSearch(0, countRows, mostFrequentValueInfos.mostFrequentValueId, mostFrequentValueInfos.mostFrequentValueId + 1, mostFrequentValueInfos.frequentPositions);
    }
    mostFrequentValueInfos.frequentPositions.clear_unchecked(0);

    DEV_ASSERT(
        mostFrequentValueInfos.frequentPositions.numSet() == mostFrequentValueInfos.frequency,
        "number of bits set ($num_set$) differ from frequency ($frequency$) of the most frequent value ($value_id$); size of BV=$size_bv$, value count=$value_count$, col=$col$, num jobs=$num_jobs$",
        ltt::msgarg_uint64("num_set", mostFrequentValueInfos.frequentPositions.numSet())
            << ltt::msgarg_uint("frequency", mostFrequentValueInfos.frequency)
            << ltt::msgarg_uint("value_id", mostFrequentValueInfos.mostFrequentValueId)
            << ltt::msgarg_uint64("size_bv", mostFrequentValueInfos.frequentPositions.size())
            << ltt::msgarg_uint64("value_count", m_values.itsDict.size())
            << ltt::msgarg_text("col", getQualifiedAttributeId())
            << ltt::msgarg_uint64("num_jobs", numJobs));

    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getValueIdsFromDocids(
    const ltt::vector<int>& docids, ltt::vector<int>& valueIds,
    unsigned rangeStart, unsigned rangeEnd)
{
    if (docids.empty())
        return AERC_OK;
    if (rangeEnd == 0 || rangeEnd > static_cast<unsigned>(docids.size()))
        rangeEnd = static_cast<unsigned>(docids.size());
    if (rangeStart >= rangeEnd)
        return TREX_ERROR::AERC_INVALID_PARAMS;

    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoadNoIndex(handle));

    DTEST_EXEC_WHEN_REACHED(
        "DTEST_AE_SA_GETVALUEIDSFROMDOCIDS_EXTENDED", m_attributeStore.getIndexId().nameWithNamespace(),
        { return TREX_ERROR::AERC_FAILED; });

    const DocumentId nullValueId = static_cast<DocumentId>(m_values.itsDict.size());
    return fnGetValueIdsFromDocids<TRexUtils::IndexVector>(m_values.itsDocuments, nullValueId, docids, valueIds, rangeStart, rangeEnd);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getEstimatedCompressedMemSizes(
    const CompressedMemorySizesEstimatorInput& input,
    CompressedMemorySizesEstimatorOutput& output,
    bool useNewValues)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoadNoIndex(handle));

    if (useNewValues && m_newValues == nullptr) {
        DEV_ASSERT_0(false); // as this method is only called from OptimizeCompression and there the columns are pinned
        TRC_ERROR(attributesTrace) << "SingleAttribute::getEstimatedCompressedMemSizes failed: no new values." << TrexTrace::endl;
        return TREX_ERROR::AERC_FAILED;
    }

    CompressedMemorySizesEstimator estimator;
    if (useNewValues)
        estimator.estimateCompressedMemorySizes(m_newValues->itsDocuments, m_newValues->itsDict.size(), input, output);
    else
        estimator.estimateCompressedMemorySizes(m_values.itsDocuments, m_values.itsDict.size(), input, output);

    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::saveFileSingleV1(
    SingleValues<ValueType, DictType>& values,
    AttributeSerializer& f,
    bool compressArrays,
    InvertedIndexState indexState)
{
#ifdef AE_DELTA_DICT
    if (values.itsDict.supportDeltaDict) {
        if (values.itsDict.getDelta() == nullptr) {
            AttributeEngineConfig* cfg = AttributeEngineConfig::getInstance();
            bool enabled = cfg->getStringdictEnableDelta();
            bool always = cfg->getStringdictDeltaAlways();
            if (enabled) {
                values.itsDict.createDelta();
            }
        }
        if (values.itsDict.getDelta() != nullptr) {
            values.itsDict.getDelta()->prepareDictWrite(values.itsDict);
            if (deltaDictTrace.doesTrace(TrexTrace::TL_INFO)) {
                DEF_LOCAL_TRACE_STREAM(f, deltaDictTrace, Diagnose::Trace_Info, __FILE__, __LINE__);
                f << "prepareDictWrite("
                  << m_attributeStore.getIndexId().nameWithNamespace()
                  << "." << m_name << ") set ";
                values.itsDict.getDelta()->status.trace(f);
                f << TrexTrace::endl;
            }
        } else {
            TRC_INFO(deltaDictTrace)
                << "dict of "
                << m_attributeStore.getIndexId().nameWithNamespace()
                << "." << m_name << " supports delta, but has none"
                << TrexTrace::endl;
        }
    }
#endif // AE_DELTA_DICT
    int n = values.itsDocuments.size();
    f.useArrayCompression(compressArrays);
    f.write_int(ATTRIBUTEFILE_MAGIC);
    int fileversion = ATTRIBUTEFILE_SINGLE_V1;
    if (f.getIsNvmWrite()) {
        fileversion = ATTRIBUTEFILE_SINGLE_NVM_1;
        compressArrays = true;
        f.useArrayCompression(true);
        f.setUseNvmFormat(true);
    } else if (compressArrays)
        fileversion = ATTRIBUTEFILE_SINGLE_V1C;
    f.write_int(fileversion);

#ifdef AE_DELTA_DICT
    // if (DictType::supportDeltaDict)
    {
        DictStore<DictType> ds(m_attributeStore.getIndexId(), m_name);
        ds.write(values.itsDict, compressArrays, f);
        if (ds.shouldWriteExternalDict())
            this->setWriteDict(true);
    }
#else // AE_DELTA_DICT
    DictStore<DictType> ds(m_attributeStore.getIndexId(), m_name);
    ds.write(values.itsDict, compressArrays, f);
#endif // AE_DELTA_DICT

    {
        f.write_int(n);
        if ((m_sparseWriteFlags & TRexEnums::ATTRIBUTE_FLAG_PREFIXED) != 0)
            convertAndSaveToSparse<NspDocuments>(values.itsDocuments, values.itsDict.size(), f);
        else if ((m_sparseWriteFlags & TRexEnums::ATTRIBUTE_FLAG_CLUSTERED) != 0)
            convertAndSaveToSparse<ClusterDocuments>(values.itsDocuments, values.itsDict.size(), f);
        else if ((m_sparseWriteFlags & TRexEnums::ATTRIBUTE_FLAG_INDIRECT) != 0)
            convertAndSaveToSparse<IndDocuments>(values.itsDocuments, values.itsDict.size(), f);
        else if ((m_sparseWriteFlags & TRexEnums::ATTRIBUTE_FLAG_RLE) != 0)
            convertAndSaveToSparse<RleDocuments>(values.itsDocuments, values.itsDict.size(), f);
        else if ((m_sparseWriteFlags & TRexEnums::ATTRIBUTE_FLAG_LRLE) != 0)
            convertAndSaveToSparse<LrleDocuments>(values.itsDocuments, values.itsDict.size(), f);
        else if ((m_sparseWriteFlags & TRexEnums::ATTRIBUTE_FLAG_SPARSE) != 0)
            convertAndSaveToSparse<SpDocuments>(values.itsDocuments, values.itsDict.size(), f);
        else
            f.write_index_vector(values.itsDocuments);
    }
    f.write_int(values.itsDocumentCount);
    if (AttributeEngineConfig::getInstance()->getRuntimeStructurePersistence()) {

        if (indexState == IIS_INDEXED) {
            f.openTag(ATTRIBUTEFILE_SINGLEINDEX_TAG);
            values.itsIndex->write(f);
            f.closeTag();
#ifdef DTEST
            {
                // DTEST for Bug 135088. Qualify breakpoint with <schemaname>:<tablename>:<attributeId>
                ltt::ostringstream qualifiedId(getColumnStoreTransientAllocator());
                const TrexBase::IndexName& indexId = m_attributeStore.getIndexId();

                if (!indexId.getSchema().empty())
                    qualifiedId << indexId.getSchema() << ":";

                qualifiedId << indexId.nameOnly() << ":" << m_attributeId;
                // Wait here to allow DTEST_UNBUFFERED_SERIAL_DATA_NEWDB_WRITTEN to be enabled,
                // then flush immediately to trigger DTEST_UNBUFFERED_SERIAL_DATA_NEWDB_WRITTEN.
                // We then have to re-throw the OOM error as flush() eats it.
                DTEST_EXEC_WHEN_REACHED("DTEST_AE_SINGLE_FLUSH_AFTER_WROTE_ITSINDEX", qualifiedId.c_str(),
                                        ERRCODE bpret = f.flush();
                                        if (bpret != AERC_OK) f.throwError(bpret, __FILE__, __LINE__));
            }
#endif
        }

        DataStatistics::ValueIdStatisticsHandle hStats = values.itsDataStats.getHandle();
        if (hStats.is_valid() && hStats->isPersistent()) {
            f.openTag(ATTRIBUTEFILE_DATASTATS_TAG);
            hStats->write(f);
            TRACE_DEBUG(TRACE_DATASTATS_AE, "SingleAttribute::saveFileSingleV1() itsdataStats.write()");
            f.closeTag();
        }
        f.writeTopCountsTag(values.itsTopDocumentCounts);
        f.openTag(ATTRIBUTEFILE_END_TAG);
        f.closeTag();
    }
    return AERC_OK;
}

template <class ValueType, class DictType>
template <class SpDocuments>
void SingleAttribute<ValueType, DictType>::convertAndSaveToSparse(const TRexUtils::IndexVector& value, int valueCount, AttributeSerializer& f)
{
    SpDocuments sparseValue(this->getAttributeLocation());
    sparseValue.compress(value, valueCount);
    sparseValue.write(f);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::loadFileSingleV1(
    AttributeDeserializer& f,
    SingleValues<ValueType, DictType>& values,
    bool compressArrays,
    InvertedIndexState& indexState)
{
    PROF_SCOPE("SingleAttribute::loadFileSingleV1");

    int m, n;
    try {
        f.useArrayCompression(compressArrays);

        indexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
        // TODO(Ani, Robert): RESET DATASTATS_STATE HERE
        values.clear();

        _STL::vector<int> dictRenumber;
        {
            PROF_SCOPE("DictStore::read");
            DictStore<DictType> store(m_attributeStore.getIndexId(), m_name);
            store.read(values.itsDict, dictRenumber, f);
        }
        m = values.itsDict.size();
        values.itsDocuments.setRange(m + 1);

        {
            n = f.read_int();
            {
                PROF_SCOPE("f.read_index_vector");
                f.read_index_vector(values.itsDocuments, n, dictRenumber);
            }
        }
        values.itsDocumentCount = f.read_int();
    } catch (const AttributeStoreFileException&) {
        indexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
        // TODO(Ani, Robert): RESET DATASTATS_STATE HERE
        values.clear();
        ERRCODE ret = AE_TRACE_ERROR(TREX_ERROR::AERC_LOAD_FAILED, "loadFileSingleV1");
        return ret;
    }

    if (AttributeEngineConfig::getInstance()->getRuntimeStructurePersistence()) {
        int tag = ATTRIBUTEFILE_INVALID_TAG;
        size_t tagSize;
        bool calculateTopCountsFromIndex = false;
        while (f.read_tag(tag, tagSize)) {
            bool wasValidTag = true;
            // skip_tag() should not fail but raise an exception
            // instead (one may not continue to read after skipping
            // failed, because the file position is unknown by then;
            // there is no point in remembering if a tag was "valid"
            // or not, we want to skip everything that we may not read
            // and determine if we can read it or not before doing
            // things that lead to undefined.
            switch (tag) {
                case ATTRIBUTEFILE_SINGLEINDEX_TAG: {
                    if (isIndexDefined()) {
                        PROF_SCOPE("SingleAttribute::read_inverted_index");
                        TRACE_INFO(TRACE_CREATE_INDEX, "loading inverted index for " << getQualifiedAttributeId());
                        if (values.itsIndex->read(f, values.itsDocuments, values.itsDocumentCount, values.itsDict.size())) {
                            DEV_ASSERT_0(values.itsIndex->isIndexed());
                            indexState = IIS_INDEXED;
                            calculateTopCountsFromIndex = true;
                        } else {
                            indexState = IIS_NOT_INDEXED;
                            TRACE_INFO(TRACE_CREATE_INDEX, "Error while loading inverted index for " << getQualifiedAttributeId()); // severe errors are traced within the call
                            if (!f.skip_tag()) {
                                TRACE_ERROR(TRACE_CREATE_INDEX, "Cannot skip forward, canceling. " << getQualifiedAttributeId());
                                wasValidTag = false;
                            }
                        }
                    } else {
                        // probably changeAttributeDefinition
                        if (!f.skip_tag()) {
                            TRACE_ERROR(TRACE_CREATE_INDEX, "Cannot skip forward, canceling. " << getQualifiedAttributeId());
                            wasValidTag = false;
                        }
                    }
                    break;
                }
                case ATTRIBUTEFILE_DATASTATS_TAG: {
                    // Always deserialize data statistics when found here.
                    // The catalog will be checked later to ensure that statistics
                    // are still required and the build parameters match.  If not,
                    // the deserialized object will be dropped or replaced.
                    bool valid = values.itsDataStats.read(f, tagSize);
                    TRACE_DEBUG(TRACE_DATASTATS_AE, "itsdataStats.read()");
                    DEV_ASSERT_0(!values.itsDataStats.getHandle().is_valid() || valid);
                    break;
                }
                case ATTRIBUTEFILE_TOP_COUNTS_TAG:
                    if (f.readTopCountsTag(values.itsTopDocumentCounts)) {
                        f.setTopCountsLoaded(true);
                    } else {
                        if (!f.skip_tag()) {
                            TRACE_ERROR(TRACE_CREATE_INDEX, "Cannot skip forward, canceling. " << getQualifiedAttributeId());
                            wasValidTag = false;
                        }
                    }
                    break;
                case ATTRIBUTEFILE_END_TAG:
                    if (!f.skip_tag()) {
                        TRACE_ERROR(TRACE_CREATE_INDEX, "skip end tag failed loading " << getQualifiedAttributeId());
                    }
                    wasValidTag = false;
                    break;
                default:
                    if (!f.skip_tag()) {
                        TRACE_ERROR(TRACE_CREATE_INDEX, "Cannot skip forward, canceling. " << getQualifiedAttributeId());
                        wasValidTag = false;
                    }
                    TRC_WARNING(attributesTrace) << "found unknown tag " << tag << " while loading data for " << getQualifiedAttributeId() << TrexTrace::endl;
            } // switch (tag)
            if (!wasValidTag) {
                break;
            }
        } // while (f.read_tag())
        if (!f.getTopCountsLoaded() && calculateTopCountsFromIndex)
            values.itsIndex->calculateTopDocumentCounts(
                values.itsDict.size(), values.itsTopDocumentCounts);
    } // if configured runtime-structure-persistence
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::loadFileLobV1(
    AttributeDeserializer& f,
    SingleValues<ValueType, DictType>& values,
    InvertedIndexState& indexState)
{
    ASSERT_UNREACHABLE;
}

#ifdef SINGLE_ATTRIBUTE_FIXED12
template <>
ERRCODE SingleAttribute<TrexTypes::FixedAttributeValue12, ValueDict<TrexTypes::FixedAttributeValue12>>::loadFileLobV1(
    AttributeDeserializer& f,
    SingleValues<TrexTypes::FixedAttributeValue12, ValueDict<TrexTypes::FixedAttributeValue12>>& values,
    InvertedIndexState& indexState)
{
    itsIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
    itsDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
    f.useArrayCompression(true);

    try {
        _STL::vector<int> dictRenumber;
        DictStore<ValueDict<TrexTypes::FixedAttributeValue12>>(m_attributeStore.getIndexId(), m_name).read(values.itsDict, dictRenumber, f);

        const int numDocuments = f.read_int();
        values.itsDocuments.setRange(values.itsDict.size() + 1);
        f.read_index_vector(values.itsDocuments, numDocuments, dictRenumber);
    } catch (const AttributeStoreFileException& ex) {
        TRC_ERROR(attributesTrace) << "load failed: " << ex.what() << TrexTrace::endl;
        clear();
        return AE_TRACE_ERROR(TREX_ERROR::AERC_LOAD_FAILED, "loadFileLobV1");
    }

    ERRCODE rc = values.calculateTopDocumentCounts(m_attributeStore.getIndexId(), m_name);
    cacheMemoryInfo();

    return rc;
}
#endif

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::importAttribute(AttributeDeserializer& f, bool persistAfterImport)
{
    TRACE_INFO(TRACE_CREATE_INDEX, "SingleAttribute::importAttribute()");
    ERRCODE ret = AERC_OK, ret2;
    Synchronization::UncheckedMutexScope guard1(m_writeLock);
    Synchronization::UncheckedExclusiveScope guard2(m_readLock);
    itsIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
    itsDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
    DocumentId docid;
    _STL::string valueString;
    ValueType value;
    int magic = f.read_int();
    if (magic != ATTRIBUTEFILE_MAGIC)
        return AE_TRACE_ERROR(TREX_ERROR::AERC_WRONG_MAGIC_NUMBER, "importAttribute");
    int fileversion = f.read_int();
    if (fileversion != ATTRIBUTEFILE_DOCORDER_V1 && fileversion != ATTRIBUTEFILE_DOCORDER_MULTI_V1 && fileversion != ATTRIBUTEFILE_DOCORDER_MULTI_V2) {
        TRACE_ERROR(TRACE_CREATE_INDEX, "found file version " << fileversion);
        return AE_TRACE_ERROR3(TREX_ERROR::AERC_UNSUPPORTED_FILE_VERSION,
            "importAttribute", m_attributeStore.getIndexId(), m_name, m_attributeId);
    }
    m_values.clear();
    m_updates.clear();
    int i, n;
    n = f.read_int();
    size_t importAttributeOptimizeAfter = AttributeEngineConfig::importAttributeOptimizeAfter;
    int changedValueCount = 0;
    for (i = 0; i < n; ++i) {
        docid = f.read_int();
        switch (fileversion) {
            case ATTRIBUTEFILE_DOCORDER_MULTI_V1:
                if (f.read_int() != 1) {
                    return AE_TRACE_ERROR3(TREX_ERROR::AERC_FAILED,
                        "importAttribute", m_attributeStore.getIndexId(), m_name, m_attributeId);
                }
                break;
            case ATTRIBUTEFILE_DOCORDER_MULTI_V2:
                if (f.read_int() != 1) {
                    return AE_TRACE_ERROR3(TREX_ERROR::AERC_FAILED,
                        "importAttribute", m_attributeStore.getIndexId(), m_name, m_attributeId);
                } else {
                    int isNull = f.read_int();
                    if (isNull)
                        continue; // Skip NULLs in multi to single value conversion. NULL values will be inserted implicitly.
                }
                break;
            default:
                break;
        }
        f.read_string(valueString);
        if ((ret2 = value.set(m_definition, valueString)) == AERC_OK) {
            m_updates.setDocumentValue(docid, value);
            ++changedValueCount;
        } else
            ret = ret2;

        if (static_cast<size_t>(changedValueCount) >= importAttributeOptimizeAfter) {
            if ((ret2 = optimizeDuringImport()) != AERC_OK)
                ret = ret2;
            changedValueCount = 0;
            importAttributeOptimizeAfter *= AttributeEngineConfig::importAttributeOptimizeAfterFactor;
        }
    }
    if (!m_updates.empty() && (ret2 = optimizeDuringImport()) != AERC_OK)
        ret = ret2;
    if ((ret2 = saveAfterImport(persistAfterImport)) != AERC_OK)
        ret = ret2;
    TRACE_INFO(TRACE_CREATE_INDEX,
        "SingleAttribute::importAttribute() checks index creation on attribute "
            << getQualifiedAttributeId()
            << ", itsIndexState=" << AeSymbols::indexStateString(itsIndexState)
            << ", itsNewIndexState=" << AeSymbols::indexStateString(itsNewIndexState));
    if (ret == AERC_OK)
        ret = checkIndexCreation();
    if (ret == AERC_OK && !isIndexDefined())
        ret = calculateTopDocumentCounts();
    cacheMemoryInfo();
    return ret;
}

#ifdef SINGLE_ATTRIBUTE_LOB
template <>
ERRCODE SingleAttribute<TrexTypes::LobAttributeValue, ValueDict<TrexTypes::LobAttributeValue>>::importAttribute(AttributeDeserializer& f, bool persistAfterImport)
{
    TRACE_INFO(TRACE_CREATE_INDEX, "SingleAttribute::importAttribute()");
    ERRCODE ret = AERC_OK, ret2;

    REL_ASSERT_0(m_attributeStore.getTableOid() != 0);
    PersistenceLayer::OwnerOid oid(m_attributeStore.getTableOid(), m_attributeId, m_attributeStore.getIndexId().partId());
    PersistenceLayer::LobType lobtype = PersistenceLayer::lobTypeFromCSType(m_definition.getAttributeType());

    {
        Synchronization::UncheckedMutexScope guard1(m_writeLock);
        Synchronization::UncheckedExclusiveScope guard2(m_readLock);
        itsIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
        itsDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
        DocumentId docid;
        _STL::string valueString;
        TrexTypes::LobAttributeValue value;
        int magic = f.read_int();
        if (magic != ATTRIBUTEFILE_MAGIC)
            return AE_TRACE_ERROR(TREX_ERROR::AERC_WRONG_MAGIC_NUMBER, "importAttribute");
        int fileversion = f.read_int();
        if (fileversion != ATTRIBUTEFILE_DOCORDER_V1 && fileversion != ATTRIBUTEFILE_DOCORDER_MULTI_V1 && fileversion != ATTRIBUTEFILE_DOCORDER_MULTI_V2) {
            TRACE_ERROR(TRACE_CREATE_INDEX, "found file version " << fileversion);
            return AE_TRACE_ERROR3(TREX_ERROR::AERC_UNSUPPORTED_FILE_VERSION,
                "importAttribute", m_attributeStore.getIndexId(), m_name, m_attributeId);
        }
        m_values.clear();
        m_updates.clear();
        int i, n;
        n = f.read_int();
        size_t importAttributeOptimizeAfter = AttributeEngineConfig::importAttributeOptimizeAfter;
        int changedValueCount = 0;
        for (i = 0; i < n; ++i) {
            docid = f.read_int();
            switch (fileversion) {
                case ATTRIBUTEFILE_DOCORDER_MULTI_V1:
                    if (f.read_int() != 1) {
                        return AE_TRACE_ERROR3(TREX_ERROR::AERC_FAILED,
                            "importAttribute", m_attributeStore.getIndexId(), m_name, m_attributeId);
                    }
                    break;
                case ATTRIBUTEFILE_DOCORDER_MULTI_V2:
                    if (f.read_int() != 1) {
                        return AE_TRACE_ERROR3(TREX_ERROR::AERC_FAILED,
                            "importAttribute", m_attributeStore.getIndexId(), m_name, m_attributeId);
                    } else {
                        int isNull = f.read_int();
                        if (isNull)
                            continue; // Skip NULLs in multi to single value conversion. NULL values will be inserted implicitly.
                    }
                    break;
                default:
                    break;
            }

            f.read_string(valueString);

            try {
                value.assign(valueString, m_definition, oid);
            } catch (const PersistenceLayer::PersistenceException& e) {
                return AE_TRACE_ERROR4(TREX_ERROR::AERC_LOAD_FAILED,
                    "importAttribute", m_attributeStore.getIndexId(), m_name, m_attributeId, e.what(getColumnStoreTransientAllocator(), false).str().c_str());
            }

            m_updates.setDocumentValue(docid, value);
            ++changedValueCount;

            if (static_cast<size_t>(changedValueCount) >= importAttributeOptimizeAfter) {
                if ((ret2 = optimizeDuringImport()) != AERC_OK)
                    ret = ret2;
                changedValueCount = 0;
                importAttributeOptimizeAfter *= AttributeEngineConfig::importAttributeOptimizeAfterFactor;
            }
        }
        if (!m_updates.empty() && (ret2 = optimizeDuringImport()) != AERC_OK)
            ret = ret2;
        if ((ret2 = saveAfterImport(persistAfterImport)) != AERC_OK)
            ret = ret2;
    }
    TRACE_INFO(TRACE_CREATE_INDEX,
        "SingleAttribute::importAttribute() builds index on attribute "
            << getQualifiedAttributeId()
            << ", itsIndexState=" << AeSymbols::indexStateString(itsIndexState)
            << ", itsNewIndexState=" << AeSymbols::indexStateString(itsNewIndexState));
    if (ret == AERC_OK)
        ret = checkIndexCreation();
    if (ret == AERC_OK && !isIndexDefined())
        ret = calculateTopDocumentCounts();
    cacheMemoryInfo();
    return ret;
}
#endif

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::importAttribute(AttributeValueContainer* src, const TRexUtils::BitVector* validDocids, bool persistAfterImport)
{
    return MemoryAvc2::importAttribute(src, validDocids, persistAfterImport);
}

#ifdef SINGLE_ATTRIBUTE_LOB
template <class ValueType>
class HybridLobDictGetter : public PersistenceLayer::HybridLobGetter
{
public:
    typedef ValueDict<ValueType> DictType;
    HybridLobDictGetter(const DictType& dict)
        : m_dict(dict)
    {
    }
    void getGlobalContainerId(size_t pos, PersistenceLayer::GlobalContainerId2& gid) const;
    size_t getSize() const { return m_dict.size(); }
    void getInplaceData(size_t pos, const void*& data, size_t& size, _STL::string& buffer) const;
    size_t getInplaceDataSize(size_t pos) const;

private:
    const DictType& m_dict;
};

template <class ValueType>
void HybridLobDictGetter<ValueType>::getGlobalContainerId(size_t pos, PersistenceLayer::GlobalContainerId2& gid) const
{
    // non-lobs are always in-place
    gid = PersistenceLayer::GlobalContainerId2(PersistenceLayer::LOBTYPE_BLOB);
}

template <>
void HybridLobDictGetter<TrexTypes::LobAttributeValue>::getGlobalContainerId(size_t pos, PersistenceLayer::GlobalContainerId2& gid) const
{
    DictType::iterator dictIt;
    const TrexTypes::LobAttributeValue& value = m_dict.get(pos, dictIt);
    gid = value.getData().getHybridContainerID();
}

// legacy disk lobs
template <>
void HybridLobDictGetter<TrexTypes::FixedAttributeValue12>::getGlobalContainerId(size_t pos, PersistenceLayer::GlobalContainerId2& gid) const
{
    DictType::iterator dictIt;
    const TrexTypes::FixedAttributeValue12& value = m_dict.get(pos, dictIt);
    gid.assignOld(&value);
}

template <>
void HybridLobDictGetter<TrexTypes::FixedAttributeValue12>::getInplaceData(size_t pos, const void*& data, size_t& size, _STL::string& buffer) const
{
    ASSERT_UNREACHABLE;
}

template <>
void HybridLobDictGetter<TrexTypes::LobAttributeValue>::getInplaceData(size_t pos, const void*& data, size_t& size, _STL::string& buffer) const
{
    DictType::iterator dictIt;
    m_dict.get(pos, dictIt);
    buffer = dictIt.value;
    reinterpret_cast<TrexTypes::LobAttributeValue*>(&buffer)->getData().getInplaceData(data, size);
}

template <class ValueType>
void HybridLobDictGetter<ValueType>::getInplaceData(size_t pos, const void*& data, size_t& size, _STL::string& buffer) const
{
    typename DictType::iterator dictIt;
    const ValueType& value = m_dict.get(pos, dictIt);
    buffer = value;
    data = buffer.data();
    size = buffer.size();
}

template <>
size_t HybridLobDictGetter<TrexTypes::FixedAttributeValue12>::getInplaceDataSize(size_t pos) const
{
    ASSERT_UNREACHABLE;
}

template <>
size_t HybridLobDictGetter<TrexTypes::LobAttributeValue>::getInplaceDataSize(size_t pos) const
{
    DictType::iterator dictIt;
    const TrexTypes::LobAttributeValue& value = m_dict.get(pos, dictIt);
    return value.getData().getInplaceDataSize();
}

template <class ValueType>
size_t HybridLobDictGetter<ValueType>::getInplaceDataSize(size_t pos) const
{
    typename DictType::iterator dictIt;
    const ValueType& value = m_dict.get(pos, dictIt);
    return value.size();
}

class HybridLobDictDataSetter : public PersistenceLayer::HybridLobSetter
{
public:
    typedef _STL::map<TrexTypes::LobAttributeValue, _STL::set<int>> DictData;
    HybridLobDictDataSetter(DictData& dictData)
        : m_dictData(dictData)
    {
    }
    void setGlobalContainerId(size_t pos, const PersistenceLayer::GlobalContainerId2& gid);
    void setInplaceData(size_t pos, const void* data, const size_t size, PersistenceLayer::LobType type);
    void setNull(size_t pos);
    bool targetIsSource() const;

private:
    DictData& m_dictData;
};

void HybridLobDictDataSetter::setNull(size_t pos)
{
    // NOOP
}

bool HybridLobDictDataSetter::targetIsSource() const
{
    return false;
}

void HybridLobDictDataSetter::setGlobalContainerId(size_t pos, const PersistenceLayer::GlobalContainerId2& gid)
{
    TrexTypes::LobAttributeValue value;
    value.getData().setContainerID(gid);
    m_dictData[value].insert(static_cast<int>(pos));
}

void HybridLobDictDataSetter::setInplaceData(size_t pos, const void* data, const size_t size, PersistenceLayer::LobType type)
{
    TrexTypes::LobAttributeValue value;
    value.getData().setInplaceData(data, size, type);
    m_dictData[value].insert(static_cast<int>(pos));
}

template <class SourceType>
ERRCODE importLobAttribute(SingleValues<TrexTypes::LobAttributeValue, ValueDict<TrexTypes::LobAttributeValue>>* dst,
    AttributeValueContainer* src,
    const TRexUtils::BitVector* validDocids,
    PersistenceLayer::OwnerOid oid)
{
    dst->clear();

    ERRCODE rc;
    typedef ValueDict<SourceType> SourceDict;
    SourceDict srcDict(src->getDefinition(), nullptr);

    void* ptr = &srcDict;
    if ((rc = src->getAttributeDict(ptr, nullptr, 0, VALUEDICT)) != AERC_OK)
        return rc;

    {
        TRexUtils::IndexVector dummy1, dummy2;

        if ((rc = src->getAttributeIndex(dst->itsDocuments, dummy1, dummy2)) != AERC_OK)
            return rc;
    }

    // new values -> old value-ids
    // when increasing the threshold more than one on-disk values
    // might map to the same in-memory value
    typedef _STL::map<TrexTypes::LobAttributeValue, _STL::set<int>> NewDictData;
    NewDictData newDictData;

    HybridLobDictGetter<SourceType> input(srcDict);
    HybridLobDictDataSetter output(newDictData);

    const TrexTypes::TypeDefinition& dstDefinition = dst->itsDefinition;
    size_t memoryThreshold = dstDefinition.getFractDigits() >= 0
        ? dstDefinition.getFractDigits()
        : ltt::numeric_limits<size_t>::max();
    PersistenceLayer::LobType lobType = PersistenceLayer::lobTypeFromCSType(dstDefinition.getAttributeType());

    PersistenceLayer::LOBStorage().adaptLOBs(input, oid, memoryThreshold, lobType, output, nullptr, true);

    // old value-id -> new value-id
    _STL::vector<int> newValueIds;
    newValueIds.resize(srcDict.size());
    int newValueId = 0;

    rc = dst->itsDict.startSortedAdd(srcDict.size());

    if (rc != AERC_OK)
        return rc;

    for (NewDictData::const_iterator it = newDictData.begin(); it != newDictData.end(); ++it, ++newValueId) {
        rc = dst->itsDict.sortedAdd(it->first);

        if (rc != AERC_OK)
            return rc;

        // translate old value ids to new value ids
        for (_STL::set<int>::const_iterator it2 = it->second.begin(); it2 != it->second.end(); ++it2)
            newValueIds[*it2] = newValueId;
    }

    rc = dst->itsDict.finishSortedAdd();

    if (rc != AERC_OK)
        return rc;

    int srcNullId = srcDict.size();
    int dstNullId = dst->itsDict.size();

    for (size_t i = 0; i < dst->itsDocuments.size(); ++i) {
        int srcValueId = dst->itsDocuments.get(i);
        int dstValueId;

        if (srcValueId == srcNullId) {
            dstValueId = dstNullId;
        } else {
            dstValueId = newValueIds[srcValueId];
            ++dst->itsDocumentCount;
        }

        dst->itsDocuments.set(i, dstValueId);
    }

    return AERC_OK;
}

template <>
ERRCODE SingleAttribute<TrexTypes::LobAttributeValue, ValueDict<TrexTypes::LobAttributeValue>>::importAttribute(AttributeValueContainer* src, const TRexUtils::BitVector* validDocids, bool persistAfterImport)
{
    Synchronization::UncheckedMutexScope guard1(m_writeLock);
    Synchronization::UncheckedExclusiveScope guard2(m_readLock);
    ERRCODE rc = TREX_ERROR::AERC_NOT_IMPLEMENTED;

    PersistenceLayer::OwnerOid oid(m_attributeStore.getTableOid(), m_attributeId, m_attributeStore.getIndexId().partId());

    switch (src->getDefinition().getValueClass()) {
        case VALUE_CLASS_STRING:
            rc = importLobAttribute<TrexTypes::StringAttributeValue>(&m_values, src, validDocids, oid);
            break;
        case VALUE_CLASS_RAW:
            rc = importLobAttribute<TrexTypes::RawAttributeValue>(&m_values, src, validDocids, oid);
            break;
        case VALUE_CLASS_BLOB:
        case VALUE_CLASS_CLOB:
        case VALUE_CLASS_NCLOB:
            rc = importLobAttribute<TrexTypes::LobAttributeValue>(&m_values, src, validDocids, oid);
            break;
        case VALUE_CLASS_FIXED_12:
            if (src->getDefinition().getAttributeFlags() & TRexEnums::ATTRIBUTE_FLAG_LOB)
                rc = importLobAttribute<TrexTypes::FixedAttributeValue12>(&m_values, src, validDocids, oid);
        default:
            break;
    }

    if (rc == AERC_OK) {
        m_values.setAttributeVersion(makeAttributeVersion());

        if ((rc = saveAfterImport(persistAfterImport)) != AERC_OK)
            return rc;

        TRACE_INFO(TRACE_CREATE_INDEX,
            "SingleAttribute::importAttribute() builds index on attribute "
                << getQualifiedAttributeId()
                << ", itsIndexState=" << AeSymbols::indexStateString(itsIndexState)
                << ", itsNewIndexState=" << AeSymbols::indexStateString(itsNewIndexState));
        if ((rc = checkIndexCreation()) != AERC_OK)
            return rc;
        if (!isIndexDefined() && (rc = calculateTopDocumentCounts()) != AERC_OK)
            return rc;

        cacheMemoryInfo();
        return AERC_OK;
    } else if (rc != TREX_ERROR::AERC_NOT_IMPLEMENTED) {
        return rc;
    } else {
        guard2.unlock();
        guard1.unlock();
        return MemoryAvc2::importAttribute(src, validDocids, persistAfterImport);
    }
}
#endif

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::exportAttribute(
    AttributeSerializer& f, const TRexUtils::BitVector* validUdiv, TRexEnums::AttributeType::typeenum targetAttributeType)
{
    TRACE_INFO(TRACE_CREATE_INDEX, "SingleAttribute::exportAttribute()");
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));
    int pairs = 0;
    unsigned valueId, nullid = m_values.itsDict.size();
    size_t i, n = m_values.itsDocuments.size();
    for (i = 0; i < n; ++i)
        if (m_values.itsDocuments.get(i) < nullid)
            ++pairs;
    f.write_int(ATTRIBUTEFILE_MAGIC);
    f.write_int(ATTRIBUTEFILE_DOCORDER_V1);
    f.write_int(pairs);
    typename DictType::iterator dictIt;
    _STL::string stringBuf;
    for (i = 0; i < n; ++i)
        if ((valueId = m_values.itsDocuments.get(i)) < nullid) {
            f.write_int(i);
            const _STL::string& valueString
                = m_values.itsDict.getString(valueId, dictIt, stringBuf);
            f.write_string(valueString);
        }
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::optimizeDuringImport()
{
    TRACE_INFO(TRACE_CREATE_INDEX, "SingleAttribute::optimizeDuringImport()");
    SingleUpdater<ValueType, DictType> updater(
        m_values.itsDefinition,
        m_values.itsDict,
        m_values.itsDocuments,
        m_values.itsDocumentCount,
        m_updates);
    ERRCODE ret = updater.optimize1();
    if (ret != AERC_OK)
        return ret;
    updater.optimize2(
        m_values.itsDict,
        m_values.itsDocuments,
        m_values.itsDocumentCount);
    m_updates.clear();
    cacheMemoryInfo();
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::fill(UDIV to, const _STL::string* value)
{
    Synchronization::UncheckedMutexScope guard(m_writeLock);
    Synchronization::UncheckedExclusiveScope guard1(m_readLock);

    ERRCODE ret = AERC_OK;

    ret = clearOld();
    if (ret != AERC_OK)
        return ret;

    // UDIV 'to' is exclusive (e.g., to=5 --> set docids d0, d1, d2, d3, d4)

    if (to > 1) {
        // data vector:
        m_values.itsDocuments.setBits(1); // just one value --> bit width = 1
        m_values.itsDocuments.resize(to, 0); // resize to desired size, set all to vid=0

        // Note: we could go oom at any point and mess up the old/current
        // version, that's ok as long as we are called from ADD COLUMN and the
        // added column is dropped in case of error

        if (value != nullptr) {
            // inserted value is not-NULL, store it in dictionary:

            ValueType valueType;
            ret = valueType.set(m_definition, *value);
            if (ret != AERC_OK)
                return ret;

            if ((ret = m_values.itsDict.startSortedAdd(1)) != 0
                || (ret = m_values.itsDict.sortedAdd(valueType)) != 0
                || (ret = m_values.itsDict.finishSortedAdd()) != 0)
                return ret;

            m_values.itsDocuments.set(0, 1); // docId=0 is 1 when the dictionary holds a value

            m_values.itsDocumentCount = to - 1;
        }
    }

    // index and top document counts:

    ret = checkIndexCreation();
    if (ret != AERC_OK)
        return ret;

    if (!this->isIndexDefined()) {
        ret = calculateTopDocumentCounts();
        if (ret != AERC_OK)
            return ret;
    }

    cacheMemoryInfo();

    // persist

    ret = saveAfterImport(true);
    if (ret != AERC_OK) {
        return ret;
    }

    return ret;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::changeDefinition(
    const AttributeDefinition& newDefinition,
    const TRexUtils::BitVector* validDocids)
{
    AttributeDefinition oldDefinitionCopy(m_definition), newDefinitionCopy(newDefinition);

    // mask out index flags
    oldDefinitionCopy.setAttributeFlags(oldDefinitionCopy.getAttributeFlags() & ~(UnifiedTable::COLUMN_FLAG_INDEX));
    newDefinitionCopy.setAttributeFlags(newDefinitionCopy.getAttributeFlags() & ~(UnifiedTable::COLUMN_FLAG_INDEX));

    if (!oldDefinitionCopy.isEqual(newDefinitionCopy)) {
        return TREX_ERROR::AERC_NOT_IMPLEMENTED;
    }
    // everything else except index flags is unchanged

    int oldFlags = m_definition.getAttributeFlags();
    int newFlags = newDefinition.getAttributeFlags();

    // topDocumentCounts: in both cases we continue to use the ones were
    // previously created from inverted index or the data vector
    if /* drop inverted index? */ ((oldFlags & UnifiedTable::COLUMN_FLAG_INDEX) && !(newFlags & UnifiedTable::COLUMN_FLAG_INDEX)) {
        m_values.itsIndex.reset();
        itsIndexState = IIS_NOT_DEFINED;
        m_definition = newDefinition;
        m_values.itsDict.getDefinition() = newDefinition;
        cacheMemoryInfo();
        return AERC_OK;
    } else if /* create inverted index? */ (!(oldFlags & UnifiedTable::COLUMN_FLAG_INDEX) && (newFlags & UnifiedTable::COLUMN_FLAG_INDEX)) {
        AE_TEST_RET(lazyLoad());

        itsIndexState = IIS_NOT_INDEXED;

        bool indexCreated = false;
        static config::ConfigValue<bool> _lazy_inverted_index("global", "lazy_inverted_index", false);
        if (!_lazy_inverted_index) {
            m_values.itsIndex = ltt::make_unique<SingleIndex>(getColumnStoreMainIndexSingleAllocator(), getColumnStoreMainIndexSingleAllocator());
            ERRCODE ret2 = checkIndexCreation();
            if (ret2 != AERC_OK)
                return ret2;
            indexCreated = true;
        }
        m_definition = newDefinition;
        m_values.itsDict.getDefinition() = newDefinition;
        if (indexCreated) {
            cacheMemoryInfo();
        }
        return AERC_OK;
    } else {
        // index flag did not change, unclear why we were called at all
    }

    return TREX_ERROR::AERC_NOT_IMPLEMENTED;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getAttributeIndex(
    TRexUtils::IndexVector& v1, TRexUtils::IndexVector& v2, TRexUtils::IndexVector& v3)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));

    v1 = m_values.itsDocuments;
    v2.clear();
    v3.clear();
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getMemoryInfo(MemoryInfo& info, bool grossSize, bool allowLazyLoad, bool newOnly)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    if (allowLazyLoad)
        AE_TEST_RET(lazyLoadNoIndex(handle));
    if (grossSize) {
        if (newOnly)
            info.addMemoryInfo(m_allocatedNewMemoryInfo);
        else {
            info.addMemoryInfo(m_allocatedMemoryInfo);
            info.addMiscSize(m_allocatedNewMemoryInfo.getTotalSize());
        }
        return AERC_OK;
    } else
        return getNetMemoryInfoWhileLocked(info);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getNetMemoryInfoWhileLocked(MemoryInfo& info)
{
    info.addMemoryInfo(m_memoryInfo);
    return AERC_OK;
}

template <class ValueType, class DictType>
inline void SingleAttribute<ValueType, DictType>::cacheMemoryInfo(
    bool itsUpdatesChanged, bool itsNewValuesChanged)
{
    // TODO(Reza,m): cache based on itsUpdatesChanged and itsNewValuesChanged
    m_memoryInfo.reset();
    m_values.getMemoryInfo(m_memoryInfo, false, isIndexed(false));
    m_updates.getMemoryInfo(m_memoryInfo, false);
    m_memoryInfo.addMiscSize(sizeof(*this));

    m_allocatedMemoryInfo.reset();
    m_values.getMemoryInfo(m_allocatedMemoryInfo, true, isIndexed(false));
    m_updates.getMemoryInfo(m_allocatedMemoryInfo, true);

    m_allocatedNewMemoryInfo.reset();
    if (m_newValues != nullptr)
        m_newValues->getMemoryInfo(m_allocatedNewMemoryInfo, true, isIndexed(true));
    m_allocatedNewMemoryInfo.addMiscSize(sizeof(*this));
}

template <class ValueType, class DictType>
ltt::ostream& SingleAttribute<ValueType, DictType>::toStream(ltt::ostream& stream) const
{
    Synchronization::UncheckedMutexScope guard(m_writeLock);
    Synchronization::UncheckedExclusiveScope guard1(m_readLock);
    stream << "[SingleAttribute: ";
    stream << m_values.toStream(stream);
    if (m_newValues)
        stream << m_newValues->toStream(stream);
    stream << m_updates.toStream(stream);
    stream << "]";
    return stream;
}

template <class ValueType, class DictType>
size_t SingleAttribute<ValueType, DictType>::getSizeOfMemoryIntersection(void* corruptRangeBegin, void* corruptRangeBeyond) const throw()
{
    size_t range = 0;
    range += m_values.getSizeOfMemoryIntersection(corruptRangeBegin, corruptRangeBeyond);
    if (m_newValues)
        range += m_newValues->getSizeOfMemoryIntersection(corruptRangeBegin, corruptRangeBeyond);
    range += m_updates.getSizeOfMemoryIntersection(corruptRangeBegin, corruptRangeBeyond);
    return range;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getPageNumaLocationMap(TRexUtils::NumaLocationMap& pageMap) const
{
    Synchronization::UncheckedMutexScope guard(m_writeLock);
    Synchronization::UncheckedExclusiveScope guard1(m_readLock);
    m_values.getPageNumaLocationMap(pageMap);

    if (m_newValues != nullptr) {
        m_newValues->getPageNumaLocationMap(pageMap);
    }

    m_updates.getPageNumaLocationMap(pageMap);
    return AERC_OK;
}

template <class ValueType, class DictType>
SingleQueryPredicate<ValueType, DictType>& SingleAttribute<ValueType, DictType>::initQueryData(
    QueryData& queryData,
    const AttributeQuery& query,
    bool updateResultDocs,
    ERRCODE& ret) const
{
    SingleQueryData<ValueType, DictType>* sqd;
    if (queryData.checkAttributeVersion(this->getAttributeVersion())) {
        QueryDataInternal* iqd = queryData.getInternalQueryData();
        sqd = dynamic_cast<SingleQueryData<ValueType, DictType>*>(iqd);
        if (sqd == nullptr) { // Bug 117108: should not happen (downcast must succeed)
            TRACE_ERROR(TRACE_ATTR, "invalid internal QueryData for " << getQualifiedAttributeId());
            // abort:
            ret = TREX_ERROR::AERC_FAILED;
            sqd = new SingleQueryData<ValueType, DictType>(m_values, query, this->getAttributeVersion());
            queryData.setInternalQueryData(sqd); // transfer ownership of sqd
            return sqd->getQueryPredicate();
        }
        if (updateResultDocs)
            sqd->getQueryPredicate().updateResultDocs(query);
    } else {
        sqd = new SingleQueryData<ValueType, DictType>(m_values, query, this->getAttributeVersion());
        queryData.setInternalQueryData(sqd);

        if (query.getIsPredicate()) {
            ret = AERC_OK;
        } else {
            if (query.meansFuzzySimilarityZero()) {
                const ltt::vector<DocumentId>* resultDocsRef = query.getResultDocsRef();
                if (resultDocsRef != nullptr) {
                    const int countRows = (int)m_values.itsDocuments.size();
                    AttributeQuery& aq = const_cast<AttributeQuery&>(query);
                    aq.clearCompareValueIDs();
                    for (ltt::vector<DocumentId>::const_iterator row = resultDocsRef->begin(); row != resultDocsRef->end(); ++row) {
                        if (*row >= countRows)
                            break; // not a valid row in this column & previous result is sorted
                        aq.insertCompareValueID(m_values.itsDocuments.get(*row));
                    }
                }
            }

            ret = sqd->getQueryPredicate().initQuery(query, getIndexId(), getAttributeId());
        }
    }
    return sqd->getQueryPredicate();
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::saveOld(AttributeSerializer& f)
{
    return save(m_values, f, itsIndexState);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::saveNew(AttributeSerializer& f)
{
    if (m_newValues != nullptr)
        return save(*m_newValues, f, itsNewIndexState);
    else
        return TREX_ERROR::AERC_NO_UPDATED_VERSION;
}

#ifdef AE_DELTA_DICT
template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::saveOldDict(AttributeSerializer& f)
{
    const bool compressArrays = true;
    TRC_INFO(deltaDictTrace)
        << "saveOldDict calling writeComplete, dictSize "
        << m_values.itsDict.size() << TrexTrace::endl;
    DictStore<DictType>(m_attributeStore.getIndexId(), m_name).writeComplete(m_values.itsDict, compressArrays, f);
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::saveNewDict(AttributeSerializer& f)
{
    if (m_newValues == NULL)
        return TREX_ERROR::AERC_FAILED;
    const bool compressArrays = true;
    TRC_INFO(deltaDictTrace)
        << "saveNewDict calling writeComplete" << TrexTrace::endl;
    DictStore<DictType>(m_attributeStore.getIndexId(), m_name).writeComplete(m_newValues->itsDict, compressArrays, f);
    return AERC_OK;
}
#endif // AE_DELTA_DICT

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::loadOld(AttributeDeserializer& f)
{
    return load(m_values, f, itsIndexState, false);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::loadNew(AttributeDeserializer& f)
{
    if (m_newValues == nullptr) {
        m_newValues = new (getColumnStoreMainUncompressedAllocator()) SingleValues<ValueType, DictType>(m_definition, this);
        itsNewIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
        itsNewDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
    }
    return load(*m_newValues, f, itsNewIndexState, true);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::save(
    SingleValues<ValueType, DictType>& values,
    AttributeSerializer& f,
    InvertedIndexState indexState)
{
    return saveFileSingleV1(values, f, true, indexState);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::load(
    SingleValues<ValueType, DictType>& values,
    AttributeDeserializer& f,
    InvertedIndexState& indexState,
    bool useNewValues)
{
    ERRCODE ret = AERC_OK;
    int magic = f.read_int();
    if (magic != ATTRIBUTEFILE_MAGIC)
        return AE_TRACE_ERROR(TREX_ERROR::AERC_WRONG_MAGIC_NUMBER, "load");
    int fileversion = f.read_int();
    ret = loadFileVersion(f, fileversion, values, indexState);

    if (ret == AERC_OK) {
        ret = this->buildDataStatisticsNoLock(useNewValues);
    }

    values.setAttributeVersion(makeAttributeVersion());
    cacheMemoryInfo();
    return ret;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::clearOld()
{
    m_values.clear();
    itsIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
    itsDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
    cacheMemoryInfo();
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::clearNew()
{
    if (m_newValues != nullptr) {
        delete m_newValues;
        m_newValues = nullptr;
    }
    itsNewIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
    itsNewDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
    cacheMemoryInfo();
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::close()
{
    return MemoryAvc2::close();
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::remove(const bool enforceWriteRedoForMain)
{
    return MemoryAvc2::remove(enforceWriteRedoForMain);
}

template <class ValueType, class DictType>
bool SingleAttribute<ValueType, DictType>::hasUpdatesInMemory() const
{
    return !m_updates.empty() || (m_newValues != nullptr && m_newValues->alreadyContainsUpdates);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::paInitNewValues()
{
    if (m_newValues != nullptr) {
        return TREX_ERROR::AERC_FAILED;
    }
    m_newValues = new (getColumnStoreMainUncompressedAllocator()) SingleValues<ValueType, DictType>(m_definition, this);
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::paInitNewSplitDocuments(
    AttributeValueContainer* sourceAttribute,
    const TRexUtils::BitVector& splitRows,
    const ltt::vector<unsigned int>& mappedValueIds,
    const unsigned int& nullIndicator)
{

    paComputeSplitDocuments(m_newValues->itsDocuments, sourceAttribute, splitRows, mappedValueIds, nullIndicator);
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::paInitNewMergeDocuments(
    const ltt::vector<AttributeValueContainer*>& sourceAttributes,
    const ltt::vector<ltt::vector<unsigned int>>& mappedValueIds,
    const ltt::vector<TRexUtils::BitVector>& validRows,
    const unsigned int& nullIndicator,
    size_t& aCountTotal)
{

    paComputeMergeDocuments(m_newValues->itsDocuments, sourceAttributes, mappedValueIds, validRows, nullIndicator, aCountTotal);
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::updateOldIntoNew()
{
    ERRCODE ret = AERC_OK;
    if (m_newValues == nullptr || !m_newValues->alreadyContainsUpdates) {
        SingleUpdater<ValueType, DictType> updater(
            m_values.itsDefinition,
            m_values.itsDict,
            m_values.itsDocuments,
            m_values.itsDocumentCount,
            m_updates);
        ret = updater.optimize1();
        if (ret != AERC_OK)
            return ret;
        if (m_newValues == nullptr) {
            m_newValues = new (getColumnStoreMainUncompressedAllocator()) SingleValues<ValueType, DictType>(m_definition, this);
            itsNewIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
            itsNewDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
        }
        updater.optimize2(
            m_newValues->itsDict,
            m_newValues->itsDocuments,
            m_newValues->itsDocumentCount);
    }
    m_newValues->setAttributeVersion(makeAttributeVersion());
    m_updates.clear();
    cacheMemoryInfo();
    if (!checkMinMax<ValueType, DictType>(m_attributeStore.getIndexId(), m_name, m_newValues->itsDict))
        ret = TREX_ERROR::AERC_VALUE_OUT_OF_RANGE;
    return ret;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::updateNewIntoNew()
{
    if (m_newValues == nullptr)
        return TREX_ERROR::AERC_NO_UPDATED_VERSION;
    SingleUpdater<ValueType, DictType> updater(
        m_newValues->itsDefinition,
        m_newValues->itsDict,
        m_newValues->itsDocuments,
        m_newValues->itsDocumentCount,
        m_updates);
    ERRCODE ret = updater.optimize1();
    if (ret != AERC_OK)
        return ret;
    SingleValues<ValueType, DictType> updatedValues(m_definition, this);
    updater.optimize2(
        updatedValues.itsDict,
        updatedValues.itsDocuments,
        updatedValues.itsDocumentCount);
    updatedValues.setAttributeVersion(makeAttributeVersion());
    m_newValues->swap(updatedValues);
    m_updates.clear();
    cacheMemoryInfo();
    if (!checkMinMax<ValueType, DictType>(m_attributeStore.getIndexId(), m_name, m_newValues->itsDict))
        ret = TREX_ERROR::AERC_VALUE_OUT_OF_RANGE;
    return ret;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::moveNewToOld()
{
    if (m_newValues == nullptr)
        return TREX_ERROR::AERC_NO_UPDATED_VERSION;
    m_values.swap(*m_newValues);
    itsIndexState = itsNewIndexState;
    itsDataStatsState = itsNewDataStatsState;
    DEV_ASSERT_0(itsIndexState != IIS_INDEXED || m_values.itsIndex->isIndexed());
    delete m_newValues;
    m_newValues = nullptr;
    itsNewIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
    itsNewDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
    cacheMemoryInfo();
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::buildIndex_internal(bool onNewVersion)
{
    if (onNewVersion && m_newValues == nullptr)
        return TREX_ERROR::AERC_FAILED;

    TRACE_INFO(TRACE_CREATE_INDEX, "start buildIndex on " << getQualifiedAttributeId()
                                                          << ", onNewVersion=" << onNewVersion
                                                          << ", itsIndexState=" << AeSymbols::indexStateString(itsIndexState)
                                                          << ", itsNewIndexState=" << AeSymbols::indexStateString(itsNewIndexState));

    ERRCODE rc = AERC_OK;
    bool isCorrect = onNewVersion ? m_newValues->buildIndex() : m_values.buildIndex();
    if (!isCorrect)
        rc = TREX_ERROR::AERC_INDEX_INCONSISTENT;

    // bad_alloc's are propagated upwards

    updateIndexState(rc, onNewVersion);
    DEV_ASSERT_0(rc != AERC_OK || (onNewVersion ? (!m_newValues->itsIndex || m_newValues->itsIndex->isIndexed()) : (!m_values.itsIndex || m_values.itsIndex->isIndexed())));

    cacheMemoryInfo();

    TRACE_INFO(TRACE_CREATE_INDEX, "finished buildIndex on " << getQualifiedAttributeId()
                                                             << ", onNewVersion=" << onNewVersion
                                                             << ", itsIndexState=" << AeSymbols::indexStateString(itsIndexState)
                                                             << ", itsNewIndexState=" << AeSymbols::indexStateString(itsNewIndexState));

    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::buildInvertedIndex(bool onNewVersion)
{
    TRACE_INFO(TRACE_CREATE_INDEX,
        "SingleAttribute::buildInvertedIndex() checks index creation on attribute "
            << getQualifiedAttributeId()
            << ", onNewVersion=" << onNewVersion
            << ", itsIndexState=" << AeSymbols::indexStateString(itsIndexState)
            << ", itsNewIndexState=" << AeSymbols::indexStateString(itsNewIndexState));
    if (!onNewVersion) {
        Synchronization::UncheckedMutexScope guard(m_writeLock);
        Synchronization::UncheckedExclusiveScope guard1(m_readLock);
        return checkIndexCreation();
    } else {
        Synchronization::UncheckedMutexScope guard(m_writeLock);
        return checkIndexCreation(true);
    }
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getInvertedIndexState(InvertedIndexState& status, unsigned& createDuration) const
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    status = (isIndexDefined() && m_values.itsIndex.get() != nullptr) ? m_values.itsIndex->getState() : IIS_NOT_DEFINED;
    createDuration = isIndexed() ? static_cast<unsigned>(m_values.itsIndex->getCreateDuration()) : 0;
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE fnBwGetAggregateMeasures1(
    SingleAttribute<ValueType, DictType>& self,
    int startDocid, int* docids, size_t& count,
    char* measures, int lineSize, int* nullValues, int nullBitmask)
{
    bool allValuesAreZero = false;
    return self.AttributeValueContainer::bwGetAggregateMeasures(
        startDocid, docids, count, measures, lineSize, allValuesAreZero, nullValues, nullBitmask);
}

#define DEFINE_SPECIALIZED_METHOD(ValueType, DictType)                                                                              \
    template <>                                                                                                                     \
    ERRCODE fnBwGetAggregateMeasures1(                                                                                              \
        SingleAttribute<ValueType, DictType>& self,                                                                                 \
        int startDocid,                                                                                                             \
        int* docids,                                                                                                                \
        size_t& count,                                                                                                              \
        char* measures,                                                                                                             \
        int lineSize,                                                                                                               \
        int* nullValues, int nullBitmask)                                                                                           \
    {                                                                                                                               \
        return fnBwGetAggregateMeasures2(self.getValues(), startDocid, docids, count, measures, lineSize, nullValues, nullBitmask); \
    }

#ifdef SINGLE_ATTRIBUTE_FLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::FloatAttributeValue, ValueDict<TrexTypes::FloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DOUBLE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DoubleAttributeValue, ValueDict<TrexTypes::DoubleAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED8
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue8, ValueDict<TrexTypes::FixedAttributeValue8>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED12
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue12, ValueDict<TrexTypes::FixedAttributeValue12>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED16
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue16, ValueDict<TrexTypes::FixedAttributeValue16>)
#endif
#ifdef SINGLE_ATTRIBUTE_DECFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::DecFloatAttributeValue, ValueDict<TrexTypes::DecFloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SDFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::SdfloatAttributeValue, ValueDict<TrexTypes::SdfloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_INT
DEFINE_SPECIALIZED_METHOD(TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::IntAttributeValue, LRLEDict<TrexTypes::IntAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, ValueDict<TrexTypes::DateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, LRLEDict<TrexTypes::DateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, LRLEDict_Month<TrexTypes::DateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DAYDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, ValueDict<TrexTypes::DaydateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, LRLEDict<TrexTypes::DaydateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, LRLEDict_Month<TrexTypes::DaydateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_LONGDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, ValueDict<TrexTypes::LongdateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, LRLEDict<TrexTypes::LongdateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, LRLEDict_Month<TrexTypes::LongdateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, ValueDict<TrexTypes::SeconddateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, LRLEDict<TrexTypes::SeconddateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, LRLEDict_Month<TrexTypes::SeconddateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDTIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::SecondtimeAttributeValue, ValueDict<TrexTypes::SecondtimeAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::SecondtimeAttributeValue, LRLEDict<TrexTypes::SecondtimeAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_TIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::TimeAttributeValue, ValueDict<TrexTypes::TimeAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::TimeAttributeValue, LRLEDict<TrexTypes::TimeAttributeValue>)
#endif

#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetAggregateMeasures(
    int startDocid, int* docids, size_t& count, char* measures, int lineLength, bool& allValuesAreZero,
    int* nullValues, int nullBitmask)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));

    allValuesAreZero = false;
    return fnBwGetAggregateMeasures1(*this, startDocid, docids, count, measures, lineLength, nullValues, nullBitmask);
}

template <class ValueType, class DictType>
ERRCODE fnBwGetAggregateMeasures1(
    SingleAttribute<ValueType, DictType>& self,
    const int* docids, size_t count,
    char* measures, int lineSize,
    int* nullValues, int nullBitmask)
{
    bool hasOnlyZeroValues = false;
    return self.AttributeValueContainer::bwGetAggregateMeasures(
        docids, count, measures, lineSize, hasOnlyZeroValues, nullValues, nullBitmask);
}

#define DEFINE_SPECIALIZED_METHOD(ValueType, DictType)                                                                  \
    template <>                                                                                                         \
    ERRCODE fnBwGetAggregateMeasures1(                                                                                  \
        SingleAttribute<ValueType, DictType>& self,                                                                     \
        const int* docids,                                                                                              \
        size_t count,                                                                                                   \
        char* measures,                                                                                                 \
        int lineSize,                                                                                                   \
        int* nullValues, int nullBitmask)                                                                               \
    {                                                                                                                   \
        return fnBwGetAggregateMeasures2(self.getValues(), docids, count, measures, lineSize, nullValues, nullBitmask); \
    }

#ifdef SINGLE_ATTRIBUTE_FLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::FloatAttributeValue, ValueDict<TrexTypes::FloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DOUBLE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DoubleAttributeValue, ValueDict<TrexTypes::DoubleAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED8
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue8, ValueDict<TrexTypes::FixedAttributeValue8>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED12
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue12, ValueDict<TrexTypes::FixedAttributeValue12>)
// Add specialization for FixedAttributeValue12 even if
// AE_NO_COMPRESSED_RAW_DICT is defined, because HashKey relies on it
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue12, CompressedRawDict<TrexTypes::FixedAttributeValue12>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED16
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue16, ValueDict<TrexTypes::FixedAttributeValue16>)
#endif
#ifdef SINGLE_ATTRIBUTE_DECFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::DecFloatAttributeValue, ValueDict<TrexTypes::DecFloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SDFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::SdfloatAttributeValue, ValueDict<TrexTypes::SdfloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_INT
DEFINE_SPECIALIZED_METHOD(TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::IntAttributeValue, LRLEDict<TrexTypes::IntAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, ValueDict<TrexTypes::DateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, LRLEDict<TrexTypes::DateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, LRLEDict_Month<TrexTypes::DateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DAYDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, ValueDict<TrexTypes::DaydateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, LRLEDict<TrexTypes::DaydateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, LRLEDict_Month<TrexTypes::DaydateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_LONGDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, ValueDict<TrexTypes::LongdateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, LRLEDict<TrexTypes::LongdateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, LRLEDict_Month<TrexTypes::LongdateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, ValueDict<TrexTypes::SeconddateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, LRLEDict<TrexTypes::SeconddateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, LRLEDict_Month<TrexTypes::SeconddateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDTIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::SecondtimeAttributeValue, ValueDict<TrexTypes::SecondtimeAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::SecondtimeAttributeValue, LRLEDict<TrexTypes::SecondtimeAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_TIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::TimeAttributeValue, ValueDict<TrexTypes::TimeAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::TimeAttributeValue, LRLEDict<TrexTypes::TimeAttributeValue>)
#endif

#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetAggregateMeasures(
    const int* docids, size_t count, char* measures, int lineLength,
    bool& allValuesAreZero, int* nullValues, int nullBitmask)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));
    allValuesAreZero = false;
    return fnBwGetAggregateMeasures1(*this, docids, count, measures, lineLength, nullValues, nullBitmask);
}

template <class ValueType, class DictType>
ERRCODE fnBwGetAggregateMeasures1(
    SingleAttribute<ValueType, DictType>& self,
    const int* docids, size_t count,
    char* measures, int lineSize, bool& hasNulls,
    TRexUtils::BitVector& validRows)
{
    return self.AttributeValueContainer::bwGetAggregateMeasures(
        docids, count, measures, lineSize, hasNulls, validRows);
}

#define DEFINE_SPECIALIZED_METHOD(ValueType, DictType)                                                              \
    template <>                                                                                                     \
    ERRCODE fnBwGetAggregateMeasures1(                                                                              \
        SingleAttribute<ValueType, DictType>& self,                                                                 \
        const int* docids,                                                                                          \
        size_t count,                                                                                               \
        char* measures,                                                                                             \
        int lineSize,                                                                                               \
        bool& hasNulls,                                                                                             \
        TRexUtils::BitVector& validRows)                                                                            \
    {                                                                                                               \
        return fnBwGetAggregateMeasures2(self.getValues(), docids, count, measures, lineSize, hasNulls, validRows); \
    }

#ifdef SINGLE_ATTRIBUTE_FLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::FloatAttributeValue, ValueDict<TrexTypes::FloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DOUBLE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DoubleAttributeValue, ValueDict<TrexTypes::DoubleAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED8
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue8, ValueDict<TrexTypes::FixedAttributeValue8>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED12
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue12, ValueDict<TrexTypes::FixedAttributeValue12>)
// Add specialization for FixedAttributeValue12 even if
// AE_NO_COMPRESSED_RAW_DICT is defined, because HashKey relies on it
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue12, CompressedRawDict<TrexTypes::FixedAttributeValue12>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED16
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue16, ValueDict<TrexTypes::FixedAttributeValue16>)
#endif
#ifdef SINGLE_ATTRIBUTE_DECFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::DecFloatAttributeValue, ValueDict<TrexTypes::DecFloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SDFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::SdfloatAttributeValue, ValueDict<TrexTypes::SdfloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_INT
DEFINE_SPECIALIZED_METHOD(TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::IntAttributeValue, LRLEDict<TrexTypes::IntAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, ValueDict<TrexTypes::DateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, LRLEDict<TrexTypes::DateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, LRLEDict_Month<TrexTypes::DateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DAYDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, ValueDict<TrexTypes::DaydateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, LRLEDict<TrexTypes::DaydateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, LRLEDict_Month<TrexTypes::DaydateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_LONGDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, ValueDict<TrexTypes::LongdateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, LRLEDict<TrexTypes::LongdateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, LRLEDict_Month<TrexTypes::LongdateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, ValueDict<TrexTypes::SeconddateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, LRLEDict<TrexTypes::SeconddateAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, LRLEDict_Month<TrexTypes::SeconddateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDTIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::SecondtimeAttributeValue, ValueDict<TrexTypes::SecondtimeAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::SecondtimeAttributeValue, LRLEDict<TrexTypes::SecondtimeAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_TIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::TimeAttributeValue, ValueDict<TrexTypes::TimeAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::TimeAttributeValue, LRLEDict<TrexTypes::TimeAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_STRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::StringAttributeValue, ValueDict<TrexTypes::StringAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXEDSTRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedStringAttributeValue, ValueDict<TrexTypes::FixedStringAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_ALPHANUM
DEFINE_SPECIALIZED_METHOD(TrexTypes::AlphanumAttributeValue, ValueDict<TrexTypes::AlphanumAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_RAW
DEFINE_SPECIALIZED_METHOD(TrexTypes::RawAttributeValue, ValueDict<TrexTypes::RawAttributeValue>)
#endif

#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetAggregateMeasures(
    const int* docids, size_t count, char* measures, int lineLength,
    bool& hasNulls, TRexUtils::BitVector& validRows)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));
    return fnBwGetAggregateMeasures1(*this, docids, count, measures, lineLength, hasNulls, validRows);
}
// ---

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::dmGetBWDocumentRestriction(
    const TRexUtils::BitVector& values,
    const TRexUtils::BitVector* validUDIVs,
    TRexUtils::BitVector& documents)
{
    TRACE_INFO(TRACE_SINGLEATTR, "bool dmGetBWDocumentRestriction(...)");
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_BRET(lazyLoad(handle));

    documents.resize(m_values.itsDocuments.size());
    m_values.itsDocuments.mgetSearch(0, m_values.itsDocuments.size(), values, documents);
    if (validUDIVs)
        documents.opAnd(*validUDIVs);

    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetDictAndDocs(
    const _STL::vector<int>* sortedResultDocs,
    const _STL::string& granularity,
    _STL::vector<int>& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));
    if (!granularity.empty())
        return bwGetDictAndDocsGranularity(sortedResultDocs,
            granularity,
            dict,
            valueIds,
            docids);
    else {
        if (isIndexed() && sortedResultDocs != nullptr && sortedResultDocs->size() * 5ull < m_values.itsDocuments.size() && m_values.itsDict.size() * 5ull < m_values.itsDocuments.size())
            // 2011-05-02: don't use if sortedResultDocs is huge (> size/5)
            // XXX why is this not used when sortedResultDocs == NULL?
            return bwGetDictAndDocsIndex(sortedResultDocs,
                dict,
                valueIds,
                docids);
        else
            return bwGetDictAndDocsNoIndex(sortedResultDocs,
                dict,
                valueIds,
                docids);
    }
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetDictAndDocs(
    const _STL::vector<int>* sortedResultDocs,
    const _STL::string& granularity,
    TRexCommonObjects::AllocatedStringVector& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));
    if (!granularity.empty())
        return bwGetDictAndDocsGranularity(sortedResultDocs, granularity, dict, valueIds, docids);
    else {
        if (isIndexed() && sortedResultDocs != nullptr && sortedResultDocs->size() * 5ull < m_values.itsDocuments.size() && m_values.itsDict.size() * 5ull < m_values.itsDocuments.size())
            // 2011-05-02: don't use if sortedResultDocs is huge (> size/5)
            // XXX why is this not used when sortedResultDocs == NULL?
            return bwGetDictAndDocsIndex(sortedResultDocs, dict, valueIds, docids);
        else
            return bwGetDictAndDocsNoIndex(sortedResultDocs, dict, valueIds, docids);
    }
}

template <bool expand>
class SingleAttributeBwGetDictAndDocsHelper
{
public:
    template <class ValueType, class DictType>
    static ERRCODE callBwGetDictAndDocs2(SingleAttribute<ValueType, DictType>* attribute, const _STL::vector<int>* sortedResultDocs, FixedSizeVector& dict, _STL::vector<int>& valueIds, _STL::vector<int>& docids)
    {
        return TREX_ERROR::AERC_NOT_IMPLEMENTED;
    }
};
template <>
class SingleAttributeBwGetDictAndDocsHelper<true>
{
public:
    template <class ValueType, class DictType>
    static ERRCODE callBwGetDictAndDocs2(SingleAttribute<ValueType, DictType>* attribute, const _STL::vector<int>* sortedResultDocs, FixedSizeVector& dict, _STL::vector<int>& valueIds, _STL::vector<int>& docids)
    {
        return attribute->template bwGetDictAndDocs2<true>(sortedResultDocs, dict, valueIds, docids);
    }
};

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetDictAndDocs(
    const _STL::vector<int>* sortedResultDocs,
    const _STL::string& granularity,
    FixedSizeVector& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)
{
    if (!granularity.empty())
        return AttributeValueContainer::bwGetDictAndDocs(sortedResultDocs, granularity, dict, valueIds, docids);
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));
    // return bwGetDictAndDocs2<typename IsNumericTraits<ValueType>::isNumeric>(sortedResultDocs, dict, valueIds, docids);
    return SingleAttributeBwGetDictAndDocsHelper<IsNumericTraits<ValueType>::isNumeric>::callBwGetDictAndDocs2(this, sortedResultDocs, dict, valueIds, docids);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetDictAndDocsNoIndex(
    const _STL::vector<int>* sortedResultDocs,
    _STL::vector<int>& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)

{
    return AE_TRACE_WRONG_TYPE("bwGetDictAndDocsNoIndex");
}

#ifdef SINGLE_ATTRIBUTE_INT
template <>
ERRCODE SingleAttribute<TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>>::bwGetDictAndDocsNoIndex(
    const _STL::vector<int>* sortedResultDocs,
    _STL::vector<int>& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)
{
    return fnBwGetDictAndDocsNoIndex<TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>>(
        m_values.itsDict,
        m_values.itsDocuments,
        sortedResultDocs,
        dict,
        valueIds,
        docids);
}
#endif

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetDictAndDocsNoIndex(
    const _STL::vector<int>* sortedResultDocs,
    TRexCommonObjects::AllocatedStringVector& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)
{
    return fnBwGetDictAndDocsNoIndex<ValueType, DictType>(
        m_values.itsDict,
        m_values.itsDocuments,
        sortedResultDocs,
        dict,
        valueIds,
        docids);
}

template <class ValueType, class DictType>
template <bool expand>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetDictAndDocs2(
    const _STL::vector<int>* sortedResultDocs,
    FixedSizeVector& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)
{
    FixedSizeVectorWrapper<ValueType> wrappedDict(dict);
    return fnBwGetDictAndDocsNoIndex<ValueType, DictType>(
        m_values.itsDict,
        m_values.itsDocuments,
        sortedResultDocs,
        wrappedDict,
        valueIds,
        docids);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetDictAndDocsIndex(
    const _STL::vector<int>* sortedResultDocs,
    _STL::vector<int>& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)
{
    return AE_TRACE_WRONG_TYPE("bwGetDictAndDocsIndex");
}

#ifdef SINGLE_ATTRIBUTE_INT
template <>
ERRCODE SingleAttribute<TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>>::bwGetDictAndDocsIndex(
    const _STL::vector<int>* sortedResultDocs,
    _STL::vector<int>& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)
{
    return fnBwGetDictAndDocsIndex<TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>, _STL::vector<int>>(
        m_values.itsDict,
        *m_values.itsIndex,
        sortedResultDocs,
        m_values.getMaxDocid(),
        dict,
        valueIds,
        docids);
}
#endif

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetDictAndDocsIndex(
    const _STL::vector<int>* sortedResultDocs,
    TRexCommonObjects::AllocatedStringVector& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)
{
    return fnBwGetDictAndDocsIndex<ValueType, DictType, TRexCommonObjects::AllocatedStringVector>(
        m_values.itsDict,
        *m_values.itsIndex,
        sortedResultDocs,
        m_values.getMaxDocid(),
        dict,
        valueIds,
        docids);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetDictAndDocsGranularity(
    const _STL::vector<int>* sortedResultDocs,
    const _STL::string& granularity,
    _STL::vector<int>& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)

{
    return AE_TRACE_WRONG_TYPE("bwGetDictAndDocsNoIndex");
}

#ifdef SINGLE_ATTRIBUTE_INT
template <>
ERRCODE SingleAttribute<TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>>::bwGetDictAndDocsGranularity(
    const _STL::vector<int>* sortedResultDocs,
    const _STL::string& granularity,
    _STL::vector<int>& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)
{
    return fnBwGetDictAndDocsGranularity<TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>>(
        granularity,
        m_values.itsDict,
        m_values.itsDocuments,
        sortedResultDocs,
        dict,
        valueIds,
        docids);
}
#endif

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetDictAndDocsGranularity(
    const _STL::vector<int>* sortedResultDocs,
    const _STL::string& granularity,
    TRexCommonObjects::AllocatedStringVector& dict,
    _STL::vector<int>& valueIds,
    _STL::vector<int>& docids)
{
    return fnBwGetDictAndDocsGranularity<ValueType, DictType>(
        granularity,
        m_values.itsDict,
        m_values.itsDocuments,
        sortedResultDocs,
        dict,
        valueIds,
        docids);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwMergeDictGetDocids(const _STL::vector<int>* sortedResultDocs, const TRexUtils::BitVector* validDocids, const _STL::vector<int>& sortedValues, const _STL::vector<int>& inputRefs, const _STL::vector<int>& nullRef, RefTableFlags flags, _STL::vector<int>& docids, _STL::vector<int>& outputRefs, TRexUtils::BitVector* remainingInput)
{
    return AE_TRACE_WRONG_TYPE("bwMergeDictGetDocids_intJoin");
}

#define DEFINE_SPECIALIZED_METHOD(ValueType, DictType)                                                                                                  \
    template <>                                                                                                                                         \
    ERRCODE SingleAttribute<ValueType, DictType>::bwMergeDictGetDocids(                                                                                 \
        const _STL::vector<int>* sortedResultDocs,                                                                                                      \
        const TRexUtils::BitVector* validDocids,                                                                                                        \
        const _STL::vector<int>& sortedValues,                                                                                                          \
        const _STL::vector<int>& inputRefs,                                                                                                             \
        const _STL::vector<int>& nullRef,                                                                                                               \
        RefTableFlags flags,                                                                                                                            \
        _STL::vector<int>& docids,                                                                                                                      \
        _STL::vector<int>& outputRefs,                                                                                                                  \
        TRexUtils::BitVector* remainingInput)                                                                                                           \
    {                                                                                                                                                   \
        return bwMergeDictGetDocids2<true>(sortedResultDocs, validDocids, sortedValues, inputRefs, nullRef, flags, docids, outputRefs, remainingInput); \
    }

#ifdef SINGLE_ATTRIBUTE_INT
DEFINE_SPECIALIZED_METHOD(TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, ValueDict<TrexTypes::DateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DAYDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, ValueDict<TrexTypes::DaydateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDTIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::SecondtimeAttributeValue, ValueDict<TrexTypes::SecondtimeAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_TIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::TimeAttributeValue, ValueDict<TrexTypes::TimeAttributeValue>)
#endif
#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
template <bool expand>
ERRCODE SingleAttribute<ValueType, DictType>::bwMergeDictGetDocids2(const _STL::vector<int>* sortedResultDocs, const TRexUtils::BitVector* validDocids, const _STL::vector<int>& sortedValues, const _STL::vector<int>& inputRefs, const _STL::vector<int>& nullRef, RefTableFlags flags, _STL::vector<int>& docids, _STL::vector<int>& outputRefs, TRexUtils::BitVector* remainingInput)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));
// vector<ValueType> &sortedValues = *(vector<ValueType> *)&sortedValues0;
#ifdef AE_USE_MERGE_DICT_GET_DOCIDS_INDEX_WITH_ADD_UNMATCHED_AS_NULL
    bool useIndex = isIndexed() && inputRefs.size() == sortedValues.size();
#else // !AE_USE_MERGE_DICT_GET_DOCIDS_INDEX_WITH_ADD_UNMATCHED_AS_NULL:
    bool useIndex = isIndexed() && inputRefs.size() == sortedValues.size() && (flags & RT_ADD_UNMATCHED_AS_NULL) == 0;
#endif // !AE_USE_MERGE_DICT_GET_DOCIDS_INDEX_WITH_ADD_UNMATCHED_AS_NULL
    ERRCODE rc = AERC_OK;
    if (sortedResultDocs == nullptr)
        if (useIndex)
            rc = fnBwMergeDictGetDocidsIndex<SingleIndex, ValueType, DictType, _STL::vector<int>, TrueTest>(
                m_values.itsDict, *m_values.itsIndex,
                TrueTest(),
                validDocids, sortedValues,
                inputRefs, nullRef, flags, docids, outputRefs, remainingInput);
        else
            rc = fnBwMergeDictGetDocidsNoIndex<TRexUtils::IndexVector, ValueType, DictType, _STL::vector<int>, TrueTest>(
                m_values.itsDict, m_values.itsDocuments,
                TrueTest(),
                validDocids, sortedValues,
                inputRefs, nullRef, flags, docids, outputRefs, remainingInput, AttributeValueContainer::getAttributeLocation());
    else if (sortedResultDocs->size() > 511 && sortedValues.size() > 500) {
        TRexUtils::BitVector bv;
        fnBwSetBitsSorted(*sortedResultDocs, bv);
        if (useIndex)
            rc = fnBwMergeDictGetDocidsIndex<SingleIndex, ValueType, DictType, _STL::vector<int>, BitTest>(
                m_values.itsDict, *m_values.itsIndex,
                BitTest(bv),
                validDocids, sortedValues,
                inputRefs, nullRef, flags, docids, outputRefs, remainingInput);
        else
            rc = fnBwMergeDictGetDocidsNoIndex<TRexUtils::IndexVector, ValueType, DictType, _STL::vector<int>, BitTest>(
                m_values.itsDict, m_values.itsDocuments,
                BitTest(bv),
                validDocids, sortedValues,
                inputRefs, nullRef, flags, docids, outputRefs, remainingInput, AttributeValueContainer::getAttributeLocation());
    } else if (useIndex)
        rc = fnBwMergeDictGetDocidsIndex<SingleIndex, ValueType, DictType, _STL::vector<int>, BsearchTest>(
            m_values.itsDict, *m_values.itsIndex,
            BsearchTest(*sortedResultDocs),
            validDocids, sortedValues,
            inputRefs, nullRef, flags, docids, outputRefs, remainingInput);
    else
        rc = fnBwMergeDictGetDocidsNoIndex<TRexUtils::IndexVector, ValueType, DictType, _STL::vector<int>, BsearchTest>(
            m_values.itsDict, m_values.itsDocuments,
            BsearchTest(*sortedResultDocs),
            validDocids, sortedValues,
            inputRefs, nullRef, flags, docids, outputRefs, remainingInput, AttributeValueContainer::getAttributeLocation());
    return rc;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwMergeDictGetDocids(
    const _STL::vector<int>* sortedResultDocs,
    const TRexUtils::BitVector* validDocids,
    const TRexCommonObjects::AllocatedStringVector& sortedValues,
    const _STL::vector<int>& inputRefs,
    const _STL::vector<int>& nullRef,
    RefTableFlags flags,
    _STL::vector<int>& docids,
    _STL::vector<int>& outputRefs,
    TRexUtils::BitVector* remainingInput)
{
    return AE_TRACE_WRONG_TYPE("bwMergeDictGetDocids_stringJoin");
}

template <class ValueType, class DictType>
template <bool expand>
ERRCODE SingleAttribute<ValueType, DictType>::bwMergeDictGetDocids2(const _STL::vector<int>* sortedResultDocs, const TRexUtils::BitVector* validDocids, const TRexCommonObjects::AllocatedStringVector& sortedValues, const _STL::vector<int>& inputRefs, const _STL::vector<int>& nullRef, RefTableFlags flags, _STL::vector<int>& docids, _STL::vector<int>& outputRefs, TRexUtils::BitVector* remainingInput)
{
    unsigned columnCount = (unsigned)nullRef.size();
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));
#ifdef AE_USE_MERGE_DICT_GET_DOCIDS_INDEX_WITH_ADD_UNMATCHED_AS_NULL
    bool useIndex = isIndexed() && inputRefs.size() == sortedValues.size();
#else // !AE_USE_MERGE_DICT_GET_DOCIDS_INDEX_WITH_ADD_UNMATCHED_AS_NULL:
    bool useIndex = isIndexed() && inputRefs.size() == sortedValues.size() && (flags & RT_ADD_UNMATCHED_AS_NULL) == 0;
#endif // !AE_USE_MERGE_DICT_GET_DOCIDS_INDEX_WITH_ADD_UNMATCHED_AS_NULL
    ERRCODE rc = AERC_OK;
    if (sortedResultDocs == nullptr)
        if (useIndex)
            rc = fnBwMergeDictGetDocidsIndex<SingleIndex, ValueType, DictType, TRexCommonObjects::AllocatedStringVector, TrueTest>(
                m_values.itsDict, *m_values.itsIndex,
                TrueTest(),
                validDocids, sortedValues,
                inputRefs, nullRef, flags, docids, outputRefs, remainingInput);
        else
            rc = fnBwMergeDictGetDocidsNoIndex<TRexUtils::IndexVector, ValueType, DictType, TRexCommonObjects::AllocatedStringVector, TrueTest>(
                m_values.itsDict, m_values.itsDocuments,
                TrueTest(),
                validDocids, sortedValues,
                inputRefs, nullRef, flags, docids, outputRefs, remainingInput, AttributeValueContainer::getAttributeLocation());
    else if (sortedResultDocs->size() > 511 && sortedValues.size() > 500) {
        TRexUtils::BitVector bv;
        fnBwSetBitsSorted(*sortedResultDocs, bv);
        if (useIndex)
            rc = fnBwMergeDictGetDocidsIndex<SingleIndex, ValueType, DictType, TRexCommonObjects::AllocatedStringVector, BitTest>(
                m_values.itsDict, *m_values.itsIndex,
                BitTest(bv),
                validDocids, sortedValues,
                inputRefs, nullRef, flags, docids, outputRefs, remainingInput);
        else
            rc = fnBwMergeDictGetDocidsNoIndex<TRexUtils::IndexVector, ValueType, DictType, TRexCommonObjects::AllocatedStringVector, BitTest>(
                m_values.itsDict, m_values.itsDocuments,
                BitTest(bv),
                validDocids, sortedValues,
                inputRefs, nullRef, flags, docids, outputRefs, remainingInput, AttributeValueContainer::getAttributeLocation());
    } else if (useIndex)
        rc = fnBwMergeDictGetDocidsIndex<SingleIndex, ValueType, DictType, TRexCommonObjects::AllocatedStringVector, BsearchTest>(
            m_values.itsDict, *m_values.itsIndex,
            BsearchTest(*sortedResultDocs),
            validDocids, sortedValues,
            inputRefs, nullRef, flags, docids, outputRefs, remainingInput);
    else
        rc = fnBwMergeDictGetDocidsNoIndex<TRexUtils::IndexVector, ValueType, DictType, TRexCommonObjects::AllocatedStringVector, BsearchTest>(
            m_values.itsDict, m_values.itsDocuments,
            BsearchTest(*sortedResultDocs),
            validDocids, sortedValues,
            inputRefs, nullRef, flags, docids, outputRefs, remainingInput, AttributeValueContainer::getAttributeLocation());
    return rc;
}

#define DEFINE_SPECIALIZED_METHOD(ValueType, DictType)                                                                                                  \
    template <>                                                                                                                                         \
    ERRCODE SingleAttribute<ValueType, DictType>::bwMergeDictGetDocids(                                                                                 \
        const _STL::vector<int>* sortedResultDocs,                                                                                                      \
        const TRexUtils::BitVector* validDocids,                                                                                                        \
        const TRexCommonObjects::AllocatedStringVector& sortedValues,                                                                                   \
        const _STL::vector<int>& inputRefs,                                                                                                             \
        const _STL::vector<int>& nullRef,                                                                                                               \
        RefTableFlags flags,                                                                                                                            \
        _STL::vector<int>& docids,                                                                                                                      \
        _STL::vector<int>& outputRefs,                                                                                                                  \
        TRexUtils::BitVector* remainingInput)                                                                                                           \
    {                                                                                                                                                   \
        return bwMergeDictGetDocids2<true>(sortedResultDocs, validDocids, sortedValues, inputRefs, nullRef, flags, docids, outputRefs, remainingInput); \
    }

#ifdef SINGLE_ATTRIBUTE_STRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::StringAttributeValue, ValueDict<TrexTypes::StringAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXEDSTRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedStringAttributeValue, ValueDict<TrexTypes::FixedStringAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_RAW
DEFINE_SPECIALIZED_METHOD(TrexTypes::RawAttributeValue, ValueDict<TrexTypes::RawAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, ValueDict<TrexTypes::SeconddateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_LONGDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, ValueDict<TrexTypes::LongdateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED8
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue8, ValueDict<TrexTypes::FixedAttributeValue8>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED12
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue12, ValueDict<TrexTypes::FixedAttributeValue12>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED16
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue16, ValueDict<TrexTypes::FixedAttributeValue16>)
#endif
#ifdef SINGLE_ATTRIBUTE_DECFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::DecFloatAttributeValue, ValueDict<TrexTypes::DecFloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SDFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::SdfloatAttributeValue, ValueDict<TrexTypes::SdfloatAttributeValue>)
#endif
#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwMergeDictGetValueIdFn(
    const _STL::vector<int>& sortedValues,
    const _STL::vector<int>& inputRefs,
    int nullRef,
    const TRexUtils::BitVector* validDocids,
    const _STL::vector<int>* sortedResultDocs,
    RefTableFlags flags,
    _STL::vector<int>& outputRefs,
    TRexCommonObjects::MultiValueHash* mvHash,
    TRexUtils::BitVector* remainingInput)
{
    return AE_TRACE_WRONG_TYPE("bwMergeDictGetValueIdFn_intJoin");
}

#define DEFINE_SPECIALIZED_METHOD(ValueType, DictType)                                                                                                                                                                                                                                                                                                                                     \
    template <>                                                                                                                                                                                                                                                                                                                                                                            \
    ERRCODE SingleAttribute<ValueType, DictType>::bwMergeDictGetValueIdFn(const _STL::vector<int>& sortedValues, const _STL::vector<int>& inputRefs, int nullRef, const TRexUtils::BitVector* validDocids, const _STL::vector<int>* sortedResultDocs, RefTableFlags flags, _STL::vector<int>& outputRefs, TRexCommonObjects::MultiValueHash* mvHash, TRexUtils::BitVector* remainingInput) \
    {                                                                                                                                                                                                                                                                                                                                                                                      \
        Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());                                                                                                                                                                                                                                                                                                     \
        AE_TEST_RET(lazyLoad(handle));                                                                                                                                                                                                                                                                                                                                                     \
        return fnBwMergeDictGetValueIdFn<ValueType, DictType>(m_values.itsDict, m_values.itsDocuments, sortedValues, inputRefs, nullRef, validDocids, sortedResultDocs, flags, outputRefs, mvHash, remainingInput);                                                                                                                                                                        \
    }

#ifdef SINGLE_ATTRIBUTE_DATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, ValueDict<TrexTypes::DateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DAYDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, ValueDict<TrexTypes::DaydateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDTIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::SecondtimeAttributeValue, ValueDict<TrexTypes::SecondtimeAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_TIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::TimeAttributeValue, ValueDict<TrexTypes::TimeAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_INT
DEFINE_SPECIALIZED_METHOD(TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED8
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue8, ValueDict<TrexTypes::FixedAttributeValue8>)
#endif
#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwMergeDictGetValueIdFn(
    const TRexCommonObjects::AllocatedStringVector& sortedValues,
    const _STL::vector<int>& inputRefs,
    int nullRef,
    const TRexUtils::BitVector* validDocids,
    const _STL::vector<int>* sortedResultDocs,
    RefTableFlags flags,
    _STL::vector<int>& outputRefs,
    TRexCommonObjects::MultiValueHash* mvHash,
    TRexUtils::BitVector* remainingInput)
{
    return AE_TRACE_WRONG_TYPE("bwMergeDictGetValueIdFn_stringJoin");
}

#define DEFINE_SPECIALIZED_METHOD(ValueType, DictType)                                 \
    template <>                                                                        \
    ERRCODE SingleAttribute<ValueType, DictType>::bwMergeDictGetValueIdFn(             \
        const TRexCommonObjects::AllocatedStringVector& sortedValues,                  \
        const _STL::vector<int>& inputRefs,                                            \
        int nullRef,                                                                   \
        const TRexUtils::BitVector* validDocids,                                       \
        const _STL::vector<int>* sortedResultDocs,                                     \
        RefTableFlags flags,                                                           \
        _STL::vector<int>& outputRefs,                                                 \
        TRexCommonObjects::MultiValueHash* mvHash,                                     \
        TRexUtils::BitVector* remainingInput)                                          \
    {                                                                                  \
        Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle()); \
        AE_TEST_RET(lazyLoad(handle));                                                 \
        return fnBwMergeDictGetValueIdFn<ValueType, DictType>(                         \
            m_values.itsDict,                                                          \
            m_values.itsDocuments,                                                     \
            sortedValues,                                                              \
            inputRefs,                                                                 \
            nullRef,                                                                   \
            validDocids,                                                               \
            sortedResultDocs,                                                          \
            flags,                                                                     \
            outputRefs,                                                                \
            mvHash,                                                                    \
            remainingInput);                                                           \
    }

#ifdef SINGLE_ATTRIBUTE_STRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::StringAttributeValue, ValueDict<TrexTypes::StringAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXEDSTRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedStringAttributeValue, ValueDict<TrexTypes::FixedStringAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_RAW
DEFINE_SPECIALIZED_METHOD(TrexTypes::RawAttributeValue, ValueDict<TrexTypes::RawAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, ValueDict<TrexTypes::SeconddateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_LONGDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, ValueDict<TrexTypes::LongdateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED8
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue8, ValueDict<TrexTypes::FixedAttributeValue8>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED12
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue12, ValueDict<TrexTypes::FixedAttributeValue12>)
// Add specialization for FixedAttributeValue12 even if
// AE_NO_COMPRESSED_RAW_DICT is defined, because HashKey relies on it
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue12, CompressedRawDict<TrexTypes::FixedAttributeValue12>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED16
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue16, ValueDict<TrexTypes::FixedAttributeValue16>)
#endif
#ifdef SINGLE_ATTRIBUTE_DECFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::DecFloatAttributeValue, ValueDict<TrexTypes::DecFloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SDFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::SdfloatAttributeValue, ValueDict<TrexTypes::SdfloatAttributeValue>)
#endif
#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetValuesFromValueIds(
    _STL::vector<int>& valueIds, bool skipNegatives)

{
    return AE_TRACE_WRONG_TYPE("bwGetValuesFromValueIds");
}

#define DEFINE_SPECIALIZED_METHOD(ValueType, DictType)                               \
    template <>                                                                      \
    ERRCODE SingleAttribute<ValueType, DictType>::bwGetValuesFromValueIds(           \
        _STL::vector<int>& valueIds, bool skipNegatives)                             \
    {                                                                                \
        return fnBwGetValuesFromValueIds(m_values.itsDict, valueIds, skipNegatives); \
    }

#ifdef SINGLE_ATTRIBUTE_INT
DEFINE_SPECIALIZED_METHOD(TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>)
DEFINE_SPECIALIZED_METHOD(TrexTypes::IntAttributeValue, LRLEDict<TrexTypes::IntAttributeValue>)
#endif
#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetValueIdsFromValues(
    _STL::vector<int>& sortedValues,
    _STL::vector<int>& outputRefs,
    TRexUtils::BitVector*& bv)

{
    return AttributeValueContainer::bwGetValueIdsFromValues(sortedValues, outputRefs, bv);
}

#ifdef SINGLE_ATTRIBUTE_INT
template <>
ERRCODE SingleAttribute<TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>>::bwGetValueIdsFromValues(
    _STL::vector<int>& sortedValues,
    _STL::vector<int>& outputRefs,
    TRexUtils::BitVector*& bv)
{
    TRACE_INFO(TRACE_SINGLEATTR, "ERRCODE bwGetValueIdsFromValues(vector<int> &, vector<int> &, TRexUtils::BitVector *&)");

    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));

    return fnBwGetValueIdsFromValues(m_values.itsDict, sortedValues, outputRefs, bv);
}
#endif

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetSortedValues(const _STL::vector<int>& docids, const _STL::vector<int>& inputRefs, unsigned columnCount, MultiValuePolicy policy, _STL::vector<int>& sortedValues, _STL::vector<int>& outputRefs, RefTableFlags& flags, int singleValueRefs, TRexCommonObjects::MultiValueHash* mvHash, int discard)
{
    return AttributeValueContainer::bwGetSortedValues(docids, inputRefs, columnCount, policy, sortedValues, outputRefs, flags, singleValueRefs, mvHash, discard);
}

#define DEFINE_SPECIALIZED_METHOD(ValueType, DictType)                                     \
    template <>                                                                            \
    ERRCODE SingleAttribute<ValueType, DictType>::bwGetSortedValues(                       \
        const _STL::vector<int>& docids,                                                   \
        const _STL::vector<int>& inputRefs,                                                \
        unsigned columnCount,                                                              \
        MultiValuePolicy mvPolicy,                                                         \
        _STL::vector<int>& sortedValues,                                                   \
        _STL::vector<int>& outputRefs,                                                     \
        RefTableFlags& flags,                                                              \
        int singleValueRefs,                                                               \
        TRexCommonObjects::MultiValueHash* mvHash,                                         \
        int discard)                                                                       \
    {                                                                                      \
        Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());     \
        AE_TEST_RET(lazyLoad(handle));                                                     \
        int rc = fnBwGetSortedValues<TRexUtils::IndexVector, DictType, _STL::vector<int>>( \
            m_values.itsDict,                                                              \
            m_values.itsDocuments,                                                         \
            m_values.countDocuments(),                                                     \
            docids,                                                                        \
            inputRefs,                                                                     \
            columnCount,                                                                   \
            mvPolicy,                                                                      \
            sortedValues,                                                                  \
            outputRefs,                                                                    \
            flags,                                                                         \
            singleValueRefs,                                                               \
            mvHash,                                                                        \
            discard);                                                                      \
        if (rc == TREX_ERROR::AERC_NOT_IMPLEMENTED) {                                      \
            AE_TRACE_NOT_IMPLEMENTED("bwGetSortedValues");                                 \
        }                                                                                  \
        return rc;                                                                         \
    }

#ifdef SINGLE_ATTRIBUTE_INT
DEFINE_SPECIALIZED_METHOD(TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DateAttributeValue, ValueDict<TrexTypes::DateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_DAYDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::DaydateAttributeValue, ValueDict<TrexTypes::DaydateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_TIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::TimeAttributeValue, ValueDict<TrexTypes::TimeAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDTIME
DEFINE_SPECIALIZED_METHOD(TrexTypes::SecondtimeAttributeValue, ValueDict<TrexTypes::SecondtimeAttributeValue>)
#endif
#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetSortedValues(const _STL::vector<int>& docids, const _STL::vector<int>& inputRefs, unsigned columnCount, MultiValuePolicy policy, TRexCommonObjects::AllocatedStringVector& sortedValues, _STL::vector<int>& outputRefs, RefTableFlags& flags, int singleValueRefs, TRexCommonObjects::MultiValueHash* mvHash, int discard)
{
    return AE_TRACE_WRONG_TYPE("bwGetSortedValues");
}

#define DEFINE_SPECIALIZED_METHOD(ValueType, DictType)                                                          \
    template <>                                                                                                 \
    ERRCODE SingleAttribute<ValueType, DictType>::bwGetSortedValues(                                            \
        const _STL::vector<int>& docids,                                                                        \
        const _STL::vector<int>& inputRefs,                                                                     \
        unsigned columnCount,                                                                                   \
        MultiValuePolicy mvPolicy,                                                                              \
        TRexCommonObjects::AllocatedStringVector& sortedValues,                                                 \
        _STL::vector<int>& outputRefs,                                                                          \
        RefTableFlags& flags,                                                                                   \
        int singleValueRefs,                                                                                    \
        TRexCommonObjects::MultiValueHash* mvHash,                                                              \
        int discard)                                                                                            \
    {                                                                                                           \
        Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());                          \
        AE_TEST_RET(lazyLoad(handle));                                                                          \
        return fnBwGetSortedValues<TRexUtils::IndexVector, DictType, TRexCommonObjects::AllocatedStringVector>( \
            m_values.itsDict,                                                                                   \
            m_values.itsDocuments,                                                                              \
            m_values.countDocuments(),                                                                          \
            docids,                                                                                             \
            inputRefs,                                                                                          \
            columnCount,                                                                                        \
            mvPolicy,                                                                                           \
            sortedValues,                                                                                       \
            outputRefs,                                                                                         \
            flags,                                                                                              \
            singleValueRefs,                                                                                    \
            mvHash,                                                                                             \
            discard);                                                                                           \
    }

#ifdef SINGLE_ATTRIBUTE_STRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::StringAttributeValue, ValueDict<TrexTypes::StringAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXEDSTRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedStringAttributeValue, ValueDict<TrexTypes::FixedStringAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_RAW
DEFINE_SPECIALIZED_METHOD(TrexTypes::RawAttributeValue, ValueDict<TrexTypes::RawAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SECONDDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::SeconddateAttributeValue, ValueDict<TrexTypes::SeconddateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_LONGDATE
DEFINE_SPECIALIZED_METHOD(TrexTypes::LongdateAttributeValue, ValueDict<TrexTypes::LongdateAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED8
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue8, ValueDict<TrexTypes::FixedAttributeValue8>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED12
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue12, ValueDict<TrexTypes::FixedAttributeValue12>)
// Add specialization for FixedAttributeValue12 even if
// AE_NO_COMPRESSED_RAW_DICT is defined, because HashKey relies on it
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue12, CompressedRawDict<TrexTypes::FixedAttributeValue12>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXED16
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedAttributeValue16, ValueDict<TrexTypes::FixedAttributeValue16>)
#endif
#ifdef SINGLE_ATTRIBUTE_DECFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::DecFloatAttributeValue, ValueDict<TrexTypes::DecFloatAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_SDFLOAT
DEFINE_SPECIALIZED_METHOD(TrexTypes::SdfloatAttributeValue, ValueDict<TrexTypes::SdfloatAttributeValue>)
#endif
#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
bool SingleAttribute<ValueType, DictType>::isIndexed(bool onNewVersion /*=false */) const
{
    if (!onNewVersion)
        // DEV_ASSERT_0(itsIndexState != IIS_INDEXED || m_values.itsIndex.is_valid());
        return isIndexDefined() && m_values.itsIndex.get() != nullptr && m_values.itsIndex->isIndexed();
    else
        // DEV_ASSERT_0(itsNewIndexState != IIS_INDEXED || (m_newValues && m_newValues->itsIndex.is_valid()));
        return isIndexDefined() && m_newValues != nullptr && m_newValues->itsIndex.get() != nullptr && m_newValues->itsIndex->isIndexed();
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwGetDocids(
    const _STL::vector<int>& values, _STL::vector<int>& docids,
    const TRexUtils::BitVector* validDocids)
{
    return AttributeValueContainer::bwGetDocids(values, docids, validDocids);
}

#ifdef SINGLE_ATTRIBUTE_INT
template <>
ERRCODE SingleAttribute<TrexTypes::IntAttributeValue,
    ValueDict<TrexTypes::IntAttributeValue>>::bwGetDocids(
    const _STL::vector<int>& values, _STL::vector<int>& docids,
    const TRexUtils::BitVector* validDocids)
{
    if (validDocids != nullptr)
        return AttributeValueContainer::bwGetDocids(values, docids, validDocids);

    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));

    _STL::vector<int> valueIds;
    fnBwTranslateValues(m_values.itsDict, values, valueIds);

    return fnBwGetDocids(
        m_values.itsDocuments, m_values.itsDict, *m_values.itsIndex, m_values.itsDocumentCount, isIndexed(), valueIds,
        values, docids, validDocids);
}
#endif

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::jeReadValueIds(
    bool inputDocidsHaveOffsets,
    const TRexUtils::IndexVectorAligned* inputDocids1,
    const TRexUtils::BitVector* inputDocids2,
    const TRexUtils::BitVector* validDocids,
    TRexUtils::index_t docidOffset, TRexUtils::index_t nextDocidOffset,
    TRexUtils::index_t missingValueId,
    TRexUtils::IndexVectorAligned& vidResult,
    ListInfo& vidInfo,
    size_t resultPos,
    bool newVersion)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));

    const TRexUtils::IndexVector* docidsPtr;
    TRexUtils::index_t nullValueId;
    if (newVersion) {
        if (!m_newValues)
            return TREX_ERROR::AERC_NOT_IMPLEMENTED;
        docidsPtr = &(m_newValues->itsDocuments);
        nullValueId = m_newValues->itsDict.size();
    } else {
        docidsPtr = &(m_values.itsDocuments);
        nullValueId = m_values.itsDict.size();
    }

    return fnJeReadValueIds(*docidsPtr, nullValueId, inputDocidsHaveOffsets,
        inputDocids1, inputDocids2, validDocids, docidOffset,
        nextDocidOffset, missingValueId, vidResult, vidInfo, resultPos);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::jeReadIndex(
    const TRexUtils::IndexVectorAligned& valueIds,
    const TRexUtils::BitVector* validDocidsBv,
    const ltt::vector<int32_t>* validDocidsVec,
    bool shrinkedValidDocids,
    TRexUtils::index_t docidOffset,
    TRexUtils::IndexVectorAligned& docIdResult,
    TRexUtils::IndexVectorAligned& valueIdResult,
    QueryStats& queryStats)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));
    InvertedIndexInfoMustBeSetInThisScope invertedIndexInfoScope(queryStats);

    const TRexUtils::index_t nullValueId = m_values.countValues();
    const size_t numValuesInAttr = nullValueId;
    const size_t numDocsInAttr = m_values.countDocuments();
    const size_t numValueIds = valueIds.size();

    // make sure IVA is big enough
    docIdResult.max_index(m_values.getMaxDocid());
    valueIdResult.word_size(valueIds.word_size());

    TRexUtils::BitVector validDocidsTemp;
    const TRexUtils::BitVector* validDocids = validDocidsBv;
    if (validDocidsVec && !shrinkedValidDocids) {
        validDocidsToBitVec(*validDocidsVec, validDocidsTemp);
        validDocids = &validDocidsTemp;
    }

    // individual reads due to sparse distribution of requested value IDs
    if (validDocidsVec == nullptr && isIndexed() && (numValuesInAttr > numValueIds * 3)) {
        // the coding/decision is also used in Paged/SpAttributeDef.h -> please also adjust there if you optimize here
        size_t estSize = numDocsInAttr * numValueIds * 2 / numValuesInAttr;

        SWITCH_TO_NATIVE_IX2(fnJeReadIndexIndividual,
            (valueIds, m_values.itsIndex.get(), validDocids,
                docidOffset, numValueIds, docIdResult,
                valueIdResult, estSize, queryStats),
            docIdResult, valueIdResult);
    } else {
        // determine maxValueId:
        TRexUtils::index_t maxValueId = nullValueId - 1; //determineMaxValueId(valueIds, READ_INDEX_CHUNK_SIZE, numJobs);

        if (maxValueId < 0) { // nothing to do, no "good" valueid
            queryStats.setInvertedIndexAccess(InvertedIndexAccess::SKIPPED_EMPTY);
            return AERC_OK;
        }
        queryStats.setInvertedIndexAccess(isIndexDefined() ? InvertedIndexAccess::NO : InvertedIndexAccess::NOT_EXISTING);

        // convert input int vectors to bit vectors:
        TRexUtils::BitVector valueIdBits;
        valueIdBits.resize(maxValueId + 1, false);
        SWITCH_TO_NATIVE_IX(getValueIdBits,
            (valueIds, valueIdBits, maxValueId),
            valueIds);

        // estimate and reserve size:
        double avgNumDocsPerValue = ((double)numDocsInAttr) / numValuesInAttr;
        size_t estSize = (size_t)(avgNumDocsPerValue * 1.2 * numValueIds);

        if (validDocidsVec && shrinkedValidDocids) {
            SWITCH_TO_NATIVE_IX2(fnJeReadIndexShrinked,
                (validDocidsVec, maxValueId,
                    docidOffset, valueIdBits,
                    m_values.itsDocuments,
                    docIdResult, valueIdResult,
                    estSize),
                docIdResult, valueIdResult);
        } else {
            // chunked read
            SWITCH_TO_NATIVE_IX2(fnJeReadIndexChunked,
                (m_values.itsDocuments,
                    docidOffset, maxValueId,
                    validDocids, valueIdBits,
                    docIdResult, valueIdResult,
                    estSize),
                docIdResult, valueIdResult);
        }
    }
    return AERC_OK;
}

template <class ValueType, class DictType>
void SingleAttribute<ValueType, DictType>::fnJeSearchValueIdCounts_index(
    const TRexUtils::IndexVectorAligned* pValueIds,
    _STL::vector<int>* singleCounts,
    size_t& sumCounts,
    const TRexUtils::BitVector* validUDIVs)
{
    if (singleCounts && singleCounts->size() != pValueIds->size())
        singleCounts->resize(pValueIds->size(), 0);
    SingleIndexIterator iit(*m_values.itsIndex);
    SWITCH_TO_NATIVE_IX(fnJeSearchValueIdCounts_native1, (*pValueIds, iit, validUDIVs, singleCounts, sumCounts), (*pValueIds));
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getLastDocids(const TRexCommonObjects::ColumnBaseRef& values, _STL::vector<int>& result, int from, int to, TRexUtils::BitVector* invalidRows)
{
    AE_TEST_RET(lazyLoad());
    size_t end = values->size();
    DEV_ASSERT_0(result.size() == values->size());

    if (to > -1 && to < (int)end)
        end = to;
    size_t start = (from > 0) ? (size_t)from : 0;
    if (start >= end || this->empty())
        return AERC_OK;

    _STL::vector<int> docids;
    _STL::vector<int>::iterator res_iter = result.begin() + start;

    ERRCODE ret = AERC_OK;
    AttributeQuery query;
    QueryData queryData;
    QueryStats queryStats(QueryStats::NO_STATISTICS);
    query.setValue(QUERY_OP_EQ, "");
    _STL::string* temp = query.getValueVec().begin();

    for (size_t pos = start; pos < end && ret == AERC_OK; ++pos, ++res_iter) {
        if (invalidRows && invalidRows->test_unchecked(pos)) {
            *res_iter = 0;
            continue;
        }
        if (!values->get(pos, *temp)) {
            continue; //is null
        }
        docids.resize(0);
        queryData.reset();

        ret = _searchDocuments(query, docids, nullptr, queryData, queryStats);
        if (docids.size() > 0) {
            *res_iter = docids[0];
        }
    }
    return ret;
}

#define DEFINE_SPECIALIZED_METHOD(ValueType, DictType)                                            \
    template <>                                                                                   \
    ERRCODE SingleAttribute<ValueType, DictType>::getLastDocids(                                  \
        const TRexCommonObjects::ColumnBaseRef& values,                                           \
        _STL::vector<int>& result,                                                                \
        int from, int to,                                                                         \
        TRexUtils::BitVector* invalidRows)                                                        \
    {                                                                                             \
        if (values->size() == 0)                                                                  \
            return AERC_OK;                                                                       \
        AE_TEST_RET(lazyLoad());                                                                  \
        int valueId;                                                                              \
        _STL::string value;                                                                       \
        const char* p;                                                                            \
        uint32_t size;                                                                            \
        from = (from > -1) ? ltt::min(((uint32_t)from), values->size()) : 0;                      \
        to = (to > -1) ? ltt::min(((uint32_t)to), values->size()) : (int)values->size();          \
        DEV_ASSERT_0(result.size() == values->size());                                            \
        if (m_values.itsDocuments.size() == 0)                                                    \
            return AERC_OK;                                                                       \
        if (isIndexed()) {                                                                        \
            SingleIndexIterator iit(*m_values.itsIndex);                                          \
            for (size_t i = from; i < (unsigned)to; ++i) {                                        \
                if (invalidRows && invalidRows->test_unchecked(i)) {                              \
                    result[i] = 0;                                                                \
                    continue;                                                                     \
                }                                                                                 \
                p = values->getNative(i, size, value);                                            \
                if (!p)                                                                           \
                    continue; /* continue if NULL*/                                               \
                value.assign(p, size);                                                            \
                if (m_values.itsDict.findValue(value, valueId, true)) {                           \
                    if (iit.findValue(valueId)) {                                                 \
                        bool ok = iit.getFirstDocid(result[i]);                                   \
                        if (!ok)                                                                  \
                            result[i] = -1; /*reset if search was invalid*/                       \
                    }                                                                             \
                }                                                                                 \
            }                                                                                     \
        } else {                                                                                  \
            return AttributeValueContainer::getLastDocids(values, result, from, to, invalidRows); \
        }                                                                                         \
        return AERC_OK;                                                                           \
    }

#ifdef SINGLE_ATTRIBUTE_STRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::StringAttributeValue, ValueDict<TrexTypes::StringAttributeValue>)
#endif
#ifdef SINGLE_ATTRIBUTE_FIXEDSTRING
DEFINE_SPECIALIZED_METHOD(TrexTypes::FixedStringAttributeValue, ValueDict<TrexTypes::FixedStringAttributeValue>)
#endif
#undef DEFINE_SPECIALIZED_METHOD

//=====================================================================================================================

namespace {
namespace SingleGetValuesImpl {
struct AccessPolicies
{
    template <class InIter, class DictType>
    bool decompressDict(const InIter& in, const DictType& dict) const
    {
        const int dictSize = dict.size();
        return dictSize < 10000 && 0 < dictSize && static_cast<unsigned int>(dictSize * 128) < in.length();
    }
    template <class InIter>
    bool useDocidBuffer(const InIter& in) const
    {
        // Using mget buffer makes sense if docids are sorted and "dense", for now we use the old heuristic
        return in.length() >= 64;
        // Alternative heuristic would be:
        //   in.length() >= 64 && in.isSorted() && (in.maxDocidBound() - in.minDocidBound()) <= 8*in.length()
        // Note: DocidSequenceScanner is about 10% slower (the search only).
    }
};
}
} // End of anonymous namespace

template <class ValueType, class DictType>
void SingleAttribute<ValueType, DictType>::traceGetValues(
    const TRexCommonObjects::InputDocidWrapper& inwrap, OutputValueWrapper& outwrap, const GetValuesParams& params) const
{
    traceGetValuesGeneric(*this, "SingleAttribute", inwrap, outwrap);
    TRACE_GETVALS_INFO("docs size = " << m_values.itsDocuments.size() << ", dict size = " << m_values.itsDict.size());
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getValues(
    const TRexCommonObjects::InputDocidWrapper& inwrap, OutputValueWrapper& outwrap, const GetValuesParams& params)
{
    AE_TEST_RET(lazyLoadNoIndex());

    if (DOES_TRACE_GETVALS_INFO) {
        traceGetValues(inwrap, outwrap, params);
    }

    Synchronization::UncheckedMutexHandle lockHandle;
    if (params.useNewValuesGetValues) {
        lockHandle = m_writeLock.lockHandle();
        if (m_newValues == nullptr) {
            DEV_ASSERT_NOARG(false, "m_newValues must not be NULL.");
            TRC_ERROR(attributesTrace) << "SingleAttribute::getValues failed: no new values." << TrexTrace::endl;
            return TREX_ERROR::AERC_FAILED;
        }
    }

    TRexUtils::IndexVector& documents = (params.useNewValuesGetValues) ? m_newValues->itsDocuments : m_values.itsDocuments;
    DictType& dict = (params.useNewValuesGetValues) ? m_newValues->itsDict : m_values.itsDict;

    MainGetValuesImpl::AccessorGate<TRexUtils::IndexVector, DictType, SingleGetValuesImpl::AccessPolicies>
        ag(documents, dict, SingleGetValuesImpl::AccessPolicies());
    return GetValuesImpl::InOutDispatch(inwrap, outwrap, ag, *this);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::paGetDictAndVIDs(
    const _STL::vector<int>& rowPositions,
    TRexCommonObjects::AllocatedStringVector& valueDict,
    _STL::vector<int>& valueIds)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET_TRACE_AND_THROW(lazyLoad(handle));
    ERRCODE ret = fnBwGetDictAndDocs<ValueType, TRexUtils::IndexVector, DictType, TRexCommonObjects::AllocatedStringVector>(
        m_values.itsDict, m_values.itsDocuments, rowPositions, valueDict, valueIds);
    if (rowPositions.size() != valueIds.size()) {
        // temp tracing for bug #118881
        TRACE_ERROR(TRACE_AE_PARTITIONER, "SpAttribute::paGetDictAndVIDs rowPositions=" << rowPositions);
        TRACE_ERROR(TRACE_AE_PARTITIONER, "SpAttribute::paGetDictAndVIDs valueIds=" << valueIds);
        TRACE_ERROR(TRACE_AE_PARTITIONER, "SpAttribute::paGetDictAndVIDs valueDict=" << valueDict.size());
        TRACE_ERROR(TRACE_AE_PARTITIONER, "SpAttribute::paGetDictAndVIDs itsDocuments.size()=" << m_values.itsDocuments.size());
    }
    return ret;
}

#ifdef AE_DELTA_DICT
template <bool supportDeltaDict>
struct CopyDeltaDictUpdate
{
    template <class DictType>
    static DeltaDictUpdate* copy(const DictType& sourceDict, DictType& destDict)
    {
        return nullptr;
    }
};

template <>
struct CopyDeltaDictUpdate<true>
{
    template <class DictType>
    static DeltaDictUpdate* copy(const DictType& sourceDict, DictType& destDict)
    {
        if (&sourceDict == &destDict)
            return destDict.getDelta();
        if (sourceDict.getDelta() == NULL) {
            if (destDict.getDelta() != NULL)
                destDict.removeDelta();
            return nullptr;
        }
        if (destDict.getDelta() == NULL)
            destDict.createDelta();
        DeltaDictUpdate* update = destDict.getDelta();
        if (update == nullptr) {
            TRC_ERROR(attributesTrace)
                << "failed to create delta dict update" << TrexTrace::endl;
            return nullptr;
        }
        *update = *sourceDict.getDelta();
        if (update->mainRef.empty())
            update->mainRef.set(destDict.size());
        TRC_INFO(deltaDictTrace)
            << "copied delta dict update to new dict" << TrexTrace::endl;
        return update;
    }
};
#endif // AE_DELTA_DICT

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::mergeOldIntoNew(
    AttributeValueContainer* delta,
    const TRexUtils::BitVector* validDocids,
    const _STL::vector<int>& deltaDocids,
    CAN_MERGE mergeWith)
{
    if (m_newValues == nullptr) {
        m_newValues = new (getColumnStoreMainUncompressedAllocator()) SingleValues<ValueType, DictType>(m_definition, this);
        itsNewIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
        itsNewDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
    }
    switch (mergeWith) {
        case MERGE_WITH_SAME: {
            SingleAttribute<ValueType, DictType>& deltaAttribute = dynamic_cast<SingleAttribute<ValueType, DictType>&>(*delta);
            SingleMergePipeline<ValueType, DictType, PseudoDeltaAttribute<ValueType, DictType>> merger(m_definition, m_name, m_values.itsDict, m_values.itsDocuments, m_values.itsDocumentCount);
            PseudoDeltaAttribute<ValueType, DictType> pseudoDelta(deltaAttribute.m_values.itsDict, deltaAttribute.m_values.itsDocuments);
#ifdef AE_DELTA_DICT
            DeltaDictUpdate* deltaDictUpdate = CopyDeltaDictUpdate<DictType::supportDeltaDict>::copy(m_values.itsDict, m_newValues->itsDict);
#endif // AE_DELTA_DICT
            SingleMergeIndexVector newDoc(m_newValues->itsDocuments);
            ERRCODE ret = merger.merge(pseudoDelta, deltaDocids, validDocids,
                m_newValues->itsDict, newDoc, m_newValues->itsDocumentCount
#ifdef AE_DELTA_DICT
                ,
                deltaDictUpdate);
#else // AE_DELTA_DICT
                );
#endif // AE_DELTA_DICT
            m_newValues->setAttributeVersion(makeAttributeVersion());
            cacheMemoryInfo();
            return ret;
        }
        case MERGE_WITH_BTREE: {
            BTreeAttribute<ValueType>& deltaAttribute = dynamic_cast<BTreeAttribute<ValueType>&>(*delta);
            SingleMergePipeline<ValueType, DictType, BTreeAttribute<ValueType>> mergePipeline(m_definition, m_name, m_values.itsDict, m_values.itsDocuments, m_values.itsDocumentCount);
#ifdef AE_DELTA_DICT
            DeltaDictUpdate* deltaDictUpdate = CopyDeltaDictUpdate<DictType::supportDeltaDict>::copy(m_values.itsDict, m_newValues->itsDict);
#endif // AE_DELTA_DICT
            SingleMergeIndexVector newDoc(m_newValues->itsDocuments);
            ERRCODE ret = mergePipeline.merge(deltaAttribute, deltaDocids, validDocids,
                m_newValues->itsDict, newDoc, m_newValues->itsDocumentCount
#ifdef AE_DELTA_DICT
                ,
                deltaDictUpdate);
#else // AE_DELTA_DICT
                );
#endif // AE_DELTA_DICT
            m_newValues->setAttributeVersion(makeAttributeVersion());
            cacheMemoryInfo();
            return (ret);
        }
        default:
            return TREX_ERROR::AERC_NOT_IMPLEMENTED;
    }
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::mergeNewIntoNew(
    AttributeValueContainer* delta,
    const TRexUtils::BitVector* validDocids,
    const _STL::vector<int>& deltaDocids,
    CAN_MERGE mergeWith)
{
    if (m_newValues == nullptr)
        return TREX_ERROR::AERC_NO_UPDATED_VERSION;
    switch (mergeWith) {
        case MERGE_WITH_SAME: {
            SingleAttribute<ValueType, DictType>& deltaAttribute = dynamic_cast<SingleAttribute<ValueType, DictType>&>(*delta);
            SingleMergePipeline<ValueType, DictType, PseudoDeltaAttribute<ValueType, DictType>> merger(m_definition, m_name, m_newValues->itsDict, m_newValues->itsDocuments, m_newValues->itsDocumentCount);
            SingleValues<ValueType, DictType> updatedValues(m_definition, this);
            PseudoDeltaAttribute<ValueType, DictType> pseudoDelta(deltaAttribute.m_values.itsDict, deltaAttribute.m_values.itsDocuments);
#ifdef AE_DELTA_DICT
            DeltaDictUpdate* deltaDictUpdate = CopyDeltaDictUpdate<DictType::supportDeltaDict>::copy(m_newValues->itsDict, updatedValues.itsDict);
#endif // AE_DELTA_DICT
            SingleMergeIndexVector newDoc(updatedValues.itsDocuments);
            ERRCODE ret = merger.merge(pseudoDelta, deltaDocids, validDocids,
                updatedValues.itsDict, newDoc, updatedValues.itsDocumentCount
#ifdef AE_DELTA_DICT
                ,
                deltaDictUpdate);
#else // AE_DELTA_DICT
                );
#endif // AE_DELTA_DICT
            updatedValues.setAttributeVersion(makeAttributeVersion());
            m_newValues->swap(updatedValues);
            cacheMemoryInfo();
            return ret;
        }
        case MERGE_WITH_BTREE: {
            BTreeAttribute<ValueType>& deltaAttribute = dynamic_cast<BTreeAttribute<ValueType>&>(*delta);
            SingleMergePipeline<ValueType, DictType, BTreeAttribute<ValueType>> mergePipeline(m_definition, m_name, m_newValues->itsDict, m_newValues->itsDocuments, m_newValues->itsDocumentCount);
            SingleValues<ValueType, DictType> updatedValues(m_definition, this);
#ifdef AE_DELTA_DICT
            DeltaDictUpdate* deltaDictUpdate = CopyDeltaDictUpdate<DictType::supportDeltaDict>::copy(m_newValues->itsDict, updatedValues.itsDict);
#endif // AE_DELTA_DICT
            SingleMergeIndexVector newDoc(updatedValues.itsDocuments);
            ERRCODE ret = mergePipeline.merge(deltaAttribute, deltaDocids, validDocids,
                updatedValues.itsDict, newDoc, updatedValues.itsDocumentCount
#ifdef AE_DELTA_DICT
                ,
                deltaDictUpdate);
#else // AE_DELTA_DICT
                );
#endif // AE_DELTA_DICT
            updatedValues.setAttributeVersion(makeAttributeVersion());
            m_newValues->swap(updatedValues);
            cacheMemoryInfo();
            return ret;
        }
        default:
            return TREX_ERROR::AERC_NOT_IMPLEMENTED;
    }
}

template <class ValueType, class DictType>
bool SingleAttribute<ValueType, DictType>::hasBtreeDefinition(
    AttributeValueContainer* other) const
{
    if (other != nullptr
        && ((other->getDefinition().getAttributeFlags() & TRexEnums::ATTRIBUTE_FLAG_BTREE)
               != 0))
        return true;
    return false;
}

template <class ValueType, class DictType>
CAN_MERGE SingleAttribute<ValueType, DictType>::canMergeWith(
    AttributeValueContainer* other) const
{
    if (hasSameDefinition(other))
        return MERGE_WITH_SAME;
    if (hasBtreeDefinition(other))
        return MERGE_WITH_BTREE;
    return CAN_NOT_MERGE;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwSplitPartitions(
    const TRexUtils::BitVector* validDocids,
    int partCount,
    _STL::vector<int>& partitionStart,
    _STL::vector<int>& partitionedDocids)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));

    return fnBwSplitPartitions(m_definition, m_values.itsDict, m_values.itsDocuments, validDocids, partCount, partitionStart, partitionedDocids);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::renumberOldIntoNew(const ltt::vector<int>& oldDocids, bool mappingIsBijective)
{
    SinglePartitioner<DictType> partitioner(
        m_values.itsDict, m_values.itsDocuments);
    if (m_newValues == nullptr) {
        m_newValues = new (getColumnStoreMainUncompressedAllocator()) SingleValues<ValueType, DictType>(m_definition, this);
        itsNewIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
        itsNewDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
    }
    ERRCODE ret = AERC_OK;
    if (mappingIsBijective) {
        ret = partitioner.renumberDocsBijective(oldDocids, m_newValues->itsDocuments);
        m_newValues->itsDict = m_values.itsDict;
        m_newValues->itsDocumentCount = m_values.itsDocumentCount;
    } else {
        ret = partitioner.renumberDocs(oldDocids, m_newValues->itsDict, m_newValues->itsDocuments, m_newValues->itsDocumentCount);
    }
    cacheMemoryInfo();
    return ret;
}

class LtValue42
{
public:
    bool operator()(const DocidValueId& t1, const DocidValueId& t2) const
    {
        return t1.valueId < t2.valueId;
    }
};

#ifdef SINGLE_ATTRIBUTE_INT
template <>
ERRCODE SingleAttribute<TrexTypes::IntAttributeValue, ValueDict<TrexTypes::IntAttributeValue>>::setIntAttribute(
    const int* values, size_t count, bool writeToLog /*=false*/)
{
    Synchronization::UncheckedMutexScope guard1(m_writeLock);
    Synchronization::UncheckedExclusiveScope guard2(m_readLock);
    itsIndexState = isIndexDefined() ? IIS_NOT_INDEXED : IIS_NOT_DEFINED;
    itsDataStatsState = isDataStatsDefined() ? DSS_NO_DATA_STATS : DSS_NOT_DEFINED;
    // XXX todo: run this code only if in a memory index,
    // acquire the lock while doing so
    _STL::vector<DocidValueId> pairs;
    pairs.raw_resize(count);
    size_t i = 0;
    while (i != count) {
        DocidValueId& pair = pairs[i];
        pair.valueId = values[i];
        pair.docid = ++i;
    }
    _STL::sort(pairs.begin(), pairs.end(), LtValue42());
    _STL::vector<TrexTypes::IntAttributeValue> dictVector;
    dictVector.reserve(count);
    _STL::vector<int> docidVector;
    docidVector.raw_resize(count + 1);
    int lastValue = -1, valueId = -1;
    if (!pairs.empty())
        lastValue = pairs.front().valueId - 1;
    _STL::vector<DocidValueId>::const_iterator it, pairsEnd = pairs.end();
    for (it = pairs.begin(); it != pairsEnd; ++it) {
        if ((*it).valueId != lastValue) {
            ++valueId;
            lastValue = (*it).valueId;
            dictVector.emplace_back(lastValue);
        }
        docidVector[(*it).docid] = valueId;
    }
    m_values.itsDict.setVector_(dictVector);
    m_values.itsDocuments.release();
    m_values.itsDocuments.setRange(m_values.itsDict.size() + 1);
    m_values.itsDocuments.resize(count + 1);
    docidVector[0] = m_values.itsDict.size();
    m_values.itsDocuments.mset(0, docidVector.size(), (const unsigned*)docidVector.begin());
    m_values.itsDocumentCount = count;
    //int64_t t0 = pcounter();
    oldHasChanged(writeToLog);
    //int64_t dt = pdiff_muesec(t0, pcounter());
    TRACE_INFO(TRACE_CREATE_INDEX,
        "SingleAttribute::setIntAttribute() checks index creation on attribute "
            << getQualifiedAttributeId()
            << ", itsIndexState=" << AeSymbols::indexStateString(itsIndexState)
            << ", itsNewIndexState=" << AeSymbols::indexStateString(itsNewIndexState));
    ERRCODE ret = checkIndexCreation();
    if (ret == AERC_OK && !isIndexDefined())
        ret = calculateTopDocumentCounts();
    cacheMemoryInfo();
    return ret;
}
#endif

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::setIntAttribute(const int* values, size_t count, bool writeToLog /*=false*/)
{
    return AttributeValueContainer::setIntAttribute(values, count, writeToLog);
}

template <class ValueType, class DictType>
void SingleAttribute<ValueType, DictType>::checkOld(ContainmentCheck& c) const { m_values.check(c); }

template <class ValueType, class DictType>
void SingleAttribute<ValueType, DictType>::checkNew(ContainmentCheck& c) const
{
    if (m_newValues != nullptr)
        m_newValues->check(c);
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::compare1(AttributeValueContainer* other, Synchronization::SharedHandle& handle1, Synchronization::SharedHandle& handle2, ltt::vector<int>& transform1, ltt::vector<int>& transform2)
{
    // check if attributes are of same type:
    const AttributeDefinition& otherDefinition = other->getDefinition();
    if (m_definition.getAttributeType() != otherDefinition.getAttributeType() || m_definition.getAttributeFlags() != otherDefinition.getAttributeFlags() || m_definition.getIntDigits() != otherDefinition.getIntDigits() || m_definition.getFractDigits() != otherDefinition.getFractDigits())
        return TREX_ERROR::AERC_NOT_IMPLEMENTED;
    // cast pointer to other attribute to our type:
    SingleAttribute<ValueType, DictType>* p2 = (SingleAttribute<ValueType, DictType>*)other;
    // load and lock both attributes:
    if (m_name <= p2->m_name) {
        handle1 = m_readLock.getSharedLock().lockHandle();
        AE_TEST_RET(lazyLoad(handle1));
        if (p2 != this) {
            handle2 = p2->m_readLock.getSharedLock().lockHandle();
            AE_TEST_RET(p2->lazyLoad(handle2));
        }
    } else {
        handle2 = p2->m_readLock.getSharedLock().lockHandle();
        AE_TEST_RET(p2->lazyLoad(handle2));
        handle1 = m_readLock.getSharedLock().lockHandle();
        AE_TEST_RET(lazyLoad(handle1));
    }
    // merge dictionaries:
    typename DictType::iterator dictIt1, dictIt2;
    int dictSize1 = m_values.itsDict.size(), dictSize2 = p2->m_values.itsDict.size();
    int i1 = 0, i2 = 0;
    const ValueType *v1 = NULL, *v2 = NULL;
    if (dictSize1)
        v1 = &m_values.itsDict.get(i1, dictIt1);
    if (dictSize2)
        v2 = &p2->m_values.itsDict.get(i2, dictIt2);
    TrexTypes::LtValue<ValueType> lt(m_definition);
    int newid = 0;
    transform1.raw_resize(dictSize1 + 1);
    transform2.raw_resize(dictSize2 + 1);
    while (i1 < dictSize1 && i2 < dictSize2) {
        if (lt(*v1, *v2)) {
            transform1[i1] = newid;
            if (++i1 < dictSize1)
                v1 = &m_values.itsDict.get(i1, dictIt1);
        } else if (lt(*v2, *v1)) {
            transform2[i2] = newid;
            if (++i2 < dictSize2)
                v2 = &p2->m_values.itsDict.get(i2, dictIt2);
        } else {
            transform1[i1] = transform2[i2] = newid;
            if (++i1 < dictSize1)
                v1 = &m_values.itsDict.get(i1, dictIt1);
            if (++i2 < dictSize2)
                v2 = &p2->m_values.itsDict.get(i2, dictIt2);
        }
        ++newid;
    }
    while (i1 < dictSize1) {
        transform1[i1] = newid;
        ++i1, ++newid;
    }
    while (i2 < dictSize2) {
        transform2[i2] = newid;
        ++i2, ++newid;
    }
    transform1[i1] = transform2[i2] = newid; // null
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::setSparseWriteFlags(int flags)
{
    m_sparseWriteFlags = flags;
    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::checkNewForSparsity(bool& isSparse)
{
    isSparse = false;

    if (m_newValues == nullptr)
        return AERC_OK;

    ERRCODE rc = m_newValues->calculateTopDocumentCounts(m_attributeStore.getIndexId(), m_name);
    if (rc != 0)
        return rc;
    const _STL::vector<ValueIdCount>& topDocumentCounts = m_newValues->itsTopDocumentCounts;

    if (topDocumentCounts.empty()) {
        cacheMemoryInfo();
        return AERC_OK;
    }
    size_t numDocuments = m_newValues->itsDocumentCount;
    float topDocumentRatio = (float)topDocumentCounts[0].documentCount / (float)numDocuments;
    float threshold = AggregateConfig::getInstance().fullToSparseThreshold;
    isSparse = (topDocumentRatio > threshold);

    cacheMemoryInfo();

    return AERC_OK;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::calculateTopDocumentCounts(bool onNewVersion)
{
    ERRCODE rc = AERC_OK;
    if (onNewVersion && m_newValues != nullptr)
        rc = m_newValues->calculateTopDocumentCounts(m_attributeStore.getIndexId(), m_name);
    else if (!onNewVersion)
        rc = m_values.calculateTopDocumentCounts(m_attributeStore.getIndexId(), m_name);
    cacheMemoryInfo();
    return rc;
}

#define DEFINE_SPECIALIZED_METHOD(DictType)                                                                                                                                                               \
    template <>                                                                                                                                                                                           \
    ERRCODE SingleAttribute<TrexTypes::IntAttributeValue, DictType>::bwFilterValues(const _STL::vector<int>& values, bool isSorted, const TRexUtils::BitVector* validDocids, TRexUtils::BitVector& found) \
    {                                                                                                                                                                                                     \
        Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());                                                                                                                    \
        AE_TEST_RET(lazyLoad(handle));                                                                                                                                                                    \
        TRexUtils::BitVector usedValueIds;                                                                                                                                                                \
        if (validDocids != NULL) {                                                                                                                                                                        \
            const size_t dictSize = m_values.itsDict.size();                                                                                                                                              \
            fnBwGetUsedValueIds(m_values.itsDocuments, *validDocids, dictSize, usedValueIds);                                                                                                             \
            TRC_INFO(euclidTrace) << "bwFilterValues " << usedValueIds.numSet() << " of " << dictSize << " values" << TrexTrace::endl;                                                                    \
        }                                                                                                                                                                                                 \
        return fnBwFilterValues<>(m_values.itsDict, usedValueIds, values, isSorted, validDocids, found);                                                                                                  \
    }
#ifdef SINGLE_ATTRIBUTE_INT
DEFINE_SPECIALIZED_METHOD(ValueDict<TrexTypes::IntAttributeValue>)
#endif
#undef DEFINE_SPECIALIZED_METHOD

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::bwFilterValues(const _STL::vector<int>& values, bool isSorted, const TRexUtils::BitVector* validDocids, TRexUtils::BitVector& found)
{
    return TREX_ERROR::AERC_NOT_IMPLEMENTED;
}

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::geScanIndex(
    const TRexUtils::BitVector& valueIDs,
    TRexUtils::BitVector& docIDs,
    ltt::allocator& alloc)
{
    Synchronization::SharedHandle handle(m_readLock.getSharedLock().lockHandle());
    AE_TEST_RET(lazyLoad(handle));

    const unsigned int numValuesInAttr = m_values.countValues();
    const unsigned int numberOfDocuments = m_values.countDocuments();

    bool useIndex = false;
    unsigned int expectedResult = static_cast<unsigned int>(
        numberOfDocuments / static_cast<float>(numValuesInAttr));
    //TODO: avoid using numSet (expensive)
    expectedResult *= valueIDs.numSet();
    unsigned int threshold = static_cast<unsigned int>(
        0.001f * static_cast<float>(numberOfDocuments));

    TRACE_INFO(TRACE_SINGLEATTR, "Expected Result set size: "
            << expectedResult);

    if ((expectedResult <= threshold)) {
        useIndex = true;
    }

    if (isIndexed() && useIndex) {
        TRACE_INFO(TRACE_SINGLEATTR, "Using inverted index.");
        SingleIndexIterator iit(*m_values.itsIndex);
        TRexUtils::BitVector::const_iterator it = valueIDs.begin();
        TRexUtils::BitVector::const_iterator itEnd = valueIDs.end();
        DocumentId docId;
        for (; it != itEnd; ++it) {
            if (iit.findValue(*it)) {
                if (iit.getFirstDocid(docId)) {
                    docIDs.set_unchecked(docId);
                    while (iit.getNextDocid(docId)) {
                        docIDs.set_unchecked(docId);
                    }
                }
            }
        }
    } else {
        unsigned int numJobs = min(static_cast<unsigned int>(Execution::JobExecutor::getInstance().concurrencyHint()),
            numberOfDocuments / PARALLEL_MGETSEARCH_CHUNKSIZE_FOR_SINGLE_AS_PARAMETER + 1);
        if (TRexUtils::Parallel::mode == TRexUtils::Parallel::Off || TRexUtils::Parallel::Context::isJob() || numJobs == 0) {
            numJobs = 1;
        }
        if (numJobs == 1) {
            // single-threaded (at least at this level)
            TRACE_INFO(TRACE_SINGLEATTR, "Using sequential mgetSearch.");
            // the first docId is the dictionary size (not used)
            m_values.itsDocuments.mgetSearch(1, m_values.itsDocuments.size() - 1, valueIDs, docIDs);
        } else {
            // parallel scan
            TRACE_INFO(TRACE_SINGLEATTR, "Using parallel mgetSearch.");
            unsigned int chunkSize = (numberOfDocuments / numJobs) + 1;
            unsigned int align = 8 * sizeof(TRexUtils::BitVector::ElemType);
            chunkSize = ((((long)chunkSize) + align - 1) & ~(align - 1));

            TRACE_DEBUG(TRACE_SINGLEATTR, "Starting scan index with " << numJobs
                                                                      << " jobs and chunksize " << chunkSize);

            TRexUtils::Parallel::Context context;
            unsigned int startDocid = 0;
            ltt::vector<ltt::smart_ptr<TRexUtils::BitVector>> bvs(alloc);
            unsigned int i = 0;
            while (startDocid < numberOfDocuments) {
                if ((startDocid + chunkSize) >= numberOfDocuments) {
                    chunkSize = numberOfDocuments - startDocid;
                }
                // the first docId is the dictionary size (not used)
                if (chunkSize == 0 || (startDocid == 0 && chunkSize == 1)) {
                    break;
                }
                ltt::smart_ptr<TRexUtils::BitVector> bv;
                new (bv, alloc) TRexUtils::BitVector(numberOfDocuments + 1, false, alloc);
                bvs.push_back(bv);
                TRACE_DEBUG(TRACE_SINGLEATTR, "Execute mgetSearch() for " << startDocid << ","
                                                                          << startDocid + chunkSize);
                context.pushJob(new GEJobScanIndex(
                    m_values.itsDocuments,
                    (startDocid == 0) ? 1 : startDocid,
                    (startDocid == 0) ? chunkSize - 1 : chunkSize,
                    valueIDs,
                    bvs[i]));
                startDocid += chunkSize;
                ++i;
            }

            if (!context.run()) {
                return context.getErrors().front().getCode();
            }
            // merging partial results
            for (unsigned int j = 0; j < bvs.size(); ++j) {
                if (bvs[j]->anySet()) {
                    docIDs.opOr(*bvs[j]);
                }
            }
        }
    }

    return AERC_OK;
}

#ifdef AE_DELTA_DICT
template <class ValueType, class DictType>
bool SingleAttribute<ValueType, DictType>::dictSupportsDelta() const
{
    return DictType::supportDeltaDict;
}

template <class ValueType, class DictType>
bool SingleAttribute<ValueType, DictType>::getSupportDeltaDict() const
{
    return m_values.itsDict.supportDeltaDict;
}
#endif // AE_DELTA_DICT

template <class ValueType, class DictType>
ERRCODE SingleAttribute<ValueType, DictType>::getLobContainerIds(ltt::vector<PersistenceLayer::GlobalContainerId2>& gids,
    const TRexUtils::BitVector& docids,
    PersistenceLayer::VolumeId volumeId,
    LobContainerType lctype)
{
    return AE_TRACE_NOT_IMPLEMENTED("getLobContainerIds");
}

#ifdef SINGLE_ATTRIBUTE_LOB
template <>
ERRCODE SingleAttribute<TrexTypes::LobAttributeValue, ValueDict<TrexTypes::LobAttributeValue>>::getLobContainerIds(
    ltt::vector<PersistenceLayer::GlobalContainerId2>& gids,
    const TRexUtils::BitVector& docids,
    PersistenceLayer::VolumeId volumeId,
    LobContainerType lctype)
{
    if (!docids.anySet())
        return AERC_OK;

    AE_TEST_RET(lazyLoad()); // Bug #78831, call from LOB GC gets empty gids

    unsigned int nullValueId = m_values.itsDict.size();
    TRexUtils::BitVector visibleValueIds(nullValueId, false);
    for (TRexUtils::BitVector::iterator it = docids.beginAt_unchecked(0, false); !it.isEnd() && *it < m_values.itsDocuments.size(); ++it) {
        const unsigned int valueId = m_values.itsDocuments.get(*it);
        if (valueId < nullValueId)
            visibleValueIds.set_unchecked(valueId);
    }

    m_values.itsDict.getLobContainerIdsForValueIds(visibleValueIds, volumeId, lctype, gids);

    return AERC_OK;
}
#endif

// --------------------
// class SingleValues:

template <class ValueType, class DictType>
SingleValues<ValueType, DictType>::SingleValues(
    const AttributeDefinition& definition, const AttributeValueContainer* ownerAvc)
    : itsDefinition(definition)
    , itsDict(definition, ownerAvc)
    , itsDocumentCount(0)
    , itsDocuments(TRexUtils::AG_SINGLE)
    , itsIndex()
    , itsTopDocumentCounts(getColumnStoreMainUncompressedAllocator())
    , alreadyContainsUpdates(false)
{
    if (definition.getAttributeFlags() & TRexEnums::ATTRIBUTE_FLAG_INDEX) {
        itsIndex = ltt::make_unique<SingleIndex>(getColumnStoreMainIndexSingleAllocator(), getColumnStoreMainIndexSingleAllocator());
    }
}

template <class ValueType, class DictType>
void SingleValues<ValueType, DictType>::clear()
{
    itsDocuments.release();
    itsDict.clear();
    itsTopDocumentCounts.clear();
    if (itsIndex)
        itsIndex->clear();
    itsDocumentCount = 0;
    itsDataStats.clear();
}

template <class ValueType, class DictType>
bool SingleValues<ValueType, DictType>::buildIndex()
{
    // DEV_ASSERT_0(itsIndex.get() != NULL);
    if (itsIndex.get() != nullptr) { // TODO this can be asserted
        MemoryManager::ImplicitStatementMemoryBooking impli(false);
        bool isCorrect = false;
        itsIndex = SingleIndexFactory::newIndex(itsDocuments, itsDocumentCount, itsDict.size(), itsTopDocumentCounts, isCorrect, getColumnStoreMainIndexSingleAllocator(), false /* not from paged attribute */);
        return isCorrect;
    }
    return true;
}

template <class ValueType, class DictType>
ERRCODE SingleValues<ValueType, DictType>::getValueCounts(_STL::vector<unsigned>& valueIdFreq) const
{
    TRexUtils::index_t i, j;
    TRexUtils::index_t mgetSize;
    TRexUtils::index_t docId = 1;
    TRexUtils::index_t docSize = itsDocuments.size();
    int countDistinctValues = countValues();
    const TRexUtils::index_t arraySize = 1024;
    TRexUtils::MgetStackBuffer<arraySize> resArray;

    valueIdFreq.resize(countDistinctValues + 1);

    memset(&valueIdFreq[0], 0, (countDistinctValues + 1) * sizeof(unsigned));
    while (docId < docSize) {
        mgetSize = docSize - docId;
        if (mgetSize > arraySize)
            mgetSize = arraySize;
        itsDocuments.mget(docId, mgetSize, resArray);
        for (i = 0; i < mgetSize; ++i) {
            if (ltt_unlikely(int_cast<int>(resArray[i]) > countDistinctValues)) {
                TRACE_ERROR(TRACE_ATTR, "SingleValues::getValueCounts: invalid value found");
                return TREX_ERROR::AERC_FAILED;
            }
            ++valueIdFreq[resArray[i]];
        }
        docId += mgetSize;
    }

    if (TRACE_DATASTATS_AE.isTraceActive(Diagnose::Trace_Debug)) {
        Diagnose::TraceStream thisTraceStream(TRACE_DATASTATS_AE, Diagnose::Trace_Debug, __FILE__, __LINE__);

        j = 1;
        if (countDistinctValues >= 20) {
            j = countDistinctValues / 10;
        }

        for (i = 0; i < countDistinctValues; i += j) {
            thisTraceStream << " " << i << ",  " << valueIdFreq[i] << ";";
        }

        thisTraceStream << " " << countDistinctValues << ",  " << valueIdFreq[countDistinctValues] << ";";
    }

    return AERC_OK;
}

template <class ValueType, class DictType>
bool SingleValues<ValueType, DictType>::setDataStatistics(const DataStatistics::ValueIdStatisticsHandle& dataStats)
{
    DEV_ASSERT_0(dataStats.is_valid());
    itsDataStats.set(dataStats);
    TRACE_DEBUG(TRACE_DATASTATS_AE, "SingleValues::setDataStatistics() success");
    DTEST_CHECK_BREAKPOINT("DTEST_AE_SINGLEVALUE_SET_DATA_STATISTICS", "");
    return true;
}

template <class ValueType, class DictType>
void SingleValues<ValueType, DictType>::getMemoryInfo(MemoryInfo& info, bool grossSize, bool includeIndex) const
{
    info.addDataSize(grossSize ? itsDocuments.getAllocatedMemorySize() : itsDocuments.getMemorySize());
    itsDict.getMemoryInfo(info, grossSize);
    if (itsIndex.get() != nullptr && includeIndex)
        itsIndex->getMemoryInfo(info, grossSize);
    info.addMiscSize(grossSize ? (itsTopDocumentCounts.capacity() * sizeof(ValueIdCount))
                               : (itsTopDocumentCounts.size() * sizeof(ValueIdCount)));
    info.addMiscSize(itsDataStats.getMemorySize(grossSize));
}

template <class ValueType, class DictType>
ltt::ostream& SingleValues<ValueType, DictType>::toStream(ltt::ostream& stream) const
{
    // TODO add dictionary
    stream << "[SingleValues: ";
    stream << itsDocuments.toStream(stream);
    if (itsIndex.get() != nullptr && itsIndex->isIndexed())
        stream << itsIndex->toStream(stream);
    stream << "]";
    return stream;
}

template <class ValueType, class DictType>
size_t SingleValues<ValueType, DictType>::getSizeOfMemoryIntersection(void* corruptRangeBegin, void* corruptRangeBeyond) const throw()
{
    // TODO add dictionary
    size_t range = 0;
    // FIXME: more data members that could be protected? Need to be added to toStream as well then.
    range += itsDocuments.getSizeOfMemoryIntersection(corruptRangeBegin, corruptRangeBeyond);
    if (itsIndex.get() != nullptr && itsIndex->isIndexed())
        range += itsIndex->getSizeOfMemoryIntersection(corruptRangeBegin, corruptRangeBeyond);
    return range;
}

template <class ValueType, class DictType>
void SingleValues<ValueType, DictType>::getPageNumaLocationMap(TRexUtils::NumaLocationMap& pageMap) const
{
    MemoryManager::TransientMetadataNUMAInfo tmpVecPageMap(pageMap.getAllocator());
    itsDocuments.getPageNumaLocationMap(tmpVecPageMap);
    pageMap.addDataPageMap(tmpVecPageMap);

    tmpVecPageMap.resetNumaNodeInfo();
    itsDict.getPageNumaLocationMap(tmpVecPageMap);
    pageMap.addDictPageMap(tmpVecPageMap);

    if (itsIndex.get() != nullptr) {
        itsIndex->getPageNumaLocationMap(pageMap);
    }

    ltt::pair<void*, size_t> vectorRegion = TRexUtils::NumaLocationMap::getVectorRegion(itsTopDocumentCounts);
    pageMap.addRegionToMiscPageMap(vectorRegion);
}

template <class ValueType, class DictType>
ERRCODE SingleValues<ValueType, DictType>::calculateTopDocumentCounts(const TrexBase::IndexName& indexId, const _STL::string& attributeId)
{
    // TODO: use SingleValues::getValueCounts instead of implementing the counting here again
    const size_t bufferSize = 4096;
    const size_t docsSize = itsDocuments.size();
    TRexUtils::MgetStackBuffer<bufferSize> buffer;
    size_t mgetSize = bufferSize;
    ltt::vector<unsigned> counts(getColumnStoreTransientAllocator());
    const int dictSize = itsDict.size();
    counts.resize(dictSize + 1, 0);
    unsigned failedValue = 0;
    for (size_t pos = 0; pos != docsSize; pos += mgetSize) {
        if (pos + mgetSize > docsSize) {
            mgetSize = docsSize - pos;
        }
        itsDocuments.mget(pos, mgetSize, buffer);
        for (size_t i = 0; i != mgetSize; ++i) {
            if (ltt_unlikely(buffer[i] > static_cast<unsigned>(dictSize))) {
                if (buffer[i] > failedValue) {
                    failedValue = buffer[i];
                }
            } else {
                ++counts[buffer[i]];
            }
        }
    }

    // Temporary checking for Bugs 116659, 120613, 163020, 163133 to identify the
    // root cause of these bugs.
    if (failedValue > 0) {
        // data vector check failed
        TRACE_ERROR(TRACE_ATTR, "Inconsistent column " << indexId << "." << attributeId
                << ": found value id " << failedValue
                << " where dictSize is " << dictSize);
        InvalidValueCountsTracer::trace(counts, dictSize, itsDocuments, indexId, attributeId);
        return TREX_ERROR::AERC_LOAD_FAILED;
    }

    counts.resize(dictSize); // remove valueId for docid 0

    // Temporary checking for Bugs 116659, 120613, 163020, 163133 to identify the
    // root cause of these bugs.
    for (int i = 0; i < dictSize; ++i) {
        if (counts[i] == 0) {
            // counts vector check failed
            TRACE_ERROR(TRACE_ATTR, "Inconsistent valueCounts on column " << indexId << "." << attributeId);
            InvalidValueCountsTracer::trace(counts, dictSize, itsDocuments, indexId, attributeId);
            return TREX_ERROR::AERC_LOAD_FAILED;
        }
    }

    const int k = ltt::min(dictSize, TOP_DOCUMENT_COUNT);
    itsTopDocumentCounts.clear();
    itsTopDocumentCounts.reserve(k);

    LtValueIdCount cmp;
    ValueIdCount count;
    for (count.valueId = 0; count.valueId < k; ++count.valueId) {
        count.documentCount = counts[count.valueId];
        DEV_ASSERT_0(count.documentCount > 0); // Bug 116659 and 120613
        itsTopDocumentCounts.push_back(count);
    }
    TRexUtils::SortHeap<ValueIdCount, LtValueIdCount> heap(itsTopDocumentCounts, cmp);
    heap.heapify();
    for (; count.valueId != dictSize; ++count.valueId) {
        count.documentCount = counts[count.valueId];
        DEV_ASSERT_0(count.documentCount > 0); // Bug 116659 and 120613
        heap.addValue(count);
    }

    _STL::sort(itsTopDocumentCounts.begin(), itsTopDocumentCounts.end(), LtValueIdCountByValue());

    return AERC_OK;
}

template <class ValueType, class DictType>
void SingleValues<ValueType, DictType>::swap(SingleValues<ValueType, DictType>& other)
{
    AttributeVersionOwner::swap(other);
    _STL::swap(itsDocumentCount, other.itsDocumentCount);
    itsDict.swap(other.itsDict);
    itsDocuments.swap(other.itsDocuments);
    itsDefinition.swap(other.itsDefinition);
    _STL::swap(itsIndex, other.itsIndex);
    _STL::swap(itsTopDocumentCounts, other.itsTopDocumentCounts);
    itsDataStats.swap(other.itsDataStats);
}

template <class ValueType, class DictType>
void SingleValues<ValueType, DictType>::check(ContainmentCheck& c) const
{
    itsDict.check(c);
    c.check(itsDocuments, "documents", __FILE__, __LINE__);
}

} // namespace AttributeEngine
