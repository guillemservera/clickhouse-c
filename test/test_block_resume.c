/*
 * test_block_resume.c -- level-3 column resumption: chc__block_resume_in keeps
 * a partial block alive across CHC_WOULD_BLOCK, re-parsing only the in-progress
 * column. No server needed.
 *
 * Same fixture shape as test_ioless.c (wide UInt32+String, Nullable(String)+
 * Array(UInt32), LowCardinality(String)), decoded two ways and compared:
 *   - oracle:  io-backed chc_in over a memory source (whole stream at once).
 *   - subject: ioless chc_in driven by chc__block_resume_in with a persisted
 *              partial block + next_col across would-blocks, fed in adversarial
 *              chunkings. On would-block the driver does NOT free the partial
 *              and does NOT rewind (resume owns that): it only submits the next
 *              chunk and re-calls with the SAME partial/next_col.
 * The subject must reconstruct a byte-identical block sequence and an identical
 * `consumed` total, and must demonstrably retain a partial block mid-column
 * rather than restart from the header each chunk. Run under ASan/valgrind to
 * catch partial frees missed on a would-block rewind.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#define CHC_NO_ZSTD
#include "clickhouse.h"

static int        fail_count = 0;
static const char *current_test = "";

#include "test_common.h"
#include "test_block_compare.h"
#include "test_golden_blocks.h"

/* Counters proving retention: how often resume returned WOULD_BLOCK with a
 * partial block whose next_col was already > 0 (i.e. some columns retained),
 * vs returns where next_col was still 0 (header re-/parse, nothing retained). */
typedef struct {
    int wb_retained_midcol;   /* WOULD_BLOCK with partial != NULL && next_col > 0 */
    int wb_header;            /* WOULD_BLOCK with partial == NULL (mid-header) */
    size_t max_next_col;      /* highest next_col observed on a WOULD_BLOCK */
} resume_stats;

/* Resumption driver: persist a partial block + next_col across WOULD_BLOCK.
 * On would-block do NOT free partial and do NOT rewind; resume owns the in
 * checkpoint/rewind. Just submit the next chunk and re-call with the SAME
 * partial/next_col. chc_in_reset at each completed packet (compaction). */
static int
decode_resume(const uint8_t *bytes, size_t len, size_t chunk, const chc_alloc *al,
              const chc_block_opts *opts, chc_block **out, uint64_t *consumed,
              resume_stats *st, chc_err *err)
{
    chc_in in;
    if (chc_in_init_ioless(&in, al)) return -1;
    size_t fed = 0;
    for (size_t bi = 0; bi < TEST_GOLDEN_BLOCKS; bi++) {
        chc_block *partial = NULL;
        size_t next_col = 0;
        for (;;) {
            chc_err e = {};
            int rc = chc__block_resume_in(&in, al, opts, &partial, &next_col, &e);
            if (rc == CHC_OK && partial) break;
            if (rc == CHC_WOULD_BLOCK) {
                if (st) {
                    if (partial && next_col > 0) st->wb_retained_midcol++;
                    else                          st->wb_header++;
                    if (next_col > st->max_next_col) st->max_next_col = next_col;
                }
                if (fed >= len) {                 /* stream exhausted mid-block */
                    chc_block_destroy(partial, al);
                    chc__err_set(err, CHC_ERR_EOF, "feed underrun");
                    chc_in_free(&in);
                    return -1;
                }
                size_t take = (len - fed) < chunk ? (len - fed) : chunk;
                if (chc_in_submit(&in, bytes + fed, take, &e)) {
                    chc_block_destroy(partial, al);
                    *err = e; chc_in_free(&in); return -1;
                }
                fed += take;
                continue;
            }
            chc_block_destroy(partial, al);       /* real error or unexpected NULL */
            *err = e;
            chc_in_free(&in);
            return -1;
        }
        out[bi] = partial;
        chc_in_reset(&in);                         /* drop the just-parsed packet */
    }
    *consumed = in.consumed;
    chc_in_free(&in);
    return 0;
}

/* ---------------- tests -------------------------------------------------- */

static void
test_resume_golden(void)
{
    current_test = "resume_golden";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_block_opts opts = { .has_block_info = true, .has_custom_serialization = true };

    size_t len = 0;
    uint8_t *stream = test_build_golden_stream(&al, &opts, 5000, &len);
    CHECK(stream != NULL); if (!stream) return;

    chc_block *oracle[TEST_GOLDEN_BLOCKS] = {};
    uint64_t oracle_consumed = 0;
    int rc = test_decode_blocks_io(stream, len, &al, &opts, oracle,
                                   TEST_GOLDEN_BLOCKS, &oracle_consumed, &err);
    if (rc != 0) {
        fprintf(stderr, "%s: oracle decode failed: %s\n", current_test, err.msg);
        fail_count++;
        test_free_blocks(oracle, TEST_GOLDEN_BLOCKS, &al);
        free(stream);
        return;
    }
    CHECK_EQ_U64(oracle_consumed, len);     /* reader consumes exactly the stream */

    static const size_t chunks[] = { 1, 2, 3, 7, 64, 1u << 20 };
    for (size_t ci = 0; ci < sizeof chunks / sizeof *chunks; ci++) {
        chc_block *subj[TEST_GOLDEN_BLOCKS] = {};
        uint64_t subj_consumed = 0;
        chc_err e = {};
        int sr = decode_resume(stream, len, chunks[ci], &al, &opts, subj,
                               &subj_consumed, NULL, &e);
        if (sr != 0) {
            fprintf(stderr, "%s: chunk=%zu decode failed: %s\n",
                    current_test, chunks[ci], e.msg);
            fail_count++;
            test_free_blocks(subj, TEST_GOLDEN_BLOCKS, &al);
            continue;
        }
        if (subj_consumed != oracle_consumed) {
            fprintf(stderr, "%s: chunk=%zu consumed %llu != oracle %llu\n",
                    current_test, chunks[ci],
                    (unsigned long long) subj_consumed,
                    (unsigned long long) oracle_consumed);
            fail_count++;
        }
        for (size_t i = 0; i < TEST_GOLDEN_BLOCKS; i++) {
            if (!test_block_eq(oracle[i], subj[i])) {
                fprintf(stderr, "%s: chunk=%zu block %zu mismatch\n",
                        current_test, chunks[ci], i);
                fail_count++;
            }
        }
        test_free_blocks(subj, TEST_GOLDEN_BLOCKS, &al);
    }

    test_free_blocks(oracle, TEST_GOLDEN_BLOCKS, &al);
    free(stream);
}

/* Retention proof: with a tiny chunk size the wide block's second column (s,
 * the String column) cannot finish before the buffer drains, so resume must at
 * least once return WOULD_BLOCK holding a partial block with next_col > 0 (the
 * UInt32 column already retained). If resumption were broken (restart from
 * header each chunk) next_col would always be 0 and max_next_col would stay 0. */
static void
test_resume_retains(void)
{
    current_test = "resume_retains";
    chc_alloc al = chc_alloc_stdlib();
    chc_block_opts opts = { .has_block_info = true, .has_custom_serialization = true };

    size_t len = 0;
    uint8_t *stream = test_build_golden_stream(&al, &opts, 5000, &len);
    CHECK(stream != NULL); if (!stream) return;

    chc_block *subj[TEST_GOLDEN_BLOCKS] = {};
    uint64_t subj_consumed = 0;
    resume_stats st = {};
    chc_err e = {};
    int sr = decode_resume(stream, len, 7, &al, &opts, subj, &subj_consumed, &st, &e);
    if (sr != 0) {
        fprintf(stderr, "%s: decode failed: %s\n", current_test, e.msg);
        fail_count++;
        test_free_blocks(subj, TEST_GOLDEN_BLOCKS, &al);
        free(stream);
        return;
    }

    /* A partial block with at least one fully-decoded column was retained
     * across a would-block, not restarted from the header. */
    CHECK(st.wb_retained_midcol > 0);
    CHECK(st.max_next_col >= 1);
    /* Mid-header would-blocks happen too (the header doesn't fit in 7 bytes),
     * but retention must dominate for a 5000-row column over 7-byte chunks. */
    CHECK(st.wb_retained_midcol > st.wb_header);

    test_free_blocks(subj, TEST_GOLDEN_BLOCKS, &al);
    free(stream);
}

/* Allocator counting requests of exactly `size` bytes: a fixed column's data
 * buffer. Growth of the ioless staging buffer goes through realloc, so it does
 * not count. */
typedef struct {
    size_t size;
    size_t hits;
} size_count_alloc;

static void *
size_count_alloc_alloc(void *ud, size_t n)
{
    size_count_alloc *a = ud;
    if (n == a->size) a->hits++;
    return malloc(n ? n : 1);
}

static void *
size_count_alloc_realloc(void *ud, void *p, size_t old_n, size_t new_n)
{
    return realloc(p, new_n ? new_n : 1);
}

static void
size_count_alloc_free(void *ud, void *p, size_t n)
{
    free(p);
}

/* Short-buffer retries of a fixed column must not allocate & copy its body.
 * Rewinding to the column checkpoint after a partial copy re-reads every byte
 * buffered so far on each submit, quadratic in column size. */
static void
test_resume_fixed_no_reread(void)
{
    current_test = "resume_fixed_no_reread";
    enum { ROWS = 3000, CHUNK = 64 };
    size_count_alloc counter = { .size = ROWS * sizeof(uint64_t) };
    chc_alloc al = { &counter, size_count_alloc_alloc,
                     size_count_alloc_realloc, size_count_alloc_free };
    chc_block_opts opts = {};
    chc_block *partial = NULL;
    size_t next_col = 0, fed = 0, retries = 0;
    chc_err err = {};
    chc_in in;
    if (chc_in_init_ioless(&in, &al)) return;

    test_mem_sink s;
    chc_io io;
    test_mem_sink_init(&s, &io);
    int rc = chc__write_varuint(&io, 1, &err);
    if (!rc) rc = chc__write_varuint(&io, ROWS, &err);
    if (!rc) rc = chc__write_string(&io, "n", 1, &err);
    if (!rc) rc = chc__write_string(&io, "UInt64", 6, &err);
    for (uint64_t r = 0; !rc && r < ROWS; r++)
        rc = chc__write_bytes(&io, &r, sizeof r, &err);
    CHECK_OK(rc, err);

    while ((rc = chc__block_resume_in(&in, &al, &opts, &partial, &next_col,
                                      &err)) == CHC_WOULD_BLOCK) {
        CHECK(fed < s.len);
        if (fed >= s.len) goto out;
        size_t take = s.len - fed < CHUNK ? s.len - fed : CHUNK;
        rc = chc_in_submit(&in, s.data + fed, take, &err);
        CHECK_OK(rc, err);
        fed += take;
        retries++;
    }
    CHECK_OK(rc, err);
    CHECK(retries > ROWS * sizeof(uint64_t) / CHUNK);  /* body fed in pieces */
    CHECK_EQ_U64(counter.hits, 1);                     /* allocated once */
    CHECK(partial != NULL);
    if (partial) {
        size_t es = 0;
        const uint64_t *v = chc_column_fixed_data(chc_block_column(partial, 0), &es);
        CHECK_EQ_U64(es, sizeof *v);
        CHECK_EQ_U64(v[0], 0);
        CHECK_EQ_U64(v[ROWS - 1], ROWS - 1);
    }
out:
    chc_block_destroy(partial, &al);
    chc_in_free(&in);
    test_mem_sink_free(&s);
}

int
main(void)
{
    test_resume_golden();
    test_resume_retains();
    test_resume_fixed_no_reread();

    if (fail_count) {
        fprintf(stderr, "%d check(s) failed\n", fail_count);
        return 1;
    }
    printf("all block_resume tests passed\n");
    return 0;
}
