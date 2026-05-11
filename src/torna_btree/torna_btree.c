/**
 * Torna — multi-account B+ tree on Solana, in C.
 *
 * Layout:
 *   TreeHeader account            : metadata
 *   Node accounts (many, PDA-derived) : B+ tree nodes (one per account)
 *
 * Operations:
 *   0 InitTree   — initialize header
 *   1 InitNode   — initialize a pre-created spare account as a node
 *   2 Insert     — descend tree, insert at leaf, split if overflow
 *   3 Find       — descend tree, return value via return_data
 *   4 RangeScan  — walk leaf chain, collect values in [start, end]
 *   5 Stats      — return header info
 *
 * Cross-account aliasing: each node lives in its own account. Within a tx,
 * the SBF runtime maps every account into one address space; we read/write
 * each node by walking `accounts[i].data` directly. Splits move keys/children
 * between two account buffers with sol_memcpy — no serialization tax.
 */

#include <solana_sdk.h>

/* =========================================================================
 * Constants
 * =======================================================================*/

#define TORNA_MAGIC         0x30544254u   /* "TBT0" little-endian */
#define KEY_SIZE            32             /* fixed: 32 bytes (Pubkey-size; encodes any common composite key) */
#define VAL_SIZE_MAX        64             /* compile-time max for stack arrays / layout planning */
#define VAL_SIZE_MIN        1              /* runtime lower bound, enforced at InitTree */
/* Per-tree value_size is now a runtime parameter stored in TreeHeader.value_size, */
/* chosen at InitTree time and passed to every helper as `vs`. Strides + ix-data    */
/* offsets use this value at runtime; stack arrays are sized at VAL_SIZE_MAX.       */

/* Production-ish fanout: 64 keys per node ⇒ tree height stays low even for
 * millions of entries (height 3 holds 64^3 = 262k, height 4 holds 16M). */
#define KEYS_PER_NODE_MAX   64
#define KEYS_ARRAY_SIZE     (KEYS_PER_NODE_MAX + 1) /* +1 slot for overflow before split */
#define CHILDREN_ARRAY_SIZE (KEYS_ARRAY_SIZE + 1)

#define MAX_TREE_HEIGHT     32
#define MAX_RANGE_RESULTS   16    /* reduced from 32 with VAL_SIZE=32 to keep do_range_scan stack frame under 4KB */

/* Instruction discriminators (first byte of ix data) */
#define IX_INIT_TREE        0
#define IX_INIT_NODE        1   /* reserved (deprecated) */
#define IX_INSERT           2
#define IX_FIND             3
#define IX_RANGE_SCAN       4
#define IX_STATS            5
#define IX_INSERT_FAST      6   /* no-split path: header ro, only leaf writable */
#define IX_DELETE_FAST      7   /* no-rebalance path: header ro, only leaf writable */
#define IX_DELETE           8   /* full delete with leaf-level rebalance */
#define IX_BULK_INSERT_FAST 9   /* batch N keys into one leaf; refuses on overflow */
#define IX_BULK_DELETE_FAST 10  /* batch delete N keys from one leaf */
#define IX_TRANSFER_AUTHORITY 11 /* current authority signs, transfers to new */

#define KEYS_PER_NODE_MIN   (KEYS_PER_NODE_MAX / 2)   /* underflow threshold (32 for fanout 64) */

/* Custom error codes (must live in the LOW 32 bits; TO_BUILTIN is reserved for
 * SDK-recognized builtin runtime errors only). The runtime reports these as
 * "custom program error: 0x<hex>" in logs. */
#define ERR_BAD_MAGIC         100
#define ERR_BAD_NODE          101
#define ERR_NEED_SPLIT_SLOT   102
#define ERR_DUPLICATE_KEY     103
#define ERR_KEY_NOT_FOUND     104
#define ERR_BAD_PATH          105
#define ERR_TREE_INIT_TWICE   106
#define ERR_NODE_INIT_TWICE   107
#define ERR_HEIGHT_EXCEEDED   108
#define ERR_NOT_WRITABLE      109
#define ERR_NODE_TOO_SMALL    110
#define ERR_BAD_IX_DATA       111
#define ERR_TREE_UNINIT       112
#define ERR_NODE_UNINIT       113
#define ERR_WRONG_KEY_SIZE    114

/* =========================================================================
 * On-account layouts
 * =======================================================================*/

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t tree_id;
    uint32_t root_node_idx;     /* 0 = empty tree */
    uint32_t height;            /* 0 = empty; 1 = root is leaf; 2+ = internal root */
    uint32_t node_count;        /* number of allocated nodes (informational) */
    uint32_t leftmost_leaf_idx; /* head of leaf chain for full scans */
    uint16_t key_size;          /* fixed = KEY_SIZE */
    uint16_t value_size;        /* fixed = VAL_SIZE */
    uint64_t total_entries;     /* informational */
    uint8_t  authority[32];     /* signer required for write ix; 0 = open */
    uint8_t  reserved[12];
} TreeHeader;

#define TREE_HEADER_SIZE 80     /* sizeof packed struct = 36 + 32 + 12 = 80 */

#define ERR_NOT_AUTHORIZED   115

typedef struct __attribute__((packed)) {
    uint8_t  is_leaf;        /* 1 if leaf, 0 if internal */
    uint8_t  initialized;    /* magic flag — must be 1 to be usable */
    uint16_t key_count;
    uint32_t node_idx;       /* self-identifier */
    uint32_t parent_idx;     /* 0 if root (root's own idx may also be 0; height==1 disambiguates) */
    uint32_t next_leaf_idx;  /* leaf chain; 0 if last leaf or internal node */
} NodeHeader;

#define NODE_HEADER_SIZE 16

/* System Program ID = 32 zero bytes. */
static const SolPubkey SYSTEM_PROGRAM_ID = {{0}};

/*
 * Node body layout (offset NODE_HEADER_SIZE onward):
 *   keys[KEYS_ARRAY_SIZE][KEY_SIZE]
 *   leaf:      values[KEYS_ARRAY_SIZE][VAL_SIZE]
 *   internal:  children[CHILDREN_ARRAY_SIZE]  (each u32)
 *
 * Leaf body bytes:    NODE_HEADER_SIZE + KEYS_ARRAY_SIZE*KEY_SIZE + KEYS_ARRAY_SIZE*VAL_SIZE
 *                  = 16 + 9*32 + 9*8 = 376
 * Internal body bytes: NODE_HEADER_SIZE + KEYS_ARRAY_SIZE*KEY_SIZE + CHILDREN_ARRAY_SIZE*4
 *                  = 16 + 9*32 + 10*4 = 344
 *
 * With KEYS_PER_NODE_MAX=64, KEY_SIZE=32, VAL_SIZE=32:
 *   Leaf body:     16 + 65*32 + 65*32 = 4176 bytes
 *   Internal body: 16 + 65*32 + 66*4  = 2360 bytes
 * Round up to 8192 to comfortably fit leaves with 32-byte values plus headroom.
 */
#define NODE_ACCOUNT_DATA_SIZE 8192

/* =========================================================================
 * Accessors (work on a node's data buffer)
 * =======================================================================*/

static inline NodeHeader *node_hdr(uint8_t *data) {
    return (NodeHeader *)data;
}

static inline uint8_t *node_keys(uint8_t *data) {
    return data + NODE_HEADER_SIZE;
}

static inline uint8_t *node_values(uint8_t *data) {
    /* leaf only */
    return data + NODE_HEADER_SIZE + KEYS_ARRAY_SIZE * KEY_SIZE;
}

static inline uint32_t *node_children(uint8_t *data) {
    /* internal only */
    return (uint32_t *)(data + NODE_HEADER_SIZE + KEYS_ARRAY_SIZE * KEY_SIZE);
}

/* SDK's sol_memcmp does (uint8_t)(a-b) then returns through `int`, which
 * underflows to a positive value when a<b. It works for equality but is
 * broken for less-than. Roll our own correct signed comparison. */
static inline int key_cmp(const uint8_t *a, const uint8_t *b) {
    for (int i = 0; i < KEY_SIZE; i++) {
        if (a[i] != b[i]) return (int)a[i] - (int)b[i];
    }
    return 0;
}

/* Binary search: returns the first index i in [0..key_count] such that
 * keys[i] >= key. If keys[i] == key for some i, returns that i. */
static int node_lower_bound(uint8_t *data, const uint8_t *key) {
    NodeHeader *h = node_hdr(data);
    uint8_t *keys = node_keys(data);
    int lo = 0, hi = h->key_count;
    while (lo < hi) {
        int mid = (lo + hi) >> 1;
        int c = key_cmp(keys + (uint64_t)mid * KEY_SIZE, key);
        if (c < 0) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

/* =========================================================================
 * Helpers
 * =======================================================================*/

/* Returns true if any signer's pubkey matches the tree's authority. */
static bool tx_has_authority_signer(SolParameters *params, const uint8_t *authority) {
    /* Authority = all zeros means "open" tree (no auth required). */
    bool is_zero = true;
    for (int i = 0; i < 32; i++) if (authority[i] != 0) { is_zero = false; break; }
    if (is_zero) return true;

    for (uint64_t i = 0; i < params->ka_num; i++) {
        if (!params->ka[i].is_signer) continue;
        bool match = true;
        for (int b = 0; b < 32; b++) {
            if (params->ka[i].key->x[b] != authority[b]) { match = false; break; }
        }
        if (match) return true;
    }
    return false;
}

static uint64_t check_header(const SolAccountInfo *acc, const SolPubkey *program_id) {
    if (acc->data_len < TREE_HEADER_SIZE) return ERR_NODE_TOO_SMALL;
    if (!acc->is_writable) return ERR_NOT_WRITABLE;
    if (!SolPubkey_same(acc->owner, program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    TreeHeader *h = (TreeHeader *)acc->data;
    if (h->magic != TORNA_MAGIC) return ERR_BAD_MAGIC;
    return SUCCESS;
}

static uint64_t check_node(const SolAccountInfo *acc, const SolPubkey *program_id, uint32_t expected_idx) {
    if (acc->data_len < NODE_ACCOUNT_DATA_SIZE) return ERR_NODE_TOO_SMALL;
    if (!acc->is_writable) return ERR_NOT_WRITABLE;
    if (!SolPubkey_same(acc->owner, program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    NodeHeader *nh = node_hdr(acc->data);
    if (!nh->initialized) return ERR_NODE_UNINIT;
    if (nh->node_idx != expected_idx) return ERR_BAD_PATH;
    return SUCCESS;
}

static uint64_t check_spare(const SolAccountInfo *acc, const SolPubkey *program_id) {
    if (acc->data_len < NODE_ACCOUNT_DATA_SIZE) return ERR_NODE_TOO_SMALL;
    if (!acc->is_writable) return ERR_NOT_WRITABLE;
    if (!SolPubkey_same(acc->owner, program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    NodeHeader *nh = node_hdr(acc->data);
    if (nh->initialized) return ERR_NODE_INIT_TWICE;
    return SUCCESS;
}

/* =========================================================================
 * Instruction handlers
 * =======================================================================*/

/*
 * IX_INIT_TREE — allocates the tree header at a PDA via CPI.
 *
 *   ix_data:
 *     [0]      disc = 0
 *     [1..4]   tree_id (u32 LE)
 *     [5]      header_bump
 *     [6..13]  rent_lamports (u64 LE) — what the payer deposits
 *     [14..15] value_size (u16 LE) — per-tree value byte width, in [1, VAL_SIZE_MAX]
 *
 *   accounts:
 *     [0]   payer          (signer, writable)
 *     [1]   header_pda     (writable, must equal find_program_address(
 *                            seeds=("torna_hdr", tree_id_LE_u32), program=us))
 *     [2]   system_program (executable, read-only)
 *
 *   After this runs, anyone who knows (program_id, tree_id) can derive the
 *   header pubkey and the rest of the tree — no keypair tracking required.
 */
static uint64_t do_init_tree(SolParameters *params) {
    if (params->ka_num < 3) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;
    if (params->data_len < 1 + 4 + 1 + 8 + 2) return ERR_BAD_IX_DATA;

    uint32_t tree_id       = *(const uint32_t *)(params->data + 1);
    uint8_t  header_bump   = params->data[5];
    uint64_t rent_lamports = *(const uint64_t *)(params->data + 6);
    uint16_t value_size    = *(const uint16_t *)(params->data + 14);
    if (value_size < VAL_SIZE_MIN || value_size > VAL_SIZE_MAX) return ERR_BAD_IX_DATA;

    SolAccountInfo *payer  = &params->ka[0];
    SolAccountInfo *header = &params->ka[1];

    if (!payer->is_signer) return ERROR_MISSING_REQUIRED_SIGNATURES;
    if (!header->is_writable) return ERR_NOT_WRITABLE;
    if (header->data_len != 0) return ERR_TREE_INIT_TWICE; /* already allocated */

    /* CPI to system::create_account, signing for the header PDA. */
    uint8_t cpi_data[52];
    cpi_data[0] = 0; cpi_data[1] = 0; cpi_data[2] = 0; cpi_data[3] = 0;
    *(uint64_t *)&cpi_data[4]  = rent_lamports;
    *(uint64_t *)&cpi_data[12] = (uint64_t)TREE_HEADER_SIZE;
    sol_memcpy(&cpi_data[20], params->program_id->x, 32);

    SolAccountMeta metas[2] = {
        { (SolPubkey *)payer->key,  /*signer=*/true, /*writable=*/true },
        { (SolPubkey *)header->key, /*signer=*/true, /*writable=*/true },
    };
    SolInstruction cpi_ix = {
        .program_id  = (SolPubkey *)&SYSTEM_PROGRAM_ID,
        .accounts    = metas,
        .account_len = 2,
        .data        = cpi_data,
        .data_len    = sizeof(cpi_data),
    };
    SolSignerSeed seeds[3] = {
        { (const uint8_t *)"torna_hdr", 9 },
        { (const uint8_t *)&tree_id, 4 },
        { (const uint8_t *)&header_bump, 1 },
    };
    SolSignerSeeds signer = { seeds, 3 };
    SolSignerSeeds signers[1] = { signer };

    uint64_t cpi_err = sol_invoke_signed(&cpi_ix, params->ka, params->ka_num, signers, 1);
    if (cpi_err) return cpi_err;

    /* Initialize header. */
    TreeHeader *h = (TreeHeader *)header->data;
    sol_memset(header->data, 0, TREE_HEADER_SIZE);
    h->magic             = TORNA_MAGIC;
    h->tree_id           = tree_id;
    h->root_node_idx     = 0;
    h->height            = 0;
    h->node_count        = 0;
    h->leftmost_leaf_idx = 0;
    h->key_size          = KEY_SIZE;
    h->value_size        = value_size;
    h->total_entries     = 0;
    sol_memcpy(h->authority, payer->key->x, 32);  /* payer becomes write-authority */

    sol_log("torna: init_tree (PDA) ok");
    return SUCCESS;
}

/*
 * Consume a PDA-derived spare via CPI to system_program::create_account:
 *   - Verify spare is not yet allocated (data_len == 0)
 *   - CPI invoke_signed: pays rent, allocates NODE_ACCOUNT_DATA_SIZE, sets owner
 *   - After CPI, write NodeHeader (the runtime zeroes the new account on create)
 *
 * The spare account MUST be at PDA: seeds = ("torna", tree_id_u32, node_idx_u32),
 * program = this program. invoke_signed fails with InvalidSeeds otherwise.
 *
 * Critical: only invoked when a split actually needs a new node. Unused
 * pre-passed spares cost ZERO — they're just unallocated PDAs.
 */
static uint64_t consume_spare_pda(
    SolParameters *params,
    int payer_idx, int spare_idx,
    TreeHeader *th, uint8_t is_leaf, uint32_t parent_idx,
    uint64_t rent_lamports, uint8_t bump,
    uint32_t *out_idx
) {
    SolAccountInfo *spare = &params->ka[spare_idx];
    SolAccountInfo *payer = &params->ka[payer_idx];

    if (!spare->is_writable) return ERR_NOT_WRITABLE;
    if (spare->data_len != 0) return ERR_NODE_INIT_TWICE;

    uint32_t new_idx    = th->node_count + 1;
    uint32_t tree_id_le = th->tree_id;

    /* system::create_account ix layout: u32 disc=0, u64 lamports, u64 space, [32] owner */
    uint8_t cpi_data[52];
    cpi_data[0] = 0; cpi_data[1] = 0; cpi_data[2] = 0; cpi_data[3] = 0;
    *(uint64_t *)&cpi_data[4]  = rent_lamports;
    *(uint64_t *)&cpi_data[12] = (uint64_t)NODE_ACCOUNT_DATA_SIZE;
    sol_memcpy(&cpi_data[20], params->program_id->x, 32);

    SolAccountMeta metas[2] = {
        { (SolPubkey *)payer->key, /*signer=*/true, /*writable=*/true },
        { (SolPubkey *)spare->key, /*signer=*/true, /*writable=*/true },
    };
    SolInstruction cpi_ix = {
        .program_id  = (SolPubkey *)&SYSTEM_PROGRAM_ID,
        .accounts    = metas,
        .account_len = 2,
        .data        = cpi_data,
        .data_len    = sizeof(cpi_data),
    };

    SolSignerSeed seeds[4] = {
        { (const uint8_t *)"torna", 5 },
        { (const uint8_t *)&tree_id_le, 4 },
        { (const uint8_t *)&new_idx, 4 },
        { (const uint8_t *)&bump, 1 },
    };
    SolSignerSeeds signer  = { seeds, 4 };
    SolSignerSeeds signers[1] = { signer };

    uint64_t err = sol_invoke_signed(&cpi_ix, params->ka, params->ka_num, signers, 1);
    if (err) return err;

    /* Post-CPI: the account has been allocated and zeroed by the system program,
     * owner is set to us. Write the NodeHeader. */
    NodeHeader *nh = node_hdr(spare->data);
    nh->is_leaf       = is_leaf ? 1 : 0;
    nh->initialized   = 1;
    nh->key_count     = 0;
    nh->node_idx      = new_idx;
    nh->parent_idx    = parent_idx;
    nh->next_leaf_idx = 0;

    th->node_count = new_idx;
    *out_idx = new_idx;
    return SUCCESS;
}

/* Delete `key` from a leaf node by shifting later entries left. If
 * out_value is non-NULL, copies the deleted value out first. */
static uint64_t leaf_delete(uint8_t *data, const uint8_t *key, uint8_t *out_value, uint16_t vs) {
    NodeHeader *nh = node_hdr(data);
    uint8_t *keys = node_keys(data);
    uint8_t *vals = node_values(data);

    int pos = node_lower_bound(data, key);
    if (pos >= nh->key_count || key_cmp(keys + (uint64_t)pos * KEY_SIZE, key) != 0) {
        return ERR_KEY_NOT_FOUND;
    }
    if (out_value) sol_memcpy(out_value, vals + (uint64_t)pos * vs, vs);

    for (int i = pos; i < nh->key_count - 1; i++) {
        sol_memcpy(keys + (uint64_t)i * KEY_SIZE, keys + (uint64_t)(i + 1) * KEY_SIZE, KEY_SIZE);
        sol_memcpy(vals + (uint64_t)i * vs, vals + (uint64_t)(i + 1) * vs, vs);
    }
    nh->key_count--;
    return SUCCESS;
}

/* Borrow one entry from `right` sibling into `left` leaf. The parent's
 * separator at `sep_pos` (which is the first key of right) is updated to
 * the new first key of right (after shift). */
static void leaf_borrow_from_right(uint8_t *left, uint8_t *right, uint8_t *parent, int sep_pos, uint16_t vs) {
    NodeHeader *lh = node_hdr(left);
    NodeHeader *rh = node_hdr(right);
    uint8_t *lk = node_keys(left);
    uint8_t *lv = node_values(left);
    uint8_t *rk = node_keys(right);
    uint8_t *rv = node_values(right);

    /* Move right[0] to left[end]. */
    sol_memcpy(lk + (uint64_t)lh->key_count * KEY_SIZE, rk, KEY_SIZE);
    sol_memcpy(lv + (uint64_t)lh->key_count * vs, rv, vs);
    lh->key_count++;

    /* Shift right entries left by 1. */
    for (int i = 0; i < rh->key_count - 1; i++) {
        sol_memcpy(rk + (uint64_t)i * KEY_SIZE, rk + (uint64_t)(i + 1) * KEY_SIZE, KEY_SIZE);
        sol_memcpy(rv + (uint64_t)i * vs, rv + (uint64_t)(i + 1) * vs, vs);
    }
    rh->key_count--;

    /* Update parent separator to the new right[0]. */
    sol_memcpy(node_keys(parent) + (uint64_t)sep_pos * KEY_SIZE, rk, KEY_SIZE);
}

/* Borrow one entry from `left` sibling into `right` leaf. The parent's
 * separator at `sep_pos` is set to the new right[0] (which is the
 * borrowed key). */
static void leaf_borrow_from_left(uint8_t *left, uint8_t *right, uint8_t *parent, int sep_pos, uint16_t vs) {
    NodeHeader *lh = node_hdr(left);
    NodeHeader *rh = node_hdr(right);
    uint8_t *lk = node_keys(left);
    uint8_t *lv = node_values(left);
    uint8_t *rk = node_keys(right);
    uint8_t *rv = node_values(right);

    /* Shift right entries right by 1 to make room at index 0. */
    for (int i = rh->key_count; i > 0; i--) {
        sol_memcpy(rk + (uint64_t)i * KEY_SIZE, rk + (uint64_t)(i - 1) * KEY_SIZE, KEY_SIZE);
        sol_memcpy(rv + (uint64_t)i * vs, rv + (uint64_t)(i - 1) * vs, vs);
    }
    /* Move left[end-1] to right[0]. */
    sol_memcpy(rk, lk + (uint64_t)(lh->key_count - 1) * KEY_SIZE, KEY_SIZE);
    sol_memcpy(rv, lv + (uint64_t)(lh->key_count - 1) * vs, vs);
    rh->key_count++;
    lh->key_count--;

    /* Update parent separator to the new right[0]. */
    sol_memcpy(node_keys(parent) + (uint64_t)sep_pos * KEY_SIZE, rk, KEY_SIZE);
}

/* Merge `right` leaf into `left`. After this, `right` is empty and the
 * caller should close the right account. Parent's separator at sep_pos
 * and its child[sep_pos+1] should be removed (caller handles). */
static uint64_t leaf_merge(uint8_t *left, uint8_t *right, uint16_t vs) {
    NodeHeader *lh = node_hdr(left);
    NodeHeader *rh = node_hdr(right);
    if (lh->key_count + rh->key_count > KEYS_PER_NODE_MAX) return ERR_BAD_NODE; /* can't fit */

    uint8_t *lk = node_keys(left);
    uint8_t *lv = node_values(left);
    uint8_t *rk = node_keys(right);
    uint8_t *rv = node_values(right);

    for (int i = 0; i < rh->key_count; i++) {
        sol_memcpy(lk + (uint64_t)(lh->key_count + i) * KEY_SIZE, rk + (uint64_t)i * KEY_SIZE, KEY_SIZE);
        sol_memcpy(lv + (uint64_t)(lh->key_count + i) * vs, rv + (uint64_t)i * vs, vs);
    }
    lh->key_count += rh->key_count;
    lh->next_leaf_idx = rh->next_leaf_idx;
    rh->key_count = 0;
    rh->initialized = 0; /* mark as freeable */
    return SUCCESS;
}

/* Internal-node borrow from right sibling. cur has underflowed; sib has > MIN. */
static void internal_borrow_from_right(uint8_t *cur, uint8_t *sib, uint8_t *parent, int sep_pos) {
    NodeHeader *ch = node_hdr(cur);
    NodeHeader *sh = node_hdr(sib);
    uint8_t  *ck = node_keys(cur);
    uint32_t *cc = node_children(cur);
    uint8_t  *sk = node_keys(sib);
    uint32_t *sc = node_children(sib);
    uint8_t  *pk = node_keys(parent);

    /* cur gains: parent.keys[sep_pos] as new last key; sib.children[0] as new last child */
    sol_memcpy(ck + (uint64_t)ch->key_count * KEY_SIZE,
               pk + (uint64_t)sep_pos * KEY_SIZE, KEY_SIZE);
    cc[ch->key_count + 1] = sc[0];
    ch->key_count++;

    /* parent's separator becomes sibling's first key */
    sol_memcpy(pk + (uint64_t)sep_pos * KEY_SIZE, sk, KEY_SIZE);

    /* shift sibling left by 1 (keys and children) */
    for (int i = 0; i + 1 < sh->key_count; i++) {
        sol_memcpy(sk + (uint64_t)i * KEY_SIZE,
                   sk + (uint64_t)(i + 1) * KEY_SIZE, KEY_SIZE);
    }
    for (int i = 0; i < sh->key_count; i++) {
        sc[i] = sc[i + 1];
    }
    sh->key_count--;
}

/* Internal-node borrow from left sibling. cur has underflowed; sib has > MIN. */
static void internal_borrow_from_left(uint8_t *sib_left, uint8_t *cur, uint8_t *parent, int sep_pos) {
    NodeHeader *sh = node_hdr(sib_left);
    NodeHeader *ch = node_hdr(cur);
    uint8_t  *sk = node_keys(sib_left);
    uint32_t *sc = node_children(sib_left);
    uint8_t  *ck = node_keys(cur);
    uint32_t *cc = node_children(cur);
    uint8_t  *pk = node_keys(parent);

    /* shift cur's keys and children right by 1 */
    for (int i = ch->key_count; i > 0; i--) {
        sol_memcpy(ck + (uint64_t)i * KEY_SIZE,
                   ck + (uint64_t)(i - 1) * KEY_SIZE, KEY_SIZE);
    }
    for (int i = ch->key_count + 1; i > 0; i--) {
        cc[i] = cc[i - 1];
    }
    /* cur's new first key = parent's separator */
    sol_memcpy(ck, pk + (uint64_t)sep_pos * KEY_SIZE, KEY_SIZE);
    /* cur's new first child = sibling's last child */
    cc[0] = sc[sh->key_count];
    ch->key_count++;

    /* parent's separator = sibling's last key */
    sol_memcpy(pk + (uint64_t)sep_pos * KEY_SIZE,
               sk + (uint64_t)(sh->key_count - 1) * KEY_SIZE, KEY_SIZE);
    sh->key_count--;
}

/* Internal-node merge: pull `right` into `left` with parent's separator
 * between them as the bridge key. After this, `right` is empty and the
 * caller should close its account + remove parent.keys[sep_pos]. */
static uint64_t internal_merge_right(uint8_t *left, uint8_t *right, uint8_t *parent, int sep_pos) {
    NodeHeader *lh = node_hdr(left);
    NodeHeader *rh = node_hdr(right);
    uint8_t  *lk = node_keys(left);
    uint32_t *lc = node_children(left);
    uint8_t  *rk = node_keys(right);
    uint32_t *rc = node_children(right);
    uint8_t  *pk = node_keys(parent);

    int new_count = lh->key_count + 1 + rh->key_count;
    if (new_count > KEYS_PER_NODE_MAX) return ERR_BAD_NODE;

    /* append parent's separator key */
    sol_memcpy(lk + (uint64_t)lh->key_count * KEY_SIZE,
               pk + (uint64_t)sep_pos * KEY_SIZE, KEY_SIZE);
    /* append right's keys */
    for (int i = 0; i < rh->key_count; i++) {
        sol_memcpy(lk + (uint64_t)(lh->key_count + 1 + i) * KEY_SIZE,
                   rk + (uint64_t)i * KEY_SIZE, KEY_SIZE);
    }
    /* append right's children */
    for (int i = 0; i <= rh->key_count; i++) {
        lc[lh->key_count + 1 + i] = rc[i];
    }
    lh->key_count = new_count;
    rh->key_count = 0;
    rh->initialized = 0;
    return SUCCESS;
}

/* Close a program-owned account: drain lamports to recipient + zero data.
 * The runtime garbage-collects accounts with 0 lamports. */
static void close_account(SolAccountInfo *to_close, SolAccountInfo *recipient) {
    *recipient->lamports = *recipient->lamports + *to_close->lamports;
    *to_close->lamports = 0;
    sol_memset(to_close->data, 0, to_close->data_len);
}

/* Remove separator at `pos` and child pointer at `pos+1` from an internal node. */
static void internal_remove_at(uint8_t *data, int pos) {
    NodeHeader *nh = node_hdr(data);
    uint8_t *keys = node_keys(data);
    uint32_t *kids = node_children(data);

    /* Shift keys[pos+1..) left by 1. */
    for (int i = pos; i < nh->key_count - 1; i++) {
        sol_memcpy(keys + (uint64_t)i * KEY_SIZE, keys + (uint64_t)(i + 1) * KEY_SIZE, KEY_SIZE);
    }
    /* Shift children[pos+2..) left by 1. */
    for (int i = pos + 1; i < nh->key_count; i++) {
        kids[i] = kids[i + 1];
    }
    nh->key_count--;
}

/* Insert (key, value) into a leaf node's data buffer at sorted position. */
static uint64_t leaf_insert(uint8_t *data, const uint8_t *key, const uint8_t *value, uint16_t vs) {
    NodeHeader *nh = node_hdr(data);
    uint8_t *keys = node_keys(data);
    uint8_t *vals = node_values(data);

    int pos = node_lower_bound(data, key);
    if (pos < nh->key_count && key_cmp(keys + (uint64_t)pos * KEY_SIZE, key) == 0) {
        return ERR_DUPLICATE_KEY;
    }

    /* Shift entries [pos .. key_count) one slot right. */
    if (pos < nh->key_count) {
        for (int i = nh->key_count; i > pos; i--) {
            sol_memcpy(keys + (uint64_t)i * KEY_SIZE, keys + (uint64_t)(i - 1) * KEY_SIZE, KEY_SIZE);
            sol_memcpy(vals + (uint64_t)i * vs, vals + (uint64_t)(i - 1) * vs, vs);
        }
    }

    sol_memcpy(keys + (uint64_t)pos * KEY_SIZE, key, KEY_SIZE);
    sol_memcpy(vals + (uint64_t)pos * vs, value, vs);
    nh->key_count++;
    return SUCCESS;
}

/* Insert into internal node: a separator key + child index, at position pos.
 * The new child takes the slot to the RIGHT of the separator. */
static void internal_insert_at(uint8_t *data, int pos, const uint8_t *sep_key, uint32_t right_child_idx) {
    NodeHeader *nh = node_hdr(data);
    uint8_t  *keys = node_keys(data);
    uint32_t *kids = node_children(data);

    /* Shift keys [pos..) right, shift children [pos+1..) right. */
    for (int i = nh->key_count; i > pos; i--) {
        sol_memcpy(keys + (uint64_t)i * KEY_SIZE, keys + (uint64_t)(i - 1) * KEY_SIZE, KEY_SIZE);
    }
    for (int i = nh->key_count + 1; i > pos + 1; i--) {
        kids[i] = kids[i - 1];
    }

    sol_memcpy(keys + (uint64_t)pos * KEY_SIZE, sep_key, KEY_SIZE);
    kids[pos + 1] = right_child_idx;
    nh->key_count++;
}

/*
 * Split a leaf. `data` is the overfull leaf. `new_data` is an empty spare
 * (already initialized as leaf). Move the right half of `data` into `new_data`.
 * `out_separator` receives the first key of the new (right) leaf.
 */
static void leaf_split(uint8_t *data, uint8_t *new_data, uint8_t *out_separator, uint16_t vs) {
    NodeHeader *lh = node_hdr(data);
    NodeHeader *rh = node_hdr(new_data);
    uint8_t *l_keys = node_keys(data);
    uint8_t *l_vals = node_values(data);
    uint8_t *r_keys = node_keys(new_data);
    uint8_t *r_vals = node_values(new_data);

    int total = lh->key_count;
    int half  = total / 2;
    int moved = total - half;

    for (int i = 0; i < moved; i++) {
        sol_memcpy(r_keys + (uint64_t)i * KEY_SIZE, l_keys + (uint64_t)(half + i) * KEY_SIZE, KEY_SIZE);
        sol_memcpy(r_vals + (uint64_t)i * vs, l_vals + (uint64_t)(half + i) * vs, vs);
    }
    rh->key_count = (uint16_t)moved;
    lh->key_count = (uint16_t)half;

    /* Leaf chain: rh slots in between lh and lh's old next */
    rh->next_leaf_idx = lh->next_leaf_idx;
    lh->next_leaf_idx = rh->node_idx;
    rh->parent_idx    = lh->parent_idx; /* updated by parent insert if needed */

    /* B+ tree: separator is the first key of the right node (kept in the leaf). */
    sol_memcpy(out_separator, r_keys, KEY_SIZE);
}

/*
 * Split an internal node. The middle key is promoted (returned via out_separator),
 * NOT kept in either child. Right half moves to `new_data`.
 */
static void internal_split(uint8_t *data, uint8_t *new_data, uint8_t *out_separator) {
    NodeHeader *lh = node_hdr(data);
    NodeHeader *rh = node_hdr(new_data);
    uint8_t  *l_keys = node_keys(data);
    uint32_t *l_kids = node_children(data);
    uint8_t  *r_keys = node_keys(new_data);
    uint32_t *r_kids = node_children(new_data);

    int total = lh->key_count;
    int mid   = total / 2;

    /* Promote keys[mid] up. Right keeps keys[mid+1..total-1]; left keeps keys[0..mid-1]. */
    sol_memcpy(out_separator, l_keys + (uint64_t)mid * KEY_SIZE, KEY_SIZE);

    int right_keys = total - mid - 1;
    for (int i = 0; i < right_keys; i++) {
        sol_memcpy(r_keys + (uint64_t)i * KEY_SIZE, l_keys + (uint64_t)(mid + 1 + i) * KEY_SIZE, KEY_SIZE);
    }
    /* children[mid+1..total] move to right (right_keys + 1 children). */
    for (int i = 0; i <= right_keys; i++) {
        r_kids[i] = l_kids[mid + 1 + i];
    }
    rh->key_count  = (uint16_t)right_keys;
    lh->key_count  = (uint16_t)mid;
    rh->parent_idx = lh->parent_idx;
}

/*
 * IX_INSERT (PDA self-allocation)
 *   ix_data:
 *     [0]                disc = 2
 *     [1..32]            key (32 bytes)
 *     [33..40]           value (8 bytes)
 *     [41..48]           rent_lamports (u64 LE) — what to deposit per spare
 *     [49]               path_len
 *     [50]               spare_count
 *     [51..51+spare_count)  bump bytes, one per declared spare PDA
 *
 *   accounts:
 *     [0]                          header (writable)
 *     [1]                          payer (signer, writable) — funds spare creation
 *     [2]                          system_program (executable, read-only)
 *     [3 .. 2+path_len]            path: root → leaf (writable, must exist)
 *     [3+path_len .. 2+path_len+spare_count]
 *                                   spare PDAs (writable, may not yet exist).
 *                                   Each must equal find_program_address with
 *                                   seeds=("torna", tree_id_u32, (node_count+i+1)_u32)
 *                                   and the corresponding bump from ix_data.
 *                                   Spares are consumed in order as splits occur;
 *                                   any unused spare is never allocated — costs 0.
 */
static uint64_t do_insert(SolParameters *params) {
    /* Read header first to learn the runtime value_size. */
    if (params->ka_num < 1) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;
    uint64_t err = check_header(&params->ka[0], params->program_id);
    if (err) return err;
    TreeHeader *th = (TreeHeader *)params->ka[0].data;
    uint16_t vs = th->value_size;

    /* Min ix_data with runtime value_size = disc + key + value + rent + path_len + spare_count */
    uint64_t min_hdr = (uint64_t)1 + KEY_SIZE + vs + 8 + 1 + 1;
    if (params->data_len < min_hdr) return ERR_BAD_IX_DATA;

    uint8_t  path_len      = params->data[1 + KEY_SIZE + vs + 8];
    uint8_t  spare_count   = params->data[1 + KEY_SIZE + vs + 8 + 1];
    uint64_t rent_lamports = *(const uint64_t *)(params->data + 1 + KEY_SIZE + vs);
    const uint8_t *key     = params->data + 1;
    const uint8_t *value   = params->data + 1 + KEY_SIZE;
    const uint8_t *bumps   = params->data + min_hdr;
    uint64_t bumps_len     = params->data_len - min_hdr;
    if (bumps_len < (uint64_t)spare_count) return ERR_BAD_IX_DATA;

    if (params->ka_num < (uint64_t)3 + path_len + spare_count) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    if (path_len != th->height) return ERR_BAD_PATH;
    if (!params->ka[1].is_signer) return ERROR_MISSING_REQUIRED_SIGNATURES;
    if (!tx_has_authority_signer(params, th->authority)) return ERR_NOT_AUTHORIZED;

    /* Spare slots live after [header, payer, sysprog, path...]. */
    uint32_t spare_base = 3 + (uint32_t)path_len;
    int spare_used = 0;

    /* ---- Special case: empty tree → first insert allocates a leaf root ---- */
    if (th->height == 0) {
        if (spare_count < 1) return ERR_NEED_SPLIT_SLOT;
        uint32_t new_idx;
        err = consume_spare_pda(params, /*payer_idx=*/1, /*spare_idx=*/(int)spare_base + 0,
                                th, /*is_leaf=*/1, /*parent_idx=*/0,
                                rent_lamports, bumps[0], &new_idx);
        if (err) return err;
        SolAccountInfo *spare = &params->ka[spare_base + 0];
        err = leaf_insert(spare->data, key, value, vs);
        if (err) return err;

        th->root_node_idx     = new_idx;
        th->height            = 1;
        th->leftmost_leaf_idx = new_idx;
        th->total_entries++;
        sol_log("torna: first insert ok, height=1");
        return SUCCESS;
    }

    /* ---- General case: descend, insert at leaf, propagate splits ---- */
    if (path_len > MAX_TREE_HEIGHT) return ERR_HEIGHT_EXCEEDED;

    uint32_t path_base = 3;

    /* Validate root. */
    SolAccountInfo *root_acc = &params->ka[path_base + 0];
    err = check_node(root_acc, params->program_id, th->root_node_idx);
    if (err) return err;

    /* Walk down, validating each child link. */
    for (int level = 0; level < path_len - 1; level++) {
        SolAccountInfo *cur = &params->ka[path_base + level];
        NodeHeader *nh = node_hdr(cur->data);
        if (nh->is_leaf) return ERR_BAD_PATH;
        int pos = node_lower_bound(cur->data, key);
        uint32_t *kids = node_children(cur->data);
        uint32_t desc_idx;
        if (pos < nh->key_count && key_cmp(node_keys(cur->data) + (uint64_t)pos * KEY_SIZE, key) == 0) {
            desc_idx = kids[pos + 1];
        } else {
            desc_idx = kids[pos];
        }
        SolAccountInfo *nxt = &params->ka[path_base + level + 1];
        err = check_node(nxt, params->program_id, desc_idx);
        if (err) return err;
    }

    int leaf_path_pos = path_len - 1;
    SolAccountInfo *leaf_acc = &params->ka[path_base + leaf_path_pos];
    {
        NodeHeader *nh = node_hdr(leaf_acc->data);
        if (!nh->is_leaf) return ERR_BAD_PATH;
    }

    /* Insert into leaf. */
    err = leaf_insert(leaf_acc->data, key, value, vs);
    if (err) return err;
    th->total_entries++;

    /* Walk back up, splitting any node that overflowed. */
    uint8_t sep_buf[KEY_SIZE];
    uint8_t *separator = sep_buf;
    uint32_t new_right_idx = 0;
    int split_propagating  = 0;

    /* Leaf overflow check */
    NodeHeader *cur_nh = node_hdr(leaf_acc->data);
    if (cur_nh->key_count > KEYS_PER_NODE_MAX) {
        if (spare_used >= spare_count) return ERR_NEED_SPLIT_SLOT;
        uint32_t new_idx;
        err = consume_spare_pda(params, /*payer_idx=*/1,
                                /*spare_idx=*/(int)spare_base + spare_used,
                                th, /*is_leaf=*/1, cur_nh->parent_idx,
                                rent_lamports, bumps[spare_used], &new_idx);
        if (err) return err;
        SolAccountInfo *spare = &params->ka[spare_base + spare_used];
        spare_used++;

        leaf_split(leaf_acc->data, spare->data, separator, vs);
        new_right_idx     = new_idx;
        split_propagating = 1;
    }

    /* Propagate splits up the path. */
    for (int level = leaf_path_pos - 1; level >= 0 && split_propagating; level--) {
        SolAccountInfo *par_acc = &params->ka[path_base + level];
        NodeHeader *par_nh = node_hdr(par_acc->data);

        /* The newly-allocated child of this split needs its parent_idx set. */
        {
            SolAccountInfo *last_new = &params->ka[spare_base + spare_used - 1];
            node_hdr(last_new->data)->parent_idx = par_nh->node_idx;
        }

        int pos = node_lower_bound(par_acc->data, separator);
        internal_insert_at(par_acc->data, pos, separator, new_right_idx);

        if (par_nh->key_count > KEYS_PER_NODE_MAX) {
            if (spare_used >= spare_count) return ERR_NEED_SPLIT_SLOT;
            uint32_t new_idx;
            err = consume_spare_pda(params, /*payer_idx=*/1,
                                    /*spare_idx=*/(int)spare_base + spare_used,
                                    th, /*is_leaf=*/0, par_nh->parent_idx,
                                    rent_lamports, bumps[spare_used], &new_idx);
            if (err) return err;
            SolAccountInfo *spare = &params->ka[spare_base + spare_used];
            spare_used++;

            internal_split(par_acc->data, spare->data, separator);
            new_right_idx = new_idx;
        } else {
            split_propagating = 0;
        }
    }

    /* If we propagated past root, grow tree height: allocate one more spare as new root. */
    if (split_propagating) {
        if (spare_used >= spare_count) return ERR_NEED_SPLIT_SLOT;
        uint32_t new_root_idx;
        err = consume_spare_pda(params, /*payer_idx=*/1,
                                /*spare_idx=*/(int)spare_base + spare_used,
                                th, /*is_leaf=*/0, /*parent_idx=*/0,
                                rent_lamports, bumps[spare_used], &new_root_idx);
        if (err) return err;
        SolAccountInfo *new_root = &params->ka[spare_base + spare_used];
        spare_used++;

        NodeHeader *rh = node_hdr(new_root->data);
        rh->key_count = 1;
        sol_memcpy(node_keys(new_root->data), separator, KEY_SIZE);
        uint32_t *kids = node_children(new_root->data);
        kids[0] = th->root_node_idx;
        kids[1] = new_right_idx;

        /* Update parent pointers on old root and the latest right-half child. */
        node_hdr(params->ka[path_base + 0].data)->parent_idx = new_root_idx;
        SolAccountInfo *last_new = &params->ka[spare_base + spare_used - 2];
        node_hdr(last_new->data)->parent_idx = new_root_idx;

        th->root_node_idx = new_root_idx;
        th->height++;
        sol_log("torna: tree grew, new root");
    }

    return SUCCESS;
}

/*
 * IX_INSERT_FAST — write-only-to-leaf path.
 *
 *   No CPI, no payer, no spare allocation, no header writes. The program
 *   asserts the target leaf has slack capacity (key_count < MAX) and inserts.
 *   If the leaf would overflow, returns ERR_NEED_SPLIT_SLOT and the caller
 *   should retry with IX_INSERT (full path with spares + payer + sysprog).
 *
 *   ix_data: [u8 disc=6][u8 key[32]][u8 value[8]][u8 path_len]
 *   accounts:
 *     [0]                    header (READ-ONLY)
 *     [1..path_len-1]        internal nodes (read-only) — root, intermediates
 *     [path_len]             leaf (writable)
 *
 *   Critical: header is declared read-only in tx, so two FAST inserts to
 *   different leaves carry disjoint write sets {leaf_a} vs {leaf_b}. The
 *   Solana scheduler can execute them concurrently. This is the parallelism
 *   story that single-account designs (Phoenix, OpenBook slab) cannot offer.
 */
static uint64_t do_insert_fast(SolParameters *params) {
    if (params->ka_num < 2) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    /* Header: read-only validation only — DO NOT WRITE. */
    SolAccountInfo *hdr_acc = &params->ka[0];
    if (hdr_acc->data_len < TREE_HEADER_SIZE) return ERR_NODE_TOO_SMALL;
    if (!SolPubkey_same(hdr_acc->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    const TreeHeader *th = (const TreeHeader *)hdr_acc->data;
    if (th->magic != TORNA_MAGIC) return ERR_BAD_MAGIC;
    if (!tx_has_authority_signer(params, th->authority)) return ERR_NOT_AUTHORIZED;
    uint16_t vs = th->value_size;

    if (params->data_len < (uint64_t)(1 + KEY_SIZE + vs + 1)) return ERR_BAD_IX_DATA;

    const uint8_t *key   = params->data + 1;
    const uint8_t *value = params->data + 1 + KEY_SIZE;
    uint8_t path_len     = params->data[1 + KEY_SIZE + vs];

    if (path_len != th->height) return ERR_BAD_PATH;
    if (path_len == 0) return ERR_TREE_UNINIT;
    if (params->ka_num < (uint64_t)1 + path_len) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    /* Descend (read-only intermediate validation). */
    SolAccountInfo *root_acc = &params->ka[1];
    if (!SolPubkey_same(root_acc->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    {
        NodeHeader *rh = node_hdr(root_acc->data);
        if (!rh->initialized) return ERR_NODE_UNINIT;
        if (rh->node_idx != th->root_node_idx) return ERR_BAD_PATH;
    }

    for (int level = 0; level < path_len - 1; level++) {
        SolAccountInfo *cur = &params->ka[1 + level];
        NodeHeader *nh = node_hdr(cur->data);
        if (nh->is_leaf) return ERR_BAD_PATH;
        int pos = node_lower_bound(cur->data, key);
        uint32_t *kids = node_children(cur->data);
        uint32_t desc_idx;
        if (pos < nh->key_count && key_cmp(node_keys(cur->data) + (uint64_t)pos * KEY_SIZE, key) == 0) {
            desc_idx = kids[pos + 1];
        } else {
            desc_idx = kids[pos];
        }
        SolAccountInfo *nxt = &params->ka[1 + level + 1];
        if (!SolPubkey_same(nxt->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
        NodeHeader *nh_nxt = node_hdr(nxt->data);
        if (!nh_nxt->initialized) return ERR_NODE_UNINIT;
        if (nh_nxt->node_idx != desc_idx) return ERR_BAD_PATH;
    }

    SolAccountInfo *leaf_acc = &params->ka[1 + path_len - 1];
    if (!leaf_acc->is_writable) return ERR_NOT_WRITABLE;
    NodeHeader *lh = node_hdr(leaf_acc->data);
    if (!lh->is_leaf) return ERR_BAD_PATH;

    /* Refuse if leaf would overflow — caller must use full Insert. */
    if (lh->key_count >= KEYS_PER_NODE_MAX) return ERR_NEED_SPLIT_SLOT;

    return leaf_insert(leaf_acc->data, key, value, vs);
}

/*
 * IX_DELETE_FAST — shift-delete in leaf only, no rebalance.
 *
 *   Symmetric with IX_INSERT_FAST: header is READ-ONLY, only the target
 *   leaf is writable. Allows parallel deletes against different leaves.
 *
 *   ix_data: [u8 disc=7][u8 key[32]][u8 path_len]
 *   accounts:
 *     [0]                    header (READ-ONLY)
 *     [1..path_len-1]        internal nodes (read-only)
 *     [path_len]             leaf (writable)
 *   return_data: [u8 found=1][u8 value[8]]   (or [0] if not found)
 */
static uint64_t do_delete_fast(SolParameters *params) {
    if (params->data_len < 1 + KEY_SIZE + 1) return ERR_BAD_IX_DATA;
    if (params->ka_num < 2) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    SolAccountInfo *hdr_acc = &params->ka[0];
    if (hdr_acc->data_len < TREE_HEADER_SIZE) return ERR_NODE_TOO_SMALL;
    if (!SolPubkey_same(hdr_acc->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    const TreeHeader *th = (const TreeHeader *)hdr_acc->data;
    if (th->magic != TORNA_MAGIC) return ERR_BAD_MAGIC;
    if (!tx_has_authority_signer(params, th->authority)) return ERR_NOT_AUTHORIZED;
    uint16_t vs = th->value_size;

    const uint8_t *key = params->data + 1;
    uint8_t path_len   = params->data[1 + KEY_SIZE];

    if (path_len != th->height) return ERR_BAD_PATH;
    if (path_len == 0) return ERR_TREE_UNINIT;
    if (params->ka_num < (uint64_t)1 + path_len) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    /* Descend. */
    SolAccountInfo *root_acc = &params->ka[1];
    if (!SolPubkey_same(root_acc->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    {
        NodeHeader *rh = node_hdr(root_acc->data);
        if (!rh->initialized) return ERR_NODE_UNINIT;
        if (rh->node_idx != th->root_node_idx) return ERR_BAD_PATH;
    }

    for (int level = 0; level < path_len - 1; level++) {
        SolAccountInfo *cur = &params->ka[1 + level];
        NodeHeader *nh = node_hdr(cur->data);
        if (nh->is_leaf) return ERR_BAD_PATH;
        int pos = node_lower_bound(cur->data, key);
        uint32_t *kids = node_children(cur->data);
        uint32_t desc_idx;
        if (pos < nh->key_count && key_cmp(node_keys(cur->data) + (uint64_t)pos * KEY_SIZE, key) == 0) {
            desc_idx = kids[pos + 1];
        } else {
            desc_idx = kids[pos];
        }
        SolAccountInfo *nxt = &params->ka[1 + level + 1];
        if (!SolPubkey_same(nxt->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
        NodeHeader *nxt_h = node_hdr(nxt->data);
        if (!nxt_h->initialized) return ERR_NODE_UNINIT;
        if (nxt_h->node_idx != desc_idx) return ERR_BAD_PATH;
    }

    SolAccountInfo *leaf_acc = &params->ka[1 + path_len - 1];
    if (!leaf_acc->is_writable) return ERR_NOT_WRITABLE;
    NodeHeader *lh = node_hdr(leaf_acc->data);
    if (!lh->is_leaf) return ERR_BAD_PATH;

    uint8_t out_value[VAL_SIZE_MAX];
    uint64_t err = leaf_delete(leaf_acc->data, key, out_value, vs);
    if (err) {
        /* Return [u8 found=0] on not-found instead of failing the tx. */
        uint8_t ret[1] = {0};
        sol_set_return_data(ret, 1);
        return SUCCESS;
    }

    uint8_t ret[1 + VAL_SIZE_MAX];
    ret[0] = 1;
    sol_memcpy(&ret[1], out_value, vs);
    sol_set_return_data(ret, 1 + (uint64_t)vs);
    return SUCCESS;
}

/*
 * IX_DELETE — delete with cascading rebalance from leaf up to root.
 *
 *   ix_data:
 *     [0]                       disc = 8
 *     [1..32]                   key (32 bytes)
 *     [33]                      path_len
 *     [34..34+path_len)         sibling_sides[path_len]:
 *                                 byte per path level (0=root, last=leaf).
 *                                 0=no sibling, 1=right sibling, 2=left sibling
 *
 *   accounts:
 *     [0]                       header (writable)
 *     [1]                       payer (signer, writable; receives closed-account rent)
 *     [2..2+path_len)           path: root → leaf (writable)
 *     [2+path_len..]            siblings in level order — one entry per level
 *                                 whose sibling_sides byte is non-zero.
 *
 *   Cascade walks bottom-up: leaf → root.  At each level:
 *     - if key_count >= MIN: stop, no further cascade.
 *     - else: borrow if sibling has > MIN, else merge.
 *     - merge removes a separator from parent → parent may underflow → continue.
 *   If the root (level 0) ends up with 0 keys (single child remaining), the
 *   tree height shrinks by 1 and the lone child becomes the new root.
 */
static uint64_t do_delete(SolParameters *params) {
    /* Minimum ix_data size = 1 + KEY_SIZE + 1 (path_len) + 0 sibling bytes (path_len could be 1) */
    if (params->data_len < (uint64_t)(1 + KEY_SIZE + 1)) return ERR_BAD_IX_DATA;
    if (params->ka_num < 3) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    const uint8_t *key  = params->data + 1;
    uint8_t path_len    = params->data[1 + KEY_SIZE];
    if (path_len == 0 || path_len > MAX_TREE_HEIGHT) return ERR_BAD_PATH;
    if (params->data_len < (uint64_t)(1 + KEY_SIZE + 1 + path_len)) return ERR_BAD_IX_DATA;
    const uint8_t *sibling_sides = params->data + 1 + KEY_SIZE + 1;

    uint64_t err = check_header(&params->ka[0], params->program_id);
    if (err) return err;
    TreeHeader *th = (TreeHeader *)params->ka[0].data;
    if (path_len != th->height) return ERR_BAD_PATH;
    if (!params->ka[1].is_signer) return ERROR_MISSING_REQUIRED_SIGNATURES;
    if (!tx_has_authority_signer(params, th->authority)) return ERR_NOT_AUTHORIZED;
    uint16_t vs = th->value_size;

    /* Count expected siblings and verify ka_num. */
    int num_siblings = 0;
    for (int i = 0; i < path_len; i++) if (sibling_sides[i] != 0) num_siblings++;
    if (params->ka_num < (uint64_t)(2 + path_len + num_siblings)) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    uint32_t path_base = 2;
    uint32_t sibling_base = 2 + (uint32_t)path_len;
    SolAccountInfo *payer = &params->ka[1];

    /* Validate root. */
    err = check_node(&params->ka[path_base + 0], params->program_id, th->root_node_idx);
    if (err) return err;

    /* Descend, validate every step. */
    for (int level = 0; level < path_len - 1; level++) {
        SolAccountInfo *cur = &params->ka[path_base + level];
        NodeHeader *nh = node_hdr(cur->data);
        if (nh->is_leaf) return ERR_BAD_PATH;
        int pos = node_lower_bound(cur->data, key);
        uint32_t *kids = node_children(cur->data);
        uint32_t desc_idx;
        if (pos < nh->key_count && key_cmp(node_keys(cur->data) + (uint64_t)pos * KEY_SIZE, key) == 0) {
            desc_idx = kids[pos + 1];
        } else {
            desc_idx = kids[pos];
        }
        SolAccountInfo *nxt = &params->ka[path_base + level + 1];
        err = check_node(nxt, params->program_id, desc_idx);
        if (err) return err;
    }

    /* Delete key from leaf. */
    SolAccountInfo *leaf_acc = &params->ka[path_base + path_len - 1];
    NodeHeader *lh = node_hdr(leaf_acc->data);
    if (!lh->is_leaf) return ERR_BAD_PATH;
    uint8_t out_value[VAL_SIZE_MAX];
    err = leaf_delete(leaf_acc->data, key, out_value, vs);
    if (err) return err;
    if (th->total_entries > 0) th->total_entries--;

    /* Cascade rebalance: bottom-up. Walk siblings as we go.
     * sib_consumed counts how many sibling accounts (in account_infos order)
     * we've used so far; siblings are stored in PATH-LEVEL order from level 0
     * upward, skipping levels with no sibling. */
    int sib_consumed = 0;
    int sib_offsets[MAX_TREE_HEIGHT];
    {
        int o = 0;
        for (int i = 0; i < path_len; i++) {
            sib_offsets[i] = (sibling_sides[i] != 0) ? o++ : -1;
        }
    }

    for (int level = path_len - 1; level >= 1; level--) {
        SolAccountInfo *cur = &params->ka[path_base + level];
        NodeHeader *cur_nh = node_hdr(cur->data);

        /* No underflow? cascade stops. */
        if (cur_nh->key_count >= KEYS_PER_NODE_MIN) break;

        /* Sibling for this level (if provided). */
        if (sibling_sides[level] == 0) break; /* nothing we can do */
        SolAccountInfo *sib = &params->ka[sibling_base + sib_offsets[level]];
        NodeHeader *sib_nh = node_hdr(sib->data);
        sib_consumed++;

        SolAccountInfo *par = &params->ka[path_base + level - 1];
        NodeHeader *par_nh = node_hdr(par->data);

        /* Find cur's position in parent's children array. */
        int our_pos = -1;
        {
            uint32_t *par_kids = node_children(par->data);
            for (int i = 0; i <= par_nh->key_count; i++) {
                if (par_kids[i] == cur_nh->node_idx) { our_pos = i; break; }
            }
            if (our_pos < 0) return ERR_BAD_PATH;
        }

        if (cur_nh->is_leaf) {
            /* Leaf-level rebalance. */
            if (sibling_sides[level] == 1) {
                int sep_pos = our_pos;
                if (sib_nh->key_count > KEYS_PER_NODE_MIN) {
                    leaf_borrow_from_right(cur->data, sib->data, par->data, sep_pos, vs);
                } else {
                    err = leaf_merge(cur->data, sib->data, vs);
                    if (err) return err;
                    internal_remove_at(par->data, sep_pos);
                    close_account(sib, payer);
                }
            } else {
                int sep_pos = our_pos - 1;
                if (sib_nh->key_count > KEYS_PER_NODE_MIN) {
                    leaf_borrow_from_left(sib->data, cur->data, par->data, sep_pos, vs);
                } else {
                    err = leaf_merge(sib->data, cur->data, vs);
                    if (err) return err;
                    internal_remove_at(par->data, sep_pos);
                    if (th->leftmost_leaf_idx == cur_nh->node_idx) {
                        th->leftmost_leaf_idx = node_hdr(sib->data)->node_idx;
                    }
                    close_account(cur, payer);
                }
            }
        } else {
            /* Internal-level rebalance. */
            if (sibling_sides[level] == 1) {
                int sep_pos = our_pos;
                if (sib_nh->key_count > KEYS_PER_NODE_MIN) {
                    internal_borrow_from_right(cur->data, sib->data, par->data, sep_pos);
                } else {
                    err = internal_merge_right(cur->data, sib->data, par->data, sep_pos);
                    if (err) return err;
                    internal_remove_at(par->data, sep_pos);
                    close_account(sib, payer);
                }
            } else {
                int sep_pos = our_pos - 1;
                if (sib_nh->key_count > KEYS_PER_NODE_MIN) {
                    internal_borrow_from_left(sib->data, cur->data, par->data, sep_pos);
                } else {
                    err = internal_merge_right(sib->data, cur->data, par->data, sep_pos);
                    if (err) return err;
                    internal_remove_at(par->data, sep_pos);
                    close_account(cur, payer);
                }
            }
        }
    }

    /* Root collapse: if root is internal and now has 0 keys, promote its
     * single remaining child to root, shrink height by 1. */
    if (path_len > 1) {
        SolAccountInfo *root_acc = &params->ka[path_base + 0];
        NodeHeader *root_nh = node_hdr(root_acc->data);
        if (!root_nh->is_leaf && root_nh->key_count == 0) {
            uint32_t *kids = node_children(root_acc->data);
            th->root_node_idx = kids[0];
            th->height--;
            close_account(root_acc, payer);
            sol_log("torna: root collapsed, height shrunk");
        }
    }

    (void)sib_consumed;
    uint8_t ret[1 + VAL_SIZE_MAX];
    ret[0] = 1;
    sol_memcpy(&ret[1], out_value, vs);
    sol_set_return_data(ret, 1 + (uint64_t)vs);
    return SUCCESS;
}

/*
 * IX_BULK_INSERT_FAST — insert N keys into ONE leaf in a single tx.
 *
 *   Optimized for the common case: multiple keys all routing to the same
 *   leaf with slack capacity. Refuses if any insert would overflow.
 *   Caller must pre-sort keys ascending (this is also the canonical pattern
 *   used by DEX order placement: place N orders at adjacent price levels).
 *
 *   ix_data: [u8 disc=9][u8 path_len][u8 count][(key[32] + value[VAL_SIZE]) * count]
 *   accounts: same as IX_INSERT_FAST — header(ro), path(ro), leaf(w)
 *   ix_data max length ≈ 1232 tx-cap minus overhead → ~18 entries with 32+32.
 */
static uint64_t do_bulk_insert_fast(SolParameters *params) {
    if (params->data_len < 1 + 1 + 1) return ERR_BAD_IX_DATA;
    uint8_t path_len = params->data[1];
    uint8_t count    = params->data[2];
    if (count == 0) return SUCCESS;
    if (params->ka_num < (uint64_t)1 + path_len) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    SolAccountInfo *hdr_acc = &params->ka[0];
    if (hdr_acc->data_len < TREE_HEADER_SIZE) return ERR_NODE_TOO_SMALL;
    if (!SolPubkey_same(hdr_acc->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    const TreeHeader *th = (const TreeHeader *)hdr_acc->data;
    if (th->magic != TORNA_MAGIC) return ERR_BAD_MAGIC;
    if (!tx_has_authority_signer(params, th->authority)) return ERR_NOT_AUTHORIZED;
    if (path_len != th->height) return ERR_BAD_PATH;
    if (path_len == 0) return ERR_TREE_UNINIT;
    uint16_t vs = th->value_size;
    uint64_t entry_size = (uint64_t)KEY_SIZE + vs;
    uint64_t expected_data = (uint64_t)1 + 1 + 1 + (uint64_t)count * entry_size;
    if (params->data_len < expected_data) return ERR_BAD_IX_DATA;

    /* Descend with first key, validate path, but don't bother validating per-key. */
    const uint8_t *first_key = params->data + 3; /* + KEY_SIZE step per entry */
    {
        SolAccountInfo *root_acc = &params->ka[1];
        if (!SolPubkey_same(root_acc->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
        NodeHeader *rh = node_hdr(root_acc->data);
        if (!rh->initialized) return ERR_NODE_UNINIT;
        if (rh->node_idx != th->root_node_idx) return ERR_BAD_PATH;

        for (int level = 0; level < path_len - 1; level++) {
            SolAccountInfo *cur = &params->ka[1 + level];
            NodeHeader *nh = node_hdr(cur->data);
            if (nh->is_leaf) return ERR_BAD_PATH;
            int pos = node_lower_bound(cur->data, first_key);
            uint32_t *kids = node_children(cur->data);
            uint32_t desc_idx;
            if (pos < nh->key_count && key_cmp(node_keys(cur->data) + (uint64_t)pos * KEY_SIZE, first_key) == 0) {
                desc_idx = kids[pos + 1];
            } else {
                desc_idx = kids[pos];
            }
            SolAccountInfo *nxt = &params->ka[1 + level + 1];
            if (!SolPubkey_same(nxt->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
            NodeHeader *nxt_h = node_hdr(nxt->data);
            if (!nxt_h->initialized) return ERR_NODE_UNINIT;
            if (nxt_h->node_idx != desc_idx) return ERR_BAD_PATH;
        }
    }

    SolAccountInfo *leaf_acc = &params->ka[1 + path_len - 1];
    if (!leaf_acc->is_writable) return ERR_NOT_WRITABLE;
    NodeHeader *lh = node_hdr(leaf_acc->data);
    if (!lh->is_leaf) return ERR_BAD_PATH;

    /* Bulk insert: each entry calls leaf_insert. Refuse if any would overflow. */
    for (uint8_t i = 0; i < count; i++) {
        if (lh->key_count >= KEYS_PER_NODE_MAX) return ERR_NEED_SPLIT_SLOT;
        const uint8_t *k = params->data + 3 + (uint64_t)i * entry_size;
        const uint8_t *v = k + KEY_SIZE;
        /* Also verify the key routes to this leaf — first key set the path;
         * if a later key falls outside, the result would be wrong. */
        if (i > 0) {
            const uint8_t *prev_k = params->data + 3 + (uint64_t)(i - 1) * entry_size;
            if (key_cmp(prev_k, k) >= 0) return ERR_BAD_IX_DATA; /* not ascending */
        }
        uint64_t err = leaf_insert(leaf_acc->data, k, v, vs);
        if (err) return err;
    }
    return SUCCESS;
}

/*
 * IX_BULK_DELETE_FAST — delete N keys from one leaf in a single tx.
 *
 *   ix_data: [u8 disc=10][u8 path_len][u8 count][key[32] * count]
 *   accounts: same as IX_DELETE_FAST — header(ro), path(ro), leaf(w)
 *   Skips keys not found (silent). No rebalance.
 */
static uint64_t do_bulk_delete_fast(SolParameters *params) {
    if (params->data_len < 1 + 1 + 1) return ERR_BAD_IX_DATA;
    uint8_t path_len = params->data[1];
    uint8_t count    = params->data[2];
    if (count == 0) return SUCCESS;
    uint64_t expected_data = (uint64_t)1 + 1 + 1 + (uint64_t)count * KEY_SIZE;
    if (params->data_len < expected_data) return ERR_BAD_IX_DATA;
    if (params->ka_num < (uint64_t)1 + path_len) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    SolAccountInfo *hdr_acc = &params->ka[0];
    if (hdr_acc->data_len < TREE_HEADER_SIZE) return ERR_NODE_TOO_SMALL;
    if (!SolPubkey_same(hdr_acc->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    const TreeHeader *th = (const TreeHeader *)hdr_acc->data;
    if (th->magic != TORNA_MAGIC) return ERR_BAD_MAGIC;
    if (!tx_has_authority_signer(params, th->authority)) return ERR_NOT_AUTHORIZED;
    if (path_len != th->height) return ERR_BAD_PATH;
    if (path_len == 0) return ERR_TREE_UNINIT;

    /* Descend with first key. */
    const uint8_t *first_key = params->data + 3;
    {
        SolAccountInfo *root_acc = &params->ka[1];
        if (!SolPubkey_same(root_acc->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
        NodeHeader *rh = node_hdr(root_acc->data);
        if (!rh->initialized) return ERR_NODE_UNINIT;
        if (rh->node_idx != th->root_node_idx) return ERR_BAD_PATH;

        for (int level = 0; level < path_len - 1; level++) {
            SolAccountInfo *cur = &params->ka[1 + level];
            NodeHeader *nh = node_hdr(cur->data);
            if (nh->is_leaf) return ERR_BAD_PATH;
            int pos = node_lower_bound(cur->data, first_key);
            uint32_t *kids = node_children(cur->data);
            uint32_t desc_idx;
            if (pos < nh->key_count && key_cmp(node_keys(cur->data) + (uint64_t)pos * KEY_SIZE, first_key) == 0) {
                desc_idx = kids[pos + 1];
            } else {
                desc_idx = kids[pos];
            }
            SolAccountInfo *nxt = &params->ka[1 + level + 1];
            if (!SolPubkey_same(nxt->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
            NodeHeader *nxt_h = node_hdr(nxt->data);
            if (!nxt_h->initialized) return ERR_NODE_UNINIT;
            if (nxt_h->node_idx != desc_idx) return ERR_BAD_PATH;
        }
    }

    SolAccountInfo *leaf_acc = &params->ka[1 + path_len - 1];
    if (!leaf_acc->is_writable) return ERR_NOT_WRITABLE;
    NodeHeader *lh = node_hdr(leaf_acc->data);
    if (!lh->is_leaf) return ERR_BAD_PATH;

    uint16_t deleted = 0;
    uint16_t vs = th->value_size;
    for (uint8_t i = 0; i < count; i++) {
        const uint8_t *k = params->data + 3 + (uint64_t)i * KEY_SIZE;
        uint64_t err = leaf_delete(leaf_acc->data, k, NULL, vs);
        if (err == SUCCESS) deleted++;
        /* skip not-found silently */
    }

    /* Return count of actually-deleted entries. */
    uint8_t ret[2];
    ret[0] = (uint8_t)(deleted & 0xFF);
    ret[1] = (uint8_t)((deleted >> 8) & 0xFF);
    sol_set_return_data(ret, sizeof(ret));
    return SUCCESS;
}

/*
 * IX_TRANSFER_AUTHORITY — replace the tree's write-authority with a new pubkey.
 *   ix_data: [u8 disc=11][u8 new_authority[32]]
 *   accounts: [0]=header(w), [1]=current_authority(s,ro)
 */
static uint64_t do_transfer_authority(SolParameters *params) {
    if (params->data_len < 1 + 32) return ERR_BAD_IX_DATA;
    if (params->ka_num < 2) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    uint64_t err = check_header(&params->ka[0], params->program_id);
    if (err) return err;
    TreeHeader *th = (TreeHeader *)params->ka[0].data;
    if (!tx_has_authority_signer(params, th->authority)) return ERR_NOT_AUTHORIZED;

    const uint8_t *new_authority = params->data + 1;
    sol_memcpy(th->authority, new_authority, 32);
    sol_log("torna: authority transferred");
    return SUCCESS;
}

/*
 * IX_FIND
 *   ix_data: [u8 disc=3][u8 key[32]][u8 path_len]
 *   accounts: [0]=header(ro), [1..]=path from root to leaf
 *   return_data: [u8 found][u8 value[8]]
 */
static uint64_t do_find(SolParameters *params) {
    if (params->data_len < 1 + KEY_SIZE + 1) return ERR_BAD_IX_DATA;
    if (params->ka_num < 1) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    SolAccountInfo *hdr_acc = &params->ka[0];
    if (hdr_acc->data_len < TREE_HEADER_SIZE) return ERR_NODE_TOO_SMALL;
    if (!SolPubkey_same(hdr_acc->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    TreeHeader *th = (TreeHeader *)hdr_acc->data;
    if (th->magic != TORNA_MAGIC) return ERR_BAD_MAGIC;
    uint16_t vs = th->value_size;

    const uint8_t *key = params->data + 1;
    uint8_t path_len = params->data[1 + KEY_SIZE];

    uint8_t out[1 + VAL_SIZE_MAX];
    sol_memset(out, 0, 1 + (uint64_t)vs);

    if (th->height == 0) {
        sol_set_return_data(out, 1 + (uint64_t)vs);
        return SUCCESS;
    }
    if (path_len != th->height) return ERR_BAD_PATH;
    if (params->ka_num < (uint64_t)1 + path_len) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    /* Descend (read-only checks: program-owned, initialized, idx matches). */
    SolAccountInfo *root = &params->ka[1];
    if (root->data_len < NODE_ACCOUNT_DATA_SIZE) return ERR_NODE_TOO_SMALL;
    if (!SolPubkey_same(root->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    if (!node_hdr(root->data)->initialized) return ERR_NODE_UNINIT;
    if (node_hdr(root->data)->node_idx != th->root_node_idx) return ERR_BAD_PATH;

    for (int level = 0; level < path_len - 1; level++) {
        SolAccountInfo *cur = &params->ka[1 + level];
        NodeHeader *nh = node_hdr(cur->data);
        if (nh->is_leaf) return ERR_BAD_PATH;
        int pos = node_lower_bound(cur->data, key);
        uint32_t *kids = node_children(cur->data);
        uint32_t desc_idx;
        if (pos < nh->key_count && key_cmp(node_keys(cur->data) + (uint64_t)pos * KEY_SIZE, key) == 0) {
            desc_idx = kids[pos + 1];
        } else {
            desc_idx = kids[pos];
        }
        SolAccountInfo *nxt = &params->ka[1 + level + 1];
        if (!SolPubkey_same(nxt->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
        NodeHeader *nxt_h = node_hdr(nxt->data);
        if (!nxt_h->initialized) return ERR_NODE_UNINIT;
        if (nxt_h->node_idx != desc_idx) return ERR_BAD_PATH;
    }

    SolAccountInfo *leaf = &params->ka[1 + path_len - 1];
    NodeHeader *lh = node_hdr(leaf->data);
    if (!lh->is_leaf) return ERR_BAD_PATH;

    int pos = node_lower_bound(leaf->data, key);
    if (pos < lh->key_count && key_cmp(node_keys(leaf->data) + (uint64_t)pos * KEY_SIZE, key) == 0) {
        out[0] = 1;
        sol_memcpy(out + 1, node_values(leaf->data) + (uint64_t)pos * vs, vs);
    }
    sol_set_return_data(out, 1 + (uint64_t)vs);
    return SUCCESS;
}

/*
 * IX_RANGE_SCAN
 *   ix_data: [u8 disc=4][u8 start_key[32]][u8 end_key[32]][u8 start_leaf_path_len][u8 max_results]
 *   accounts: [0]=header, [1..start_leaf_path_len]=path to leaf containing start_key,
 *             [start_leaf_path_len+1..] = additional leaf nodes following the chain.
 *   return_data: [u16 count][ (key[32] || value[8]) * count ]
 *
 *   The program walks the leaf chain via next_leaf_idx, expecting the accounts
 *   to be provided in chain order. Stops at end_key or max_results or end-of-chain.
 */
static uint64_t do_range_scan(SolParameters *params) {
    if (params->data_len < 1 + 2 * KEY_SIZE + 2) return ERR_BAD_IX_DATA;
    if (params->ka_num < 1) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    SolAccountInfo *hdr_acc = &params->ka[0];
    if (hdr_acc->data_len < TREE_HEADER_SIZE) return ERR_NODE_TOO_SMALL;
    if (!SolPubkey_same(hdr_acc->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    TreeHeader *th = (TreeHeader *)hdr_acc->data;
    if (th->magic != TORNA_MAGIC) return ERR_BAD_MAGIC;
    uint16_t vs = th->value_size;
    uint64_t entry_size = (uint64_t)KEY_SIZE + vs;

    const uint8_t *start_key  = params->data + 1;
    const uint8_t *end_key    = params->data + 1 + KEY_SIZE;
    uint8_t start_path_len    = params->data[1 + 2 * KEY_SIZE];
    uint8_t max_results       = params->data[1 + 2 * KEY_SIZE + 1];
    if (max_results > MAX_RANGE_RESULTS) max_results = MAX_RANGE_RESULTS;

    /* Output buffer sized to compile-time worst case (KEY_SIZE + VAL_SIZE_MAX). */
    uint8_t out[2 + MAX_RANGE_RESULTS * (KEY_SIZE + VAL_SIZE_MAX)];
    sol_memset(out, 0, 2 + (uint64_t)max_results * entry_size);
    uint16_t count = 0;

    if (th->height == 0 || max_results == 0) {
        out[0] = 0; out[1] = 0;
        sol_set_return_data(out, 2);
        return SUCCESS;
    }
    if (start_path_len == 0 || start_path_len > th->height) return ERR_BAD_PATH;

    /* The path occupies ka[1..start_path_len]; the leaf at the end is the starting leaf.
       After that, additional leaves in the chain (if any) start at ka[1 + start_path_len]. */
    if (params->ka_num < (uint64_t)1 + start_path_len) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;

    SolAccountInfo *cur_leaf = &params->ka[1 + start_path_len - 1];
    if (!SolPubkey_same(cur_leaf->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    NodeHeader *cur_h = node_hdr(cur_leaf->data);
    if (!cur_h->initialized || !cur_h->is_leaf) return ERR_BAD_PATH;

    /* Start iteration at lower_bound(start_key) in current leaf. */
    int idx = node_lower_bound(cur_leaf->data, start_key);

    uint64_t extra_leaf_pos = 1 + start_path_len;
    while (count < max_results) {
        uint8_t *keys = node_keys(cur_leaf->data);
        uint8_t *vals = node_values(cur_leaf->data);
        for (; idx < cur_h->key_count && count < max_results; idx++) {
            const uint8_t *k = keys + (uint64_t)idx * KEY_SIZE;
            if (key_cmp(k, end_key) > 0) {
                /* past end */
                goto done;
            }
            sol_memcpy(out + 2 + (uint64_t)count * entry_size, k, KEY_SIZE);
            sol_memcpy(out + 2 + (uint64_t)count * entry_size + KEY_SIZE,
                       vals + (uint64_t)idx * vs, vs);
            count++;
        }
        if (count >= max_results) break;

        /* Advance to next leaf. */
        if (cur_h->next_leaf_idx == 0) break;
        if (extra_leaf_pos >= params->ka_num) break;
        SolAccountInfo *nxt = &params->ka[extra_leaf_pos++];
        if (!SolPubkey_same(nxt->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
        NodeHeader *nh = node_hdr(nxt->data);
        if (!nh->initialized || !nh->is_leaf) return ERR_BAD_PATH;
        if (nh->node_idx != cur_h->next_leaf_idx) return ERR_BAD_PATH;

        cur_leaf = nxt;
        cur_h = nh;
        idx = 0;
    }

done:
    out[0] = (uint8_t)(count & 0xFF);
    out[1] = (uint8_t)((count >> 8) & 0xFF);
    sol_set_return_data(out, 2 + (uint64_t)count * entry_size);
    return SUCCESS;
}

/*
 * IX_STATS
 *   ix_data: [u8 disc=5]
 *   accounts: [0]=header
 *   return_data: [TreeHeader]
 */
static uint64_t do_stats(SolParameters *params) {
    if (params->ka_num < 1) return ERROR_NOT_ENOUGH_ACCOUNT_KEYS;
    SolAccountInfo *hdr_acc = &params->ka[0];
    if (hdr_acc->data_len < TREE_HEADER_SIZE) return ERR_NODE_TOO_SMALL;
    if (!SolPubkey_same(hdr_acc->owner, params->program_id)) return ERROR_INCORRECT_PROGRAM_ID;
    TreeHeader *th = (TreeHeader *)hdr_acc->data;
    if (th->magic != TORNA_MAGIC) return ERR_BAD_MAGIC;
    sol_set_return_data(hdr_acc->data, TREE_HEADER_SIZE);
    return SUCCESS;
}

/* =========================================================================
 * Entrypoint
 * =======================================================================*/

/* Bound max accounts per ix; covers header + path + spares + chain.
 * Each SolAccountInfo is ~64 bytes; entrypoint stack frame must stay under
 * the SBF per-frame limit (4096 bytes), so this caps at 32. */
#define MAX_ACCOUNTS 32

extern uint64_t entrypoint(const uint8_t *input) {
    SolAccountInfo accounts[MAX_ACCOUNTS];
    SolParameters params = (SolParameters){ .ka = accounts };

    if (!sol_deserialize(input, &params, MAX_ACCOUNTS)) {
        return ERROR_INVALID_ARGUMENT;
    }
    if (params.data_len < 1) return ERR_BAD_IX_DATA;

    uint8_t disc = params.data[0];
    switch (disc) {
        case IX_INIT_TREE:    return do_init_tree(&params);
        case IX_INSERT:       return do_insert(&params);
        case IX_FIND:         return do_find(&params);
        case IX_RANGE_SCAN:   return do_range_scan(&params);
        case IX_STATS:        return do_stats(&params);
        case IX_INSERT_FAST:       return do_insert_fast(&params);
        case IX_DELETE_FAST:       return do_delete_fast(&params);
        case IX_DELETE:            return do_delete(&params);
        case IX_BULK_INSERT_FAST:    return do_bulk_insert_fast(&params);
        case IX_BULK_DELETE_FAST:    return do_bulk_delete_fast(&params);
        case IX_TRANSFER_AUTHORITY:  return do_transfer_authority(&params);
        default: return ERR_BAD_IX_DATA;
    }
}
