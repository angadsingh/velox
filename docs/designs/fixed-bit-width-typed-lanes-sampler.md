# FixedBitWidth Typed-Lanes Savings Sampler

## Goal

Measure, on production writes, how many bytes FixedBitWidth typed lanes save
compared with stock FixedBitWidth, without re-encoding terabytes offline.

Some FixedBitWidth chunks take typed lanes. The writer shadow-encodes a sample
of those chunks the stock way as well, compresses the stock payload with the
same policy, records both sizes and throws the stock bytes away. The file is
unchanged. The writer publishes the sums as runtime stats, and aggregating them
across tasks gives the fleet number.

The sampler deliberately favors the chunks where lanes help most: it samples
them at a much higher rate. Every sample records the probability it was drawn
with, so the estimator corrects for that tilt (see
[Where the bias goes](#where-the-bias-goes)). The published totals therefore
stay unbiased, the high-gain strata get tight error bars from few samples, and
the sampler still catches regressions in the strata it under-samples.

## Non-goals

- Reader CPU. The sampler measures stored bytes and writer CPU only.
- Selection effects. The sampler measures the same chunk encoded both ways,
  given that FixedBitWidth was selected. With `fixedBitWidthUseExactBits` off,
  selection is identical with lanes on or off (D1), so nothing is missed. With
  it on, D5 moves some chunks (32-bit types needing 17–19 bits, 64-bit types
  needing 33–39 bits) from FixedBitWidth to Trivial. Those chunks never reach
  the sampler; see [Limitations](#limitations).

## What "stock" means for a chunk

For a chunk where `typedLaneBits()` returns `laneBits > 0`, the shadow encode
is the payload the same chunk gets with `fixedBitWidthTypedLanes` off:

| Field | Typed lanes (written) | Stock (shadow) |
|---|---|---|
| bit width | `laneBits` (16, 32 or type width) | `storedBitsRequired` (byte-rounded, or exact if `fixedBitWidthUseExactBits`) |
| baseline | chunk min (narrow) or 0 (full width) | chunk min |
| payload size | `FixedBitWidthTypedLanes::payloadSize()` | `FixedBitArray::bufferSize()` |
| payload `DataType` | `Uint16` / `Uint32` / `TypeTraits<T>::dataType` | `Undefined` |
| MetaInternal level | `fixedBitWidthTypedLanesOpenzlLevel` if set | the policy's level |

The encoding prefix and the FixedBitWidth prefix (baseline and bit-width byte)
are the same size in both, so the delta is the difference in compressed
payload sizes. "Compressed" includes the policy declining via
`shouldAccept()`. In that case the size is the uncompressed payload, exactly as
`CompressionEncoder` stores it.

Nested FixedBitWidth chunks (dictionary indices, RLE run lengths, a Nullable's
values, PFOR children) embed their bytes in the parent unchanged. Their delta
therefore adds directly to the file's delta, and they are sampled like any
other chunk.

## Where it runs

All the work happens inside `FixedBitWidthEncoding<T>::encode()`, after
`laneBits` is computed and only when `laneBits > 0`. Chunks that take no lanes
are byte-identical with the option on or off, so their delta is known to be
zero. Skipping them costs nothing and biases nothing.

```
encode(selection, values, buffer, options)
  laneBits = typedLaneBits(...)
  ... build CompressionEncoder for the lane payload (unchanged) ...
  if (laneBits > 0 && options.typedLanesSampler != nullptr) {
    decision = sampler.decide(chunkKey, stratum)       // cheap, no allocation
    if (decision.sampled) {
      stockSize = compressedStockPayloadSize(...)      // shadow encode
      sampler.record(stratum, decision.weight,
                     laneSize = compressionEncoder.getSize(),
                     stockSize, laneNanos, stockNanos)
    }
  }
```

To guarantee that the shadow encode is exactly the stock path, `encode()`
builds the payload in one helper: `encodePayload(laneBits, baseline,
payloadDataType, typedLanesLevel)`. The written path calls it with the lane
parameters. The shadow calls it with the parameters `typedLaneBits() == 0`
yields: `storedBitsRequired`, the chunk min, `Undefined`, no level. The shadow
keeps only `getSize()` and frees the buffer from the encoding pool before
returning.

### Plumbing

- `Encoding::Options` gets `TypedLanesSampler* typedLanesSampler{nullptr}`.
  The writer owns the sampler for its lifetime, and
  `WriterOptions::buildEncodingOptions()` sets the pointer. Producers that
  build their own `Encoding::Options` (SubIntSplit sections, tablet metadata,
  the cluster index, statistics) leave it null. They already keep lanes off,
  so they have nothing to sample.
- Encoding runs in parallel on `encodingExecutor`. The sampler's accumulators
  are therefore per-thread: a `folly::ThreadLocal` array of plain counters per
  stratum, merged when the writer reports `runtimeStats()`. `decide()` reads
  only immutable config and the chunk's own key, so it needs no lock.
- New options, off by default and following the existing SerDe pattern:
  - `WriterOptions::fixedBitWidthTypedLanesSampling` (bool), key
    `nimble.fixedbitwidth.typed_lanes_sampling`.
  - `WriterOptions::fixedBitWidthTypedLanesSampleBudget` (fraction of lane
    payload bytes to shadow-encode, default 0.01), key
    `nimble.fixedbitwidth.typed_lanes_sample_budget`.

  With sampling off, the encoder does exactly what it does today. With it on,
  written bytes are still unchanged. Only CPU and the published stats change.

## Sampling strategy

### Strata

Each lane chunk falls into one stratum, computed from values `encode()`
already has:

1. **Lane kind**: narrow16, narrow32, full16, full32 or full64.
2. **Stock width alignment**: whether stock `storedBitsRequired` is a typed
   width (16, 32, 64) or not (8·k for the other widths, and exact widths).
3. **Size bucket**: `floor(log2(lane payload bytes))`, clamped to
   [9, 20]. The floor is 256 values, so the smallest lane payload is 520
   bytes.

That gives 5 × 2 × 12 = 120 strata, each with a fixed array of counters.

### Rates: biased toward the improvement

A sample's information about total bytes saved scales with its size, and the
gain concentrates where stock hands a typed compressor bytes it can't model:
unaligned widths (24, 40, 48, 56 and exact widths) and narrow lanes. Rates
follow that:

| Stratum | Base rate | Why |
|---|---|---|
| unaligned stock width, narrow lanes | 1/4 | the main win: byte-packed 24/40/48/56-bit values becoming u16/u32 lanes |
| unaligned stock width, full-width lanes | 1/8 | 40–56-bit values in u64 lanes |
| aligned stock width, narrow lanes | 1/32 | same width, typed versus untyped label only |
| aligned stock width, full-width lanes | 1/64 | stock and lane widths match; lanes skip the rebase (D2). Small gain, possible regression |

The size bucket multiplies the base rate by `2^(bucket − 13)`, clamped to
[1/1024, 1]. Chunks around 8 KiB keep the base rate, larger chunks are sampled
more, and smaller ones less. The writer then scales every rate by one global
power of two so the expected shadow bytes stay within
`fixedBitWidthTypedLanesSampleBudget`. That scale is set per writer from the
first stripe's lane bytes and never changes mid-file.

Every rate is a power of two, `π = 2^-k`, so each sample's weight `1/π = 2^k`
is an exact integer. Weighted sums then fit `velox::RuntimeMetric`'s integer
`sum` with no rounding.

**Floor.** No stratum's rate goes below 1/1024. A stratum with rate zero
contributes nothing to the estimate, which reads as "zero delta", and would
hide a regression (for example, full-width lanes compressing worse than the
rebased stock payload on some column). The floor keeps every stratum
measurable at negligible cost.

### Deterministic draw

`decide()` hashes `(writerSeed, rowCount, min, max, values.front(),
values.back(), laneBits)` with `folly::hash` and samples the chunk when the
low `k` bits of the hash are zero. Benefits:

- **No shared RNG state.** Parallel encoding threads need no coordination, and
  the decision doesn't depend on thread order.
- **Reproducible.** Tests can predict which chunks are sampled.
- **Varies by file.** `writerSeed` is drawn once per writer, so identical
  chunks in different files aren't all in or all out together.

The hash depends only on the chunk, never on its compressed outcome. Each `π`
is therefore fixed before the sample is measured, which the estimator below
requires.

## Where the bias goes

The sampler oversamples high-gain strata by up to 256× relative to the
aligned full-width strata. Reporting the raw average delta over samples would
therefore overstate fleet savings by roughly that tilt. The estimator undoes
it with Horvitz–Thompson weights:

- Estimated total stock bytes: `Ŝ = Σ stockSize_i / π_i`.
- Estimated total lane bytes: `L̂ = Σ laneSize_i / π_i`.
- Estimated saving ratio: `1 − L̂ / Ŝ`. This is a ratio estimator, which is
  consistent and has much lower variance than either total alone.

Applied to absolute bytes, the ratio is anchored to the exact lane bytes
written: the sampler also counts `laneSize` for **every** lane chunk,
unsampled, at the cost of one addition. Estimated stock bytes for all lane
chunks are then `L_exact · Ŝ / L̂`.

The bias pays off in variance: per-stratum error bars where the improvement
lives are 4–16× tighter than uniform sampling gives for the same CPU budget.
It also lets the report show the favorable view directly, as long as it is
labeled with its scope. For example: "on chunks with unaligned stock widths,
lanes save X% of their bytes", next to "fleet-wide, lanes save Y% of
FixedBitWidth lane-chunk bytes and Z% of file bytes".

Do not publish the unweighted sample mean as a fleet number. It measures the
sampler's preferences, not the fleet.

### Variance and confidence intervals

Per stratum, the sampler also publishes `Σ (1 − π_i) / π_i² · d_i²`, where
`d_i = stockSize_i − laneSize_i`. This is the Poisson-sampling variance
estimate for the total delta. With `π = 2^-k`, the factor `(1 − π)/π² =
2^k(2^k − 1)` reaches about 2^20 at the floor rate, and `d_i²` reaches 2^40
for a 1 MiB delta. The product can therefore overflow `int64`, so the
per-thread accumulators sum this term in `double`, and the writer publishes it
rounded. A variance needs no exact integer arithmetic. The aggregation query
sums it across tasks and reports `Δ̂ ± 1.96·sqrt(Var̂)`.

## Published stats

The new `Writer::RuntimeStats` keys are listed below. The per-stratum ones
take a `.s<id>` suffix, and their stratum table lives in `TypedLanesSampler.h`.

| Key | Meaning |
|---|---|
| `nimble.typedLanes.chunks` | lane chunks seen (all, unsampled) |
| `nimble.typedLanes.laneBytes` | exact compressed lane payload bytes (all) |
| `nimble.typedLanes.sampledChunks` | chunks shadow-encoded |
| `nimble.typedLanes.weightedLaneBytes` | `Σ laneSize/π` |
| `nimble.typedLanes.weightedStockBytes` | `Σ stockSize/π` |
| `nimble.typedLanes.deltaVariance` | variance term above |
| `nimble.typedLanes.shadowCpuNanos` | CPU spent in shadow encodes |
| `nimble.typedLanes.weightedLaneCompressNanos` / `weightedStockCompressNanos` | weighted compression CPU of both, for a CPU-cost estimate alongside the byte estimate |

These flow through `reportRuntimeStats()` like the existing counters, so they
reach task stats without new infrastructure. The fleet number is a `SUM` of
each key across tasks, followed by the ratio.

## Cost

A sampled chunk pays one extra `bulkSetWithBaseline` (cheap) and one extra
compression of the stock payload, the dominant cost and roughly equal to the
lane compression already done. At the default 1% byte budget, shadow CPU is
about 1% of FixedBitWidth lane-chunk compression CPU, and a small fraction of
that relative to the writer total. `shadowCpuNanos` measures this directly, so
the budget can be tuned from data.

Memory: one stock payload buffer at a time per encoding thread, freed before
`encode()` returns.

## Testing

- **Unchanged output.** Extend `OptionsOffGoldenTest`: every fixture written
  with sampling on, and with lanes both on and off, matches the file written
  with sampling off. This is the key guarantee: the sampler never changes
  stored bytes.
- **Exact stock equivalence.** In `FixedBitWidthTypedLanesTest`, at rate 1,
  the shadow `stockSize` of every lane chunk equals the payload size the same
  chunk compresses to with `fixedBitWidthTypedLanes` off. The test covers
  narrow and full-width lanes, exact bits on and off, and a MetaInternal level
  set and unset.
- **Estimator.** Using a synthetic population with known per-chunk deltas,
  biased rates and many seeds, the mean of the estimated ratio matches the
  true ratio within its reported interval, while the unweighted sample mean
  does not.
- **Determinism.** The same input and seed sample the same chunks with 1 and
  8 encoding threads.
- **Floor.** A stratum made of only regressing chunks (lane larger than stock)
  shows a negative delta at the floor rate.

## Limitations

- **Selection changes with exact bits on.** Chunks that D5 moves from
  FixedBitWidth to Trivial don't reach the sampler. For writers with
  `fixedBitWidthUseExactBits` on, measure them separately: in the selection
  policy, sample chunks whose chosen encoding differs from the one the lanes-off
  estimate would choose, and record both encodings' sizes there. This is a
  follow-up with its own hook outside `FixedBitWidthEncoding`.
- **Chunk-level stream compression.** If `WriterOptions::chunkCompression` is
  Zstd or Lz4, `ChunkedStreamWriter` compresses the encoded chunk again, so
  on-disk deltas differ from encoding-level deltas. The default is
  Uncompressed. For tables that set it, either report encoding-level deltas
  only or extend the shadow through chunk compression, which costs much more.
- **Stream deduplication.** The tablet writer deduplicates identical streams
  (`nimble.duplicateStreamBytes`). A sampled chunk in a deduplicated stream
  counts toward the delta although its bytes are written once. This effect
  should be small, and it applies to both sides equally.
