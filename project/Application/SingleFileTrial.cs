using S3SyncLab.Configuration;
using S3SyncLab.Core;
using S3SyncLab.Infrastructure;

namespace S3SyncLab.Application;

public sealed class SingleFileTrial
{
    private readonly LabOptions options;
    private readonly IVersionHistory history;
    private readonly ISnapshotPolicy policy;
    private readonly IDeltaCodec delta;
    private readonly IObjectStore store;
    private readonly IPacketWriter packetWriter;
    private readonly RestoreEngine restore;
    private readonly string runDirectory;
    private readonly string objectPrefix;
    private readonly Guid group;
    private readonly IDictionary<int, FileFingerprint> referenceVersions;
    private readonly IColdReadSchedule? readSchedule;
    private readonly IColdReadObserver? readObserver;

    public SingleFileTrial(
        LabOptions options,
        IVersionHistory history,
        ISnapshotPolicy policy,
        IDeltaCodec delta,
        IObjectStore store,
        IPacketWriter packetWriter,
        RestoreEngine restore,
        string runDirectory,
        string objectPrefix,
        Guid group,
        IDictionary<int, FileFingerprint> referenceVersions,
        IColdReadSchedule? readSchedule = null,
        IColdReadObserver? readObserver = null)
    {
        this.options = options;
        this.history = history;
        this.policy = policy;
        this.delta = delta;
        this.store = store;
        this.packetWriter = packetWriter;
        this.restore = restore;
        this.runDirectory = runDirectory;
        this.objectPrefix = objectPrefix;
        this.group = group;
        this.referenceVersions = referenceVersions;
        this.readSchedule = readSchedule;
        this.readObserver = readObserver;
    }

    public async Task<RunSummary> RunAsync(int repetition, CancellationToken cancellationToken)
    {
        var info = await history.InitializeAsync(cancellationToken);
        FileOperations.WriteJsonAtomic(Path.Combine(runDirectory, "dataset.json"), info);

        string work = Path.Combine(runDirectory, "scratch");
        Directory.CreateDirectory(work);

        string previousFile = Path.Combine(work, "previous.sqlite");
        string deltaFile = Path.Combine(work, "update.delta");
        string packetFile = Path.Combine(work, "packet.bin");
        string headFile = Path.Combine(work, "head.json");
        string restoredFile = Path.Combine(work, "restored.sqlite");
        string headKey = objectPrefix + "/HEAD.json";

        var network = new TokenBucketModel(
            options.Network.CapacityBytes,
            options.Network.RefillBytesPerSecond,
            options.Network.PeakBytesPerSecond
        );

        var observed = options.Network.DecisionBudgetMode == "wall-clock-estimate"
            ? new ObservedTokenBudget(options.Network.CapacityBytes, options.Network.RefillBytesPerSecond)
            : null;

        var writes = new List<WriteMeasurement>();
        var reads = new List<ReadMeasurement>();
        var chain = new List<ChainObject>();
        var chainState = ChainState.Empty;

        string previousRoot = new('0', 96);
        FileFingerprint previous = new(0, new string('0', 64));

        using var journal = new ObjectJournal(Path.Combine(runDirectory, "object-keys.txt"));
        journal.Record(headKey);

        using var writeCsv = new CsvLog(
            Path.Combine(runDirectory, "writes.csv"),
            "version,kind,reason,threshold,decision_tokens_bytes,full_candidate_bytes,delta_candidate_bytes,packet_bytes,head_bytes," +
            "chain_bytes,delta_count,prepare_ms,fingerprint_ms,delta_ms,protection_ms,packet_put_ms,head_put_ms,write_ms," +
            "maintenance_ms,model_transfer_seconds,model_tokens_after_bytes,decision_budget_tokens_after_bytes,budget_excess_bytes,target_sha256"
        );

        using var readCsv = new CsvLog(
            Path.Combine(runDirectory, "reads.csv"),
            "version,chain_bytes,delta_count,download_body_bytes,get_count,download_ms,verify_decrypt_ms,apply_ms,total_ms,target_sha256"
        );

        try
        {
            for (int version = 0; version <= options.Updates; version++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var timer = Stopwatch.StartNew();

                if (version > 0)
                {
                    await history.AdvanceAsync(version, cancellationToken);
                }

                var preparation = history as IPreparedVersionMetrics;
                double prepareMs = preparation?.DataGenerationMs ?? timer.Elapsed.TotalMilliseconds;

                if (version > 0)
                {
                    network.Idle(options.Network.IdleSecondsPerUpdate);

                    if (observed is not null && options.Network.IdleSecondsPerUpdate > 0)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(options.Network.IdleSecondsPerUpdate), cancellationToken);
                    }
                }

                var service = Stopwatch.StartNew();
                timer.Restart();

                FileFingerprint target = FileOperations.Fingerprint(history.CurrentFile, cancellationToken);
                double fingerprintMs = timer.Elapsed.TotalMilliseconds;

                if (referenceVersions.TryGetValue(version, out var expected)
                    && target != expected)
                {
                    throw new InvalidDataException("Policies received different bytes for version " + version + ".");
                }

                referenceVersions[version] = target;

                double deltaMs = 0;
                long fullBytes = packetWriter.Measure(target.Length, PacketKind.Full);
                long deltaBytes = 0;

                if (version > 0)
                {
                    timer.Restart();
                    delta.Create(previousFile, history.CurrentFile, deltaFile, cancellationToken);
                    deltaMs = timer.Elapsed.TotalMilliseconds;
                    deltaBytes = packetWriter.Measure(new FileInfo(deltaFile).Length, PacketKind.Delta);
                }

                double decisionTokens = observed?.Balance ?? network.Tokens;

                var decision = policy.Decide(new DecisionInput(
                    fullBytes,
                    deltaBytes,
                    chainState,
                    decisionTokens,
                    network.Capacity,
                    options.MaximumDeltas
                ));

                string payload = decision.Kind == PacketKind.Full ? history.CurrentFile : deltaFile;
                var basis = decision.Kind == PacketKind.Full ? new FileFingerprint(0, new string('0', 64)) : previous;

                timer.Restart();

                var packet = packetWriter.Protect(
                    new PacketRequest(
                        version,
                        decision.Kind,
                        previousRoot,
                        target,
                        basis,
                        payload
                    ),
                    packetFile,
                    cancellationToken
                );

                double protectionMs = timer.Elapsed.TotalMilliseconds;

                long candidate = decision.Kind == PacketKind.Full ? fullBytes : deltaBytes;

                if (packet.Bytes != candidate)
                {
                    throw new InvalidDataException("Policy candidate size differs from actual packet size.");
                }

                string objectKey = objectPrefix + "/objects/" + packet.Root + ".bin";

                // Record before PUT so interrupted uploads remain discoverable for cleanup.
                journal.Record(objectKey);

                observed?.StartTransfer();
                timer.Restart();
                await store.PutFileAsync(objectKey, packetFile, cancellationToken);
                double packetPutMs = timer.Elapsed.TotalMilliseconds;
                double budgetExcess = observed?.CompleteTransfer(packet.Bytes) ?? 0;

                var obsolete = decision.Kind == PacketKind.Full ? chain.ToArray() : Array.Empty<ChainObject>();

                if (decision.Kind == PacketKind.Full)
                {
                    chain.Clear();
                }

                chain.Add(new ChainObject(objectKey, packet.Root, packet.Bytes, version, decision.Kind));
                chainState = chainState.Add(decision.Kind, packet.Bytes);

                var head = new HeadDocument(
                    1,
                    group.ToString("N"),
                    version,
                    packet.Root,
                    chain.ToArray()
                );

                FileOperations.WriteJsonAtomic(headFile, head);
                long headBytes = new FileInfo(headFile).Length;

                timer.Restart();

                // The only mutable object is published after its immutable packet has uploaded successfully.
                observed?.StartTransfer();
                await store.PutFileAsync(headKey, headFile, cancellationToken);
                double headPutMs = timer.Elapsed.TotalMilliseconds;
                budgetExcess += observed?.CompleteTransfer(headBytes) ?? 0;

                FileOperations.WriteJsonAtomic(
                    Path.Combine(runDirectory, "run.json"),
                    new RunCheckpoint(
                        1,
                        "running",
                        group.ToString("N"),
                        objectPrefix,
                        headKey,
                        version,
                        packet.Root,
                        target,
                        head
                    )
                );

                // Packaging belongs to client work; synthetic SQL/file generation does not.
                double writeMs = service.Elapsed.TotalMilliseconds + (preparation?.ClientPreparationMs ?? 0);
                double modelSeconds = network.Transfer(packet.Bytes).Seconds + network.Transfer(headBytes).Seconds;
                double decisionBudgetAfter = observed?.Balance ?? network.Tokens;

                if (budgetExcess > 1)
                {
                    Console.WriteLine("Budget estimate exceeded: check external shaping and byte-accounting parameters.");
                }

                bool shouldRead = readSchedule?.ShouldRead(version, options.Updates)
                    ?? (version == 0
                        || version % options.RestoreEvery == 0
                        || version == options.Updates);

                if (shouldRead)
                {
                    var result = await restore.RestoreAsync(
                        headKey,
                        packet.Root,
                        restoredFile,
                        work,
                        cancellationToken
                    );

                    if (result.File != target)
                    {
                        throw new InvalidDataException("Cold restoration differs from the source version.");
                    }

                    if (readObserver is not null)
                    {
                        await readObserver.AfterRestoreAsync(version, restoredFile, result, cancellationToken);
                    }

                    reads.Add(new ReadMeasurement(version, chainState.Bytes, chainState.DeltaCount, result));

                    readCsv.Append(
                        version,
                        chainState.Bytes,
                        chainState.DeltaCount,
                        result.DownloadBytes,
                        result.ObjectCount,
                        result.DownloadMs,
                        result.VerifyDecryptMs,
                        result.ApplyMs,
                        result.TotalMs,
                        result.File.Sha256
                    );

                    FileOperations.DeleteIfExists(restoredFile);
                }

                timer.Restart();

                if (options.PruneReplacedChains)
                {
                    foreach (var item in obsolete)
                    {
                        await store.DeleteAsync(item.Key, cancellationToken);
                    }
                }

                if (version < options.Updates)
                {
                    await FileOperations.CopyAtomicAsync(history.CurrentFile, previousFile, cancellationToken);
                }

                FileOperations.DeleteIfExists(deltaFile);
                FileOperations.DeleteIfExists(packetFile);

                double maintenanceMs = timer.Elapsed.TotalMilliseconds;

                var row = new WriteMeasurement(
                    version,
                    decision.Kind.ToString(),
                    decision.Reason,
                    decision.Threshold,
                    decisionTokens,
                    fullBytes,
                    deltaBytes,
                    packet.Bytes,
                    headBytes,
                    chainState.Bytes,
                    chainState.DeltaCount,
                    prepareMs,
                    fingerprintMs,
                    deltaMs,
                    protectionMs,
                    packetPutMs,
                    headPutMs,
                    writeMs,
                    maintenanceMs,
                    modelSeconds,
                    network.Tokens,
                    decisionBudgetAfter,
                    budgetExcess,
                    target.Sha256
                );

                writes.Add(row);

                writeCsv.Append(
                    row.Version,
                    row.Kind,
                    row.Reason,
                    row.Threshold,
                    row.DecisionTokens,
                    row.FullCandidateBytes,
                    row.DeltaCandidateBytes,
                    row.PacketBytes,
                    row.HeadBytes,
                    row.ChainBytes,
                    row.DeltaCount,
                    row.PrepareMs,
                    row.FingerprintMs,
                    row.DeltaMs,
                    row.ProtectionMs,
                    row.PacketPutMs,
                    row.HeadPutMs,
                    row.WriteMs,
                    row.MaintenanceMs,
                    row.ModelTransferSeconds,
                    row.ModelTokensAfter,
                    row.DecisionBudgetTokensAfter,
                    row.BudgetExcessBytes,
                    row.TargetSha256
                );

                previous = target;
                previousRoot = packet.Root;

                if (version == 0
                    || version % 10 == 0
                    || version == options.Updates)
                {
                    Console.WriteLine(
                        $"{policy.Name}, repetition {repetition}: {version}/{options.Updates}, {decision.Kind}, chain={chainState.DeltaCount}"
                    );
                }
            }

            var summary = MeasurementSummary.Create(
                Path.GetFileName(runDirectory),
                policy.Name,
                repetition,
                writes,
                reads,
                previous
            );

            FileOperations.WriteJsonAtomic(Path.Combine(runDirectory, "summary.json"), summary);

            var checkpoint = FileOperations.ReadJson<RunCheckpoint>(Path.Combine(runDirectory, "run.json"));

            FileOperations.WriteJsonAtomic(
                Path.Combine(runDirectory, "run.json"),
                checkpoint with { Status = "completed" }
            );

            return summary;
        }
        finally
        {
            // Only the private trial workspace is removed. Source data and logs are retained.
            if (Directory.Exists(work))
            {
                Directory.Delete(work, recursive: true);
            }
        }
    }
}