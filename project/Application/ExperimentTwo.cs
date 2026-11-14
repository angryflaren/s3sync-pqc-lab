using System.Reflection;
using System.Runtime.InteropServices;
using S3SyncLab.Configuration;
using S3SyncLab.Core;
using S3SyncLab.Infrastructure;
using S3SyncLab.Infrastructure.Corpus;
using S3SyncLab.Infrastructure.Crypto;
using S3SyncLab.Infrastructure.Delta;
using S3SyncLab.Infrastructure.History;

namespace S3SyncLab.Application;

public sealed record CorpusRunSummary(
    string Run,
    string Group,
    string ReadProfile,
    RunSummary Synchronization,
    double GroupReadP50Ms,
    double GroupReadP95Ms,
    long TotalReadBytes,
    int TotalGetCount,
    double TotalDataGenerationMs,
    double TotalPackagingMs
);

public sealed class ExperimentTwo(
    ExperimentTwoOptions options,
    CorpusCatalog catalog,
    IObjectStore store) : IExperiment
{
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(options.OutputDirectory);

        using var processLock = new FileStream(
            Path.Combine(options.OutputDirectory, "experiment.lock"),
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None
        );

        string batch = "experiment2-" 
            + DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ", System.Globalization.CultureInfo.InvariantCulture)
            + "-" 
            + Guid.NewGuid().ToString("N")[..8];

        string directory = Path.Combine(options.OutputDirectory, batch);
        Directory.CreateDirectory(directory);

        FileOperations.WriteJsonAtomic(Path.Combine(directory, "configuration.json"), options);
        FileOperations.WriteJsonAtomic(Path.Combine(directory, "input-validation.json"), catalog.Report);
        FileOperations.WriteJsonAtomic(Path.Combine(directory, "environment.json"), new
        {
            format = 2,
            startedUtc = DateTimeOffset.UtcNow,
            framework = RuntimeInformation.FrameworkDescription,
            os = RuntimeInformation.OSDescription,
            architecture = RuntimeInformation.ProcessArchitecture.ToString(),
            processors = Environment.ProcessorCount,
            storage = store.Description,
            dependencies = new[]
            {
                typeof(Org.BouncyCastle.Security.SecureRandom).Assembly,
                typeof(Octodiff.Core.SignatureBuilder).Assembly,
                typeof(Amazon.S3.AmazonS3Client).Assembly
            }
            .Select(assembly => new
            {
                name = assembly.GetName().Name,
                version = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            })
            .ToArray(),
            decisionBudgetMode = options.Network.DecisionBudgetMode,
            modelDefinition = "Virtual network-only body-byte model; separate from measured PUT/GET wall time.",
            readDefinition = "Entire group, new reader and no local file; OS and SeaweedFS caches are retained.",
            bundleDefinition = "S3GRP002, sorted paths, entry types, lengths, SHA-256, relative symbolic links and executable flag; no compression or timestamps."
        });

        var policies = new ISnapshotPolicy[]
        {
            new FixedThresholdPolicy(2),
            new FixedThresholdPolicy(3),
            new FixedThresholdPolicy(4),
            new AdaptiveThresholdPolicy(options.MinimumThreshold, options.MaximumThreshold)
        };

        var referenceVersions = options.Groups.ToDictionary(
            name => name,
            _ => new Dictionary<int, FileFingerprint>()
        );

        var summaries = new List<CorpusRunSummary>();
        var codec = new OctodiffCodec(options.ChunkBytes);

        Console.WriteLine("Experiment 2 results: " + directory);

        try
        {
            for (int repetition = 1; repetition <= options.Repetitions; repetition++)
            {
                foreach (string groupName in options.Groups)
                {
                    var group = catalog.GetGroup(groupName);
                    int updates = Math.Min(options.UpdateLimit ?? group.Updates, group.Updates);

                    foreach (string profile in options.ReadProfiles)
                    {
                        var schedule = groupName == "natural_zstd"
                            ? CorpusReadSchedule.Periodic(group.Updates, profile == "read_heavy" ? 1 : 20)
                            : CorpusReadSchedule.Load(
                                Path.Combine(options.DatasetDirectory, "history", groupName + "." + profile + ".jsonl"),
                                groupName,
                                group.Updates
                            );

                        for (int offset = 0; offset < policies.Length; offset++)
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            if (options.InterTrialIdleSeconds > 0)
                            {
                                await Task.Delay(TimeSpan.FromSeconds(options.InterTrialIdleSeconds), cancellationToken);
                            }

                            var policy = policies[(repetition - 1 + offset) % policies.Length];
                            string name = $"r{repetition:D2}-{groupName}-{profile}-{policy.Name}";
                            string run = Path.Combine(directory, name);
                            Directory.CreateDirectory(run);

                            using var runLock = new FileStream(
                                Path.Combine(run, "run.lock"),
                                FileMode.OpenOrCreate,
                                FileAccess.ReadWrite,
                                FileShare.None
                            );

                            string historyWork = Path.Combine(run, "history-work");
                            string prefix = options.Storage.Prefix + "/" + batch + "/" + name;
                            Guid groupId = Guid.NewGuid();

                            FileOperations.WriteJsonAtomic(
                                Path.Combine(run, "namespace.json"),
                                new RunIdentity(1, groupId.ToString("N"), prefix, prefix + "/HEAD.json")
                            );

                            FileOperations.WriteJsonAtomic(Path.Combine(run, "group.json"), new
                            {
                                group = groupName,
                                readProfile = profile,
                                updates,
                                availableUpdates = group.Updates,
                                initialFiles = group.InitialFileCount,
                                initialContentBytes = group.InitialContentBytes,
                                bundleFormat = "S3GRP002",
                                options.MaximumGroupBytes,
                                readTraceSha256 = groupName == "natural_zstd"
                                    ? ""
                                    : FileOperations.Fingerprint(
                                        Path.Combine(options.DatasetDirectory, "history", groupName + "." + profile + ".jsonl"),
                                        cancellationToken
                                    ).Sha256
                            });

                            var keys = LabKeys.Generate();
                            keys.Save(Path.Combine(run, "keys.private.json"));

                            using var writer = new HybridPacketWriter(groupId, keys);
                            using var observer = new GroupReadObserver(run, options.MaximumGroupBytes);

                            using var preparationCsv = new CsvLog(
                                Path.Combine(run, "preparation.csv"),
                                "version,files,content_bytes,bundle_bytes,data_generation_ms,packaging_ms"
                            );

                            double totalGenerationMs = 0;
                            double totalPackagingMs = 0;

                            var history = new PackagedCorpusHistory(options, catalog, group, historyWork, updates, row =>
                            {
                                totalGenerationMs += row.DataGenerationMs;
                                totalPackagingMs += row.PackagingMs;

                                preparationCsv.Append(
                                    row.Version,
                                    row.Files,
                                    row.ContentBytes,
                                    row.BundleBytes,
                                    row.DataGenerationMs,
                                    row.PackagingMs
                                );
                            });

                            var restore = new RestoreEngine(
                                store,
                                codec,
                                () => new HybridPacketReader(groupId, keys),
                                groupId,
                                prefix,
                                options.MaximumDeltas
                            );

                            try
                            {
                                var trialOptions = options.CreateTrialOptions(updates, profile == "read_heavy" ? 1 : 20);

                                var trial = new SingleFileTrial(
                                    trialOptions,
                                    history,
                                    policy,
                                    codec,
                                    store,
                                    writer,
                                    restore,
                                    run,
                                    prefix,
                                    groupId,
                                    referenceVersions[groupName],
                                    schedule,
                                    observer
                                );

                                RunSummary sync = await trial.RunAsync(repetition, cancellationToken);

                                var summary = new CorpusRunSummary(
                                    name,
                                    groupName,
                                    profile,
                                    sync,
                                    MeasurementSummary.Quantile(observer.Measurements.Select(row => row.TotalGroupReadMs), 0.5),
                                    MeasurementSummary.Quantile(observer.Measurements.Select(row => row.TotalGroupReadMs), 0.95),
                                    observer.Measurements.Sum(row => row.DownloadBytes),
                                    observer.Measurements.Sum(row => row.GetCount),
                                    totalGenerationMs,
                                    totalPackagingMs
                                );

                                summaries.Add(summary);

                                FileOperations.WriteJsonAtomic(Path.Combine(run, "corpus-summary.json"), summary);
                                FileOperations.WriteJsonAtomic(Path.Combine(directory, "summaries.json"), summaries);
                            }
                            finally
                            {
                                if (Directory.Exists(historyWork))
                                {
                                    Directory.Delete(historyWork, recursive: true);
                                }
                            }
                        }
                    }
                }
            }

            WriteSummary(Path.Combine(directory, "summaries.csv"), summaries);

            FileOperations.WriteJsonAtomic(Path.Combine(directory, "status.json"), new
            {
                status = "completed",
                experiment = 2,
                trials = summaries.Count,
                finishedUtc = DateTimeOffset.UtcNow
            });

            Console.WriteLine("Completed experiment 2: " + Path.Combine(directory, "summaries.csv"));
        }
        catch (Exception error)
        {
            FileOperations.WriteJsonAtomic(Path.Combine(directory, "status.json"), new
            {
                status = "failed",
                experiment = 2,
                error = error.Message,
                completedTrials = summaries.Count,
                utc = DateTimeOffset.UtcNow
            });

            throw;
        }
    }

    private static void WriteSummary(string file, IEnumerable<CorpusRunSummary> summaries)
    {
        using var csv = new CsvLog(
            file,
            "run,group,read_profile,policy,repetition,updates,full_updates,delta_updates,initial_upload_bytes,update_upload_bytes," +
            "total_upload_bytes,max_chain_bytes,max_delta_count,update_write_p50_ms,update_write_p95_ms," +
            "update_model_transfer_seconds,restore_count,group_read_p50_ms,group_read_p95_ms,total_read_body_bytes,total_get_count," +
            "data_generation_ms,packaging_ms,budget_excess_bytes,final_bundle_sha256"
        );

        foreach (var summary in summaries)
        {
            var row = summary.Synchronization;

            csv.Append(
                summary.Run,
                summary.Group,
                summary.ReadProfile,
                row.Policy,
                row.Repetition,
                row.Updates,
                row.FullUpdates,
                row.DeltaUpdates,
                row.InitialUploadBytes,
                row.UpdateUploadBytes,
                row.TotalUploadBytes,
                row.MaximumChainBytes,
                row.MaximumDeltaCount,
                row.UpdateWriteP50Ms,
                row.UpdateWriteP95Ms,
                row.UpdateModelTransferSeconds,
                row.RestoreCount,
                summary.GroupReadP50Ms,
                summary.GroupReadP95Ms,
                summary.TotalReadBytes,
                summary.TotalGetCount,
                summary.TotalDataGenerationMs,
                summary.TotalPackagingMs,
                row.TotalBudgetExcessBytes,
                row.FinalSha256
            );
        }
    }
}