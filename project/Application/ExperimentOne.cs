using System.Reflection;
using System.Runtime.InteropServices;
using S3SyncLab.Configuration;
using S3SyncLab.Core;
using S3SyncLab.Infrastructure;
using S3SyncLab.Infrastructure.Crypto;
using S3SyncLab.Infrastructure.Delta;
using S3SyncLab.Infrastructure.History;

namespace S3SyncLab.Application;

// Composition for experiment 1. A second IExperiment can reuse the same codecs,
// packet protection, policies, storage adapters and restoration engine.
public sealed class ExperimentOne(LabOptions options, IObjectStore store) : IExperiment
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

        string batch = DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ", System.Globalization.CultureInfo.InvariantCulture)
            + "-" + Guid.NewGuid().ToString("N")[..8];
        string directory = Path.Combine(options.OutputDirectory, batch);
        Directory.CreateDirectory(directory);

        FileOperations.WriteJsonAtomic(Path.Combine(directory, "configuration.json"), options);
        FileOperations.WriteJsonAtomic(Path.Combine(directory, "environment.json"), new
        {
            format = 1,
            startedUtc = DateTimeOffset.UtcNow,
            framework = RuntimeInformation.FrameworkDescription,
            os = RuntimeInformation.OSDescription,
            architecture = RuntimeInformation.ProcessArchitecture.ToString(),
            processors = Environment.ProcessorCount,
            serverGc = System.Runtime.GCSettings.IsServerGC,
            storage = store.Description,
            dependencies = new[]
            {
                typeof(Org.BouncyCastle.Security.SecureRandom).Assembly,
                typeof(Octodiff.Core.SignatureBuilder).Assembly,
                typeof(Amazon.S3.AmazonS3Client).Assembly,
                typeof(Amazon.Runtime.AmazonWebServiceRequest).Assembly
            }
            .Select(assembly => new
            {
                name = assembly.GetName().Name,
                assemblyVersion = assembly.GetName().Version?.ToString(),
                informationalVersion = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            })
            .OrderBy(item => item.name)
            .ToArray(),
            networkModel = "Virtual time; request body bytes only; no real bandwidth shaping; independent of measured wall time.",
            decisionBudgetMode = options.Network.DecisionBudgetMode,
            readDefinition = "New reader and empty plaintext cache; OS page cache is not flushed.",
            workload = "One SQLite file, sequential synthetic SQL updates, one writer, sequential checkpoint reads."
        });

        var policies = new ISnapshotPolicy[]
        {
            new FixedThresholdPolicy(2),
            new FixedThresholdPolicy(4),
            new AdaptiveThresholdPolicy(options.MinimumThreshold, options.MaximumThreshold)
        };

        var referenceVersions = new Dictionary<int, FileFingerprint>();
        var summaries = new List<RunSummary>();
        var delta = new OctodiffCodec(options.ChunkBytes);

        Console.WriteLine("Results: " + directory);

        try
        {
            for (int repetition = 1; repetition <= options.Repetitions; repetition++)
            {
                // Rotate the starting policy across repetitions to reduce a fixed order effect.
                for (int offset = 0; offset < policies.Length; offset++)
                {
                    var policy = policies[(repetition - 1 + offset) % policies.Length];
                    string name = $"r{repetition:D2}-{policy.Name}";
                    string runDirectory = Path.Combine(directory, name);
                    Directory.CreateDirectory(runDirectory);

                    using var runLock = new FileStream(
                        Path.Combine(runDirectory, "run.lock"),
                        FileMode.OpenOrCreate,
                        FileAccess.ReadWrite,
                        FileShare.None
                    );

                    string historyWork = Path.Combine(runDirectory, "history-work");
                    string prefix = options.Storage.Prefix + "/" + batch + "/" + name;
                    Guid group = Guid.NewGuid();

                    FileOperations.WriteJsonAtomic(
                        Path.Combine(runDirectory, "namespace.json"),
                        new RunIdentity(1, group.ToString("N"), prefix, prefix + "/HEAD.json")
                    );

                    var keys = LabKeys.Generate();
                    keys.Save(Path.Combine(runDirectory, "keys.private.json"));

                    using var writer = new HybridPacketWriter(group, keys);

                    var history = new PythonSqliteHistory(
                        options.DatasetDirectory,
                        historyWork,
                        options.PythonExecutable,
                        options.HistoryScript,
                        options.Updates,
                        options.ProcessTimeoutSeconds
                    );

                    var restore = new RestoreEngine(
                        store,
                        delta,
                        () => new HybridPacketReader(group, keys),
                        group,
                        prefix,
                        options.MaximumDeltas
                    );

                    try
                    {
                        var trial = new SingleFileTrial(
                            options,
                            history,
                            policy,
                            delta,
                            store,
                            writer,
                            restore,
                            runDirectory,
                            prefix,
                            group,
                            referenceVersions
                        );

                        summaries.Add(await trial.RunAsync(repetition, cancellationToken));
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

            using var csv = new CsvLog(
                Path.Combine(directory, "summaries.csv"),
                "run,policy,repetition,updates,full_updates,delta_updates,initial_upload_bytes,update_upload_bytes,total_upload_bytes," +
                "max_chain_bytes,max_delta_count,update_write_p50_ms,update_write_p95_ms,update_write_mean_ms," +
                "update_model_transfer_seconds,restore_count,restore_p50_ms,restore_p95_ms,total_budget_excess_bytes,final_file_bytes,final_sha256"
            );

            foreach (var row in summaries)
            {
                csv.Append(
                    row.Run,
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
                    row.UpdateWriteMeanMs,
                    row.UpdateModelTransferSeconds,
                    row.RestoreCount,
                    row.RestoreP50Ms,
                    row.RestoreP95Ms,
                    row.TotalBudgetExcessBytes,
                    row.FinalFileBytes,
                    row.FinalSha256
                );
            }

            FileOperations.WriteJsonAtomic(Path.Combine(directory, "status.json"), new
            {
                status = "completed",
                finishedUtc = DateTimeOffset.UtcNow,
                trials = summaries.Count
            });

            Console.WriteLine("Completed. Summary: " + Path.Combine(directory, "summaries.csv"));
        }
        catch (Exception error)
        {
            FileOperations.WriteJsonAtomic(Path.Combine(directory, "status.json"), new
            {
                status = "failed",
                utc = DateTimeOffset.UtcNow,
                error = error.Message,
                completedTrials = summaries.Count
            });

            throw;
        }
    }
}