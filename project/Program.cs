using S3SyncLab.Application;
using S3SyncLab.Configuration;
using S3SyncLab.Core;
using S3SyncLab.Infrastructure;
using S3SyncLab.Infrastructure.Crypto;
using S3SyncLab.Infrastructure.Delta;
using S3SyncLab.Infrastructure.Storage;
using S3SyncLab.Verification;

namespace S3SyncLab;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        using var cancellation = new CancellationTokenSource();

        ConsoleCancelEventHandler handler = (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cancellation.Cancel();
        };

        Console.CancelKeyPress += handler;

        try
        {
            if (args.Length == 1 && args[0] == "self-test")
            {
                await SelfTests.RunAsync(cancellation.Token);
                return 0;
            }

            if (args.Length == 1 && args[0] == "self-test2")
            {
                await ExperimentTwoSelfTests.RunAsync(cancellation.Token);
                return 0;
            }

            if (args.Length < 2)
            {
                PrintUsage();
                return 2;
            }

            if (args[0] is "run2" or "validate2" or "restore2" or "cleanup2")
            {
                return await ExperimentTwoCommands.ExecuteAsync(args, cancellation.Token);
            }

            var options = LabOptions.Load(args[1]);
            await using IObjectStore store = CreateStore(options.Storage);

            switch (args[0])
            {
                case "run" when args.Length == 2:
                    await new ExperimentOne(options, store).RunAsync(cancellation.Token);
                    break;

                case "storage-test" when args.Length == 2:
                    await SelfTests.CheckStoreAsync(store, options.Storage.Prefix, cancellation.Token);
                    break;

                case "restore" when args.Length == 4:
                    await RestoreAsync(options, store, args[2], args[3], cancellation.Token);
                    break;

                case "cleanup" when args.Length == 3:
                    await CleanupAsync(store, args[2], cancellation.Token);
                    break;

                default:
                    PrintUsage();
                    return 2;
            }

            return 0;
        }
        catch (OperationCanceledException)
        {
            Console.Error.WriteLine("Cancelled. Completed logs and uploaded objects are retained.");
            return 130;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine(
                Environment.GetEnvironmentVariable("S3LAB_TRACE") == "1"
                    ? error.ToString()
                    : error.GetType().Name + ": " + error.Message
            );
            return 1;
        }
        finally
        {
            Console.CancelKeyPress -= handler;
        }
    }

    internal static IObjectStore CreateStore(StorageOptions options) => options.Provider == "local"
        ? new LocalObjectStore(options.LocalDirectory)
        : new S3ObjectStore(options);

    private static async Task RestoreAsync(
        LabOptions options,
        IObjectStore store,
        string runDirectory,
        string destination,
        CancellationToken cancellationToken)
    {
        runDirectory = Path.GetFullPath(runDirectory);
        destination = Path.GetFullPath(destination);

        using var runLock = new FileStream(
            Path.Combine(runDirectory, "run.lock"),
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None
        );

        if (File.Exists(destination))
        {
            throw new IOException("Choose a new destination file for restoration.");
        }

        var checkpoint = FileOperations.ReadJson<RunCheckpoint>(Path.Combine(runDirectory, "run.json"));

        if (checkpoint.Format != 1)
        {
            throw new InvalidDataException("Unknown run checkpoint format.");
        }

        Guid group = Guid.ParseExact(checkpoint.Group, "N");
        var keys = LabKeys.Load(Path.Combine(runDirectory, "keys.private.json"));

        var restore = new RestoreEngine(
            store,
            new OctodiffCodec(options.ChunkBytes),
            () => new HybridPacketReader(group, keys),
            group,
            checkpoint.ObjectPrefix,
            options.MaximumDeltas
        );

        var result = await restore.RestoreAsync(
            checkpoint.HeadKey,
            checkpoint.Root,
            destination,
            runDirectory,
            cancellationToken
        );

        if (result.File != checkpoint.File)
        {
            throw new InvalidDataException("Restored file differs from the local checkpoint.");
        }

        Console.WriteLine($"Restored {result.File.Length} bytes; SHA-256: {result.File.Sha256}");
    }

    internal static async Task CleanupAsync(
        IObjectStore store,
        string runDirectory,
        CancellationToken cancellationToken)
    {
        runDirectory = Path.GetFullPath(runDirectory);

        using var runLock = new FileStream(
            Path.Combine(runDirectory, "run.lock"),
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None
        );

        string checkpointFile = Path.Combine(runDirectory, "run.json");
        var identity = FileOperations.ReadJson<RunIdentity>(Path.Combine(runDirectory, "namespace.json"));

        if (identity.Format != 1)
        {
            throw new InvalidDataException("Unknown trial namespace format.");
        }

        var checkpoint = File.Exists(checkpointFile)
            ? FileOperations.ReadJson<RunCheckpoint>(checkpointFile)
            : null;

        string prefix = FileOperations.ValidateObjectKey(identity.ObjectPrefix) + "/";
        string journalFile = Path.Combine(runDirectory, "object-keys.txt");
        var keys = File.Exists(journalFile)
            ? File.ReadAllLines(journalFile).Distinct().ToArray()
            : Array.Empty<string>();

        foreach (string key in keys)
        {
            if (!FileOperations.ValidateObjectKey(key).StartsWith(prefix, StringComparison.Ordinal))
            {
                throw new InvalidDataException("Cleanup journal contains a key outside this trial.");
            }
        }

        foreach (string key in keys)
        {
            await store.DeleteAsync(key, cancellationToken);
        }

        if (checkpoint is not null)
        {
            FileOperations.WriteJsonAtomic(checkpointFile, checkpoint with { Status = "objects-deleted" });
        }

        FileOperations.WriteJsonAtomic(
            Path.Combine(runDirectory, "cleanup.json"),
            new
            {
                status = "objects-deleted",
                utc = DateTimeOffset.UtcNow,
                recordedKeys = keys.Length
            }
        );

        Console.WriteLine($"Deleted {keys.Length} recorded object keys for this trial. Local logs and original dataset were retained.");
    }

    private static void PrintUsage()
    {
        Console.WriteLine("S3SyncLab (.NET 10), experiments 1 and 2");
        Console.WriteLine("  self-test");
        Console.WriteLine("  self-test2");
        Console.WriteLine("  storage-test CONFIG.json");
        Console.WriteLine("  run CONFIG.json");
        Console.WriteLine("  restore CONFIG.json RUN_DIRECTORY NEW_OUTPUT.sqlite");
        Console.WriteLine("  cleanup CONFIG.json RUN_DIRECTORY");
        Console.WriteLine("  validate2 CONFIG.experiment2.json");
        Console.WriteLine("  run2 CONFIG.experiment2.json");
        Console.WriteLine("  restore2 CONFIG.experiment2.json RUN_DIRECTORY NEW_OUTPUT_DIRECTORY");
        Console.WriteLine("  cleanup2 CONFIG.experiment2.json RUN_DIRECTORY");
    }
}