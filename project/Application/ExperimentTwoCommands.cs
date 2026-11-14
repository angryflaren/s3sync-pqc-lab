using S3SyncLab.Configuration;
using S3SyncLab.Core;
using S3SyncLab.Infrastructure;
using S3SyncLab.Infrastructure.Corpus;
using S3SyncLab.Infrastructure.Crypto;
using S3SyncLab.Infrastructure.Delta;

namespace S3SyncLab.Application;

public static class ExperimentTwoCommands
{
    public static async Task<int> ExecuteAsync(string[] args, CancellationToken cancellationToken)
    {
        if (args.Length < 2)
        {
            throw new ArgumentException("Set the experiment 2 configuration file.");
        }

        var options = ExperimentTwoOptions.Load(args[1]);

        if (args[0] is "validate2" or "run2")
        {
            if (args.Length != 2)
            {
                throw new ArgumentException("Expected COMMAND CONFIG.json.");
            }

            var catalog = await CorpusCatalog.ValidateAsync(options, cancellationToken);

            Console.WriteLine("Input files, recipes and read traces verified.");

            foreach (var group in catalog.Report.Groups)
            {
                Console.WriteLine(
                    $"{group.Name}: {group.InitialFileCount} initial files, {group.InitialContentBytes} bytes, {group.Updates} available updates"
                );
            }

            if (args[0] == "validate2")
            {
                return 0;
            }

            await using IObjectStore store = Program.CreateStore(options.Storage);
            await new ExperimentTwo(options, catalog, store).RunAsync(cancellationToken);

            return 0;
        }

        await using IObjectStore storage = Program.CreateStore(options.Storage);

        if (args[0] == "cleanup2" && args.Length == 3)
        {
            await Program.CleanupAsync(storage, args[2], cancellationToken);
            return 0;
        }

        if (args[0] == "restore2" && args.Length == 4)
        {
            await RestoreGroupAsync(options, storage, args[2], args[3], cancellationToken);
            return 0;
        }

        throw new ArgumentException(
            "Expected run2/validate2 CONFIG, restore2 CONFIG TRIAL NEW_DIRECTORY, or cleanup2 CONFIG TRIAL."
        );
    }

    private static async Task RestoreGroupAsync(
        ExperimentTwoOptions options,
        IObjectStore store,
        string runDirectory,
        string destination,
        CancellationToken cancellationToken)
    {
        runDirectory = Path.GetFullPath(runDirectory);
        destination = Path.GetFullPath(destination);

        if (Directory.Exists(destination)
            || File.Exists(destination))
        {
            throw new IOException("Choose a new group destination directory.");
        }

        using var runLock = new FileStream(
            Path.Combine(runDirectory, "run.lock"),
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None
        );

        var checkpoint = FileOperations.ReadJson<RunCheckpoint>(Path.Combine(runDirectory, "run.json"));

        if (checkpoint.Format != 1)
        {
            throw new InvalidDataException("Unknown run checkpoint.");
        }

        Guid group = Guid.ParseExact(checkpoint.Group, "N");
        var keys = LabKeys.Load(Path.Combine(runDirectory, "keys.private.json"));
        string temporaryBundle = Path.Combine(runDirectory, "restore-group-" + Guid.NewGuid().ToString("N") + ".bin");

        string parent = Path.GetDirectoryName(destination)!;
        Directory.CreateDirectory(parent);
        string extracted = Path.Combine(parent, ".s3sync-extract-" + Guid.NewGuid().ToString("N"));

        try
        {
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
                temporaryBundle,
                runDirectory,
                cancellationToken
            );

            if (result.File != checkpoint.File)
            {
                throw new InvalidDataException("Bundle differs from the local checkpoint.");
            }

            var info = GroupBundle.Extract(
                temporaryBundle,
                extracted,
                options.MaximumGroupBytes,
                cancellationToken
            );

            Directory.Move(extracted, destination);

            Console.WriteLine(
                $"Restored group: {info.Files} files, {info.ContentBytes} content bytes; bundle SHA-256 {result.File.Sha256}"
            );
        }
        finally
        {
            FileOperations.DeleteIfExists(temporaryBundle);

            if (Directory.Exists(extracted))
            {
                Directory.Delete(extracted, recursive: true);
            }
        }
    }
}