using S3SyncLab.Application;
using S3SyncLab.Configuration;
using S3SyncLab.Core;
using S3SyncLab.Infrastructure;
using S3SyncLab.Infrastructure.Corpus;
using S3SyncLab.Infrastructure.Crypto;
using S3SyncLab.Infrastructure.Delta;
using S3SyncLab.Infrastructure.Storage;

namespace S3SyncLab.Verification;

public static class ExperimentTwoSelfTests
{
    public static async Task RunAsync(CancellationToken cancellationToken)
    {
        string root = Path.Combine(Path.GetTempPath(), "s3sync-experiment2-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);

        try
        {
            CheckBundle(root, cancellationToken);
            CheckReadSchedule(root);
            await CheckTrialAsync(root, cancellationToken);

            Console.WriteLine("PASS experiment 2: canonical group bytes, paths, empty files, additions/deletions,");
            Console.WriteLine("  corrupt container rejection, read traces, full/delta/full restoration and separate packaging metrics.");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static void CheckBundle(string root, CancellationToken cancellationToken)
    {
        string one = Path.Combine(root, "one.txt");
        string empty = Path.Combine(root, "empty.bin");

        File.WriteAllText(one, "first version\n", new UTF8Encoding(false));
        File.WriteAllBytes(empty, []);

        GroupSourceFile[] entries =
        [
            new("docs/one.txt", one),
            new("empty.bin", empty)
        ];

        string first = Path.Combine(root, "first.group");
        string second = Path.Combine(root, "second.group");

        GroupBundle.Write(entries, first, 1024 * 1024, cancellationToken);
        File.SetLastWriteTimeUtc(one, DateTime.UtcNow.AddYears(-1));
        GroupBundle.Write(entries.Reverse(), second, 1024 * 1024, cancellationToken);

        Require(
            FileOperations.Fingerprint(first, cancellationToken) == FileOperations.Fingerprint(second, cancellationToken),
            "Packaging must ignore source enumeration order and timestamps."
        );

        string extracted = Path.Combine(root, "extracted");
        var info = GroupBundle.Extract(first, extracted, 1024 * 1024, cancellationToken);

        Require(
            info.Files == 2
                && new FileInfo(Path.Combine(extracted, "empty.bin")).Length == 0,
            "All files, including an empty file, must restore."
        );

        Require(File.ReadAllText(Path.Combine(extracted, "docs", "one.txt")) == "first version\n", "Restored text.");
        ExpectFailure(() => GroupBundle.Extract(first, extracted, 1024 * 1024, cancellationToken));
        Require(File.Exists(Path.Combine(extracted, "docs", "one.txt")), "Existing destination must remain intact.");

        string changed = Path.Combine(root, "changed.group");
        GroupBundle.Write([new GroupSourceFile("new.txt", one)], changed, 1024 * 1024, cancellationToken);

        string next = Path.Combine(root, "next-extracted");
        GroupBundle.Extract(changed, next, 1024 * 1024, cancellationToken);

        Require(
            File.Exists(Path.Combine(next, "new.txt"))
                && !File.Exists(Path.Combine(next, "empty.bin")),
            "A new full group view must reflect additions and deletions."
        );

        foreach (string path in new[] { "../escape", "/absolute", "a/../b", ".git/config", "CON.txt", "a\\b" })
        {
            ExpectFailure(() => GroupBundle.ValidateRelativePath(path));
        }

        ExpectFailure(() => GroupBundle.ValidateLinkTarget("docs/link", "../../escape"));

        if (!OperatingSystem.IsWindows())
        {
            string linksBundle = Path.Combine(root, "links.group");

            GroupBundle.Write(
                [
                    new("docs/one.txt", one, Executable: true),
                    new("link", "", "docs/one.txt")
                ],
                linksBundle,
                1024 * 1024,
                cancellationToken
            );

            string linkDirectory = Path.Combine(root, "links-extracted");
            GroupBundle.Extract(linksBundle, linkDirectory, 1024 * 1024, cancellationToken);

            Require(
                new FileInfo(Path.Combine(linkDirectory, "link")).LinkTarget == "docs/one.txt",
                "Symbolic link targets must be preserved."
            );

            Require(
                (File.GetUnixFileMode(Path.Combine(linkDirectory, "docs", "one.txt")) & UnixFileMode.UserExecute) != 0,
                "Executable files must retain the executable flag on Unix."
            );
        }

        ExpectFailure(() => GroupBundle.Write([new("A.txt", one), new("a.txt", one)], second, 1024 * 1024, cancellationToken));

        byte[] corrupt = File.ReadAllBytes(first);
        corrupt[^1] ^= 1;

        string corruptFile = Path.Combine(root, "corrupt.group");
        File.WriteAllBytes(corruptFile, corrupt);

        string corruptDestination = Path.Combine(root, "corrupt-output");
        ExpectFailure(() => GroupBundle.Extract(corruptFile, corruptDestination, 1024 * 1024, cancellationToken));
        Require(!Directory.Exists(corruptDestination), "Rejected content must not remain as a restored directory.");
    }

    private static void CheckReadSchedule(string root)
    {
        string trace = Path.Combine(root, "trace.jsonl");

        File.WriteAllLines(
            trace,
            Enumerable.Range(1, 5).SelectMany(step =>
                step is 2 or 5
                    ? new[]
                    {
                        $"{{\"action\":\"write\",\"group\":\"jsonl\",\"step\":{step}}}",
                        $"{{\"action\":\"cold_restore\",\"group\":\"jsonl\",\"step\":{step}}}"
                    }
                    : new[]
                    {
                        $"{{\"action\":\"write\",\"group\":\"jsonl\",\"step\":{step}}}"
                    }
            )
        );

        var schedule = CorpusReadSchedule.Load(trace, "jsonl", 5);

        Require(
            schedule.ShouldRead(0, 5)
                && schedule.ShouldRead(2, 5)
                && !schedule.ShouldRead(3, 5),
            "Exact trace reads."
        );

        Require(schedule.ShouldRead(3, 3), "A capped pilot still verifies its final state.");

        File.AppendAllText(trace, "{\"action\":\"cold_restore\",\"group\":\"jsonl\",\"step\":5}\n");
        ExpectFailure(() => CorpusReadSchedule.Load(trace, "jsonl", 5));
    }

    private static async Task CheckTrialAsync(string root, CancellationToken cancellationToken)
    {
        string run = Path.Combine(root, "trial");
        Directory.CreateDirectory(run);

        var options = new LabOptions
        {
            Updates = 3,
            RestoreEvery = 2,
            MaximumDeltas = 1
        };

        await using var store = new LocalObjectStore(Path.Combine(root, "objects"));
        var history = new SmallGroupHistory(Path.Combine(run, "source"));

        Guid group = Guid.NewGuid();
        var keys = LabKeys.Generate();

        using var writer = new HybridPacketWriter(group, keys);
        var codec = new OctodiffCodec(128);
        string prefix = "experiment2-test/" + group.ToString("N");

        var restore = new RestoreEngine(
            store,
            codec,
            () => new HybridPacketReader(group, keys),
            group,
            prefix,
            1
        );

        using var observer = new GroupReadObserver(run, 1024 * 1024);

        var trial = new SingleFileTrial(
            options,
            history,
            new FixedThresholdPolicy(4),
            codec,
            store,
            writer,
            restore,
            run,
            prefix,
            group,
            new Dictionary<int, FileFingerprint>(),
            CorpusReadSchedule.Periodic(3, 2),
            observer
        );

        var summary = await trial.RunAsync(1, cancellationToken);

        Require(
            summary.Updates == 3
                && summary.FullUpdates >= 1
                && observer.Measurements.Count == 3,
            "The shared trial must restore groups and compact the chain."
        );

        Require(observer.Measurements.All(row => row.Files == 2), "Every restored group must have its expected files.");

        // The history exposes a deliberately fixed metric to test accounting, not performance.
        Require(
            summary.UpdateWriteMeanMs >= SmallGroupHistory.TestPackagingMs,
            "Client packaging must be included in measured write cost."
        );
    }

    private sealed class SmallGroupHistory(string directory) : IVersionHistory, IPreparedVersionMetrics
    {
        public const double TestPackagingMs = 123.0;

        public string CurrentFile => Path.Combine(directory, "current.group");

        public double DataGenerationMs => 0;

        public double ClientPreparationMs => TestPackagingMs;

        public Task<DatasetInfo> InitializeAsync(CancellationToken cancellationToken)
        {
            Directory.CreateDirectory(directory);

            File.WriteAllBytes(
                Path.Combine(directory, "one.bin"),
                Enumerable.Range(0, 16384)
                    .Select(index => (byte)(index % 251))
                    .ToArray()
            );
            File.WriteAllBytes(Path.Combine(directory, "empty.bin"), []);

            Package(cancellationToken);

            return Task.FromResult(new DatasetInfo(
                "self-test-group",
                3,
                FileOperations.Fingerprint(CurrentFile, cancellationToken),
                "",
                "",
                "C# fixture"
            ));
        }

        public Task AdvanceAsync(int expectedVersion, CancellationToken cancellationToken)
        {
            using (var output = new FileStream(Path.Combine(directory, "one.bin"), FileMode.Open, FileAccess.Write))
            {
                output.Position = expectedVersion * 1000;
                output.WriteByte((byte)expectedVersion);
            }

            Package(cancellationToken);

            return Task.CompletedTask;
        }

        private void Package(CancellationToken cancellationToken) => GroupBundle.Write(
            [
                new("one.bin", Path.Combine(directory, "one.bin")),
                new("empty.bin", Path.Combine(directory, "empty.bin"))
            ],
            CurrentFile,
            1024 * 1024,
            cancellationToken
        );
    }

    private static void ExpectFailure(Action action)
    {
        bool failed = false;

        try
        {
            action();
        }
        catch (Exception error) when (error is IOException or InvalidDataException or ArgumentException)
        {
            failed = true;
        }

        Require(failed, "Invalid group input must be rejected.");
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException("Experiment 2 self-test failed: " + message);
        }
    }
}