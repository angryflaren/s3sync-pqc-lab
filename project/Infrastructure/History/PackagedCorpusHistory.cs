using System.Text.Json;
using S3SyncLab.Configuration;
using S3SyncLab.Core;
using S3SyncLab.Infrastructure.Corpus;
using S3SyncLab.Infrastructure.Processes;

namespace S3SyncLab.Infrastructure.History;

public sealed record GroupPreparationMeasurement(
    int Version,
    int Files,
    long ContentBytes,
    long BundleBytes,
    double DataGenerationMs,
    double PackagingMs);

public sealed class PackagedCorpusHistory : IVersionHistory, IPreparedVersionMetrics
{
    private readonly ExperimentTwoOptions options;
    private readonly CorpusCatalog catalog;
    private readonly CorpusGroup group;
    private readonly string workspace;
    private readonly int updates;
    private readonly ExternalProcess python;
    private readonly Action<GroupPreparationMeasurement>? report;
    private int version = -1;

    public string CurrentFile => Path.Combine(workspace, "current.group");

    public double DataGenerationMs { get; private set; }

    public double ClientPreparationMs { get; private set; }

    public PackagedCorpusHistory(
        ExperimentTwoOptions options,
        CorpusCatalog catalog,
        CorpusGroup group,
        string workspace,
        int updates,
        Action<GroupPreparationMeasurement>? report = null)
    {
        this.options = options;
        this.catalog = catalog;
        this.group = group;
        this.workspace = workspace;
        this.updates = updates;
        this.report = report;

        python = new ExternalProcess(options.PythonExecutable, options.ProcessTimeoutSeconds);
    }

    public async Task<DatasetInfo> InitializeAsync(CancellationToken cancellationToken)
    {
        if (Directory.Exists(workspace))
        {
            throw new IOException("History workspace must be new.");
        }

        if (updates < 1
            || updates > group.Updates)
        {
            throw new InvalidDataException("Requested history is out of range.");
        }

        Directory.CreateDirectory(workspace);

        var timer = Stopwatch.StartNew();

        if (group.Name == "natural_zstd")
        {
            await python.RunAsync(
                [
                    options.NaturalScript,
                    "--root",
                    Path.Combine(options.DatasetDirectory, "natural_zstd"),
                    "--work",
                    Path.Combine(workspace, "source"),
                    "reset"
                ],
                cancellationToken
            );
        }
        else
        {
            File.Copy(
                Path.Combine(options.DatasetDirectory, "manifest.json"),
                Path.Combine(workspace, "manifest.json")
            );

            foreach (var file in group.Files)
            {
                string source = GroupBundle.Resolve(Path.Combine(options.DatasetDirectory, "base"), file.RelativePath);
                string target = GroupBundle.Resolve(Path.Combine(workspace, "base"), file.RelativePath);

                await FileOperations.CopyAtomicAsync(source, target, cancellationToken);
                FileOperations.RequireMatch(target, file.Fingerprint, cancellationToken);
            }

            await python.RunAsync(
                [
                    options.CorpusScript,
                    "--root",
                    workspace,
                    "reset",
                    "--group",
                    group.Name
                ],
                cancellationToken
            );

            CheckControlledState(0);
        }

        var sources = await GetFilesAsync(cancellationToken);
        DataGenerationMs = timer.Elapsed.TotalMilliseconds;
        version = 0;

        Package(sources, cancellationToken);

        return new DatasetInfo(
            "s3sync-corpus-v1/" + group.Name,
            group.Updates,
            FileOperations.Fingerprint(CurrentFile, cancellationToken),
            group.ManifestSha256,
            group.HistorySha256,
            catalog.Report.PreparationRuntime
                + "; corpus-script="
                + catalog.Report.CorpusScriptSha256
                + "; natural-script="
                + catalog.Report.NaturalScriptSha256
        );
    }

    public async Task AdvanceAsync(int expectedVersion, CancellationToken cancellationToken)
    {
        if (version < 0
            || expectedVersion != version + 1
            || expectedVersion > updates)
        {
            throw new InvalidOperationException("Group histories must advance sequentially.");
        }

        var timer = Stopwatch.StartNew();

        if (group.Name == "natural_zstd")
        {
            await python.RunAsync(
                [
                    options.NaturalScript,
                    "--root",
                    Path.Combine(options.DatasetDirectory, "natural_zstd"),
                    "--work",
                    Path.Combine(workspace, "source"),
                    "step",
                    "--number",
                    expectedVersion.ToString(System.Globalization.CultureInfo.InvariantCulture)
                ],
                cancellationToken
            );
        }
        else
        {
            await python.RunAsync(
                [
                    options.CorpusScript,
                    "--root",
                    workspace,
                    "step",
                    "--group",
                    group.Name,
                    "--number",
                    expectedVersion.ToString(System.Globalization.CultureInfo.InvariantCulture)
                ],
                cancellationToken
            );

            CheckControlledState(expectedVersion);
        }

        var sources = await GetFilesAsync(cancellationToken);
        DataGenerationMs = timer.Elapsed.TotalMilliseconds;
        version = expectedVersion;

        Package(sources, cancellationToken);
    }

    private async Task<IReadOnlyList<GroupSourceFile>> GetFilesAsync(CancellationToken cancellationToken)
    {
        if (group.Name != "natural_zstd")
        {
            var files = group.Files
                .Select(file => new GroupSourceFile(
                    file.RelativePath,
                    GroupBundle.Resolve(Path.Combine(workspace, "work"), file.RelativePath)
                ))
                .ToArray();

            foreach (var file in files)
            {
                if (File.Exists(file.FullPath + "-wal")
                    || File.Exists(file.FullPath + "-journal"))
                {
                    throw new InvalidDataException("An SQLite journal remains open in the source group.");
                }
            }

            return files;
        }

        string root = Path.Combine(workspace, "source");

        string output = await new ExternalProcess("git", options.ProcessTimeoutSeconds).RunAsync(
            ["-C", root, "-c", "core.autocrlf=false", "ls-files", "--stage", "-z"],
            cancellationToken
        );

        return output
            .Split('\0', StringSplitOptions.RemoveEmptyEntries)
            .Select(record =>
            {
                int tab = record.IndexOf('\t');

                if (tab < 0)
                {
                    throw new InvalidDataException("Invalid Git file record.");
                }

                string mode = record.Split(' ', 2)[0];
                string path = record[(tab + 1)..];
                string fullPath = GroupBundle.Resolve(root, path);

                string? linkTarget = mode == "120000"
                    ? new FileInfo(fullPath).LinkTarget ?? File.ReadAllText(fullPath, new UTF8Encoding(false, true))
                    : null;

                if (mode is not ("100644" or "100755" or "120000"))
                {
                    throw new InvalidDataException("Unsupported Git file mode: " + mode);
                }

                return new GroupSourceFile(path, fullPath, linkTarget, mode == "100755");
            })
            .ToArray();
    }

    private void Package(IReadOnlyList<GroupSourceFile> sources, CancellationToken cancellationToken)
    {
        var timer = Stopwatch.StartNew();
        var info = GroupBundle.Write(sources, CurrentFile, options.MaximumGroupBytes, cancellationToken);
        ClientPreparationMs = timer.Elapsed.TotalMilliseconds;

        report?.Invoke(new GroupPreparationMeasurement(
            version,
            info.Files,
            info.ContentBytes,
            info.BundleBytes,
            DataGenerationMs,
            ClientPreparationMs
        ));
    }

    private void CheckControlledState(int expected)
    {
        string file = Path.Combine(workspace, "work", "state", group.Name + ".json");
        using var document = JsonDocument.Parse(File.ReadAllText(file));

        if (document.RootElement.GetProperty("step").GetInt32() != expected)
        {
            throw new InvalidDataException("Corpus generator prepared an unexpected version.");
        }
    }
}