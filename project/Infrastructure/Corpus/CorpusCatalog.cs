using System.Text.Json;
using S3SyncLab.Configuration;
using S3SyncLab.Core;
using S3SyncLab.Infrastructure.Processes;

namespace S3SyncLab.Infrastructure.Corpus;

public sealed record CorpusBaseFile(
    string RelativePath,
    FileFingerprint Fingerprint);

public sealed record CorpusGroup(
    string Name,
    int Updates,
    IReadOnlyList<CorpusBaseFile> Files,
    int InitialFileCount,
    long InitialContentBytes,
    string ManifestSha256,
    string HistorySha256);

public sealed record CorpusValidationReport(
    string DatasetDirectory,
    string PreparationRuntime,
    string CorpusScriptSha256,
    string NaturalScriptSha256,
    IReadOnlyList<CorpusGroup> Groups);

public sealed class CorpusCatalog
{
    // Constants from the published corpus-v1 replay recipe, not tuning parameters.
    private const int JsonlFilesPerUpdate = 8;
    private const int JsonlStepMultiplier = 17;
    private const int JsonlFileStride = 29;
    private const int SqliteStepMultiplier = 7919;
    private const int SqliteRowStride = 31;
    private const int ZipStepMultiplier = 7;

    public CorpusValidationReport Report { get; }

    private CorpusCatalog(CorpusValidationReport report) => Report = report;

    public CorpusGroup GetGroup(string name) => Report.Groups.Single(group => group.Name == name);

    public static async Task<CorpusCatalog> ValidateAsync(
        ExperimentTwoOptions options,
        CancellationToken cancellationToken)
    {
        options.Validate();

        var groups = new List<CorpusGroup>();
        var missing = new List<string>();
        bool controlled = options.Groups.Any(group => group != "natural_zstd");
        string root = options.DatasetDirectory;
        string manifestFile = Path.Combine(root, "manifest.json");

        if (controlled)
        {
            RequireFile(options.CorpusScript, missing);

            if (!File.Exists(manifestFile))
            {
                throw new FileNotFoundException("Corpus manifest is missing.", manifestFile);
            }

            using var document = JsonDocument.Parse(File.ReadAllText(manifestFile));
            var manifest = document.RootElement;

            if (manifest.GetProperty("dataset").GetString() != "s3sync-corpus-v1")
            {
                throw new InvalidDataException("Expected the s3sync-corpus-v1 manifest.");
            }

            _ = manifest.GetProperty("data_seed").GetString() ?? throw new InvalidDataException("Missing corpus data_seed.");

            var files = manifest.GetProperty("files")
                .EnumerateArray()
                .Select(item => new CorpusBaseFile(
                    item.GetProperty("path").GetString() ?? "",
                    new FileFingerprint(
                        item.GetProperty("bytes").GetInt64(),
                        item.GetProperty("sha256").GetString()!.ToLowerInvariant()
                    )
                ))
                .ToArray();

            var unique = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var file in files)
            {
                GroupBundle.ValidateRelativePath(file.RelativePath);

                if (!unique.Add(file.RelativePath)
                    || file.Fingerprint.Length < 0
                    || Convert.FromHexString(file.Fingerprint.Sha256).Length != 32)
                {
                    throw new InvalidDataException("Invalid or duplicate corpus manifest entry.");
                }
            }

            string manifestHash = FileOperations.Fingerprint(manifestFile, cancellationToken).Sha256;

            foreach (string name in options.Groups.Where(group => group != "natural_zstd"))
            {
                int updates = manifest.GetProperty("groups").GetProperty(name).GetInt32();

                if (updates < 1)
                {
                    throw new InvalidDataException("Invalid group update count.");
                }

                var selected = files
                    .Where(file => name switch
                    {
                        "jsonl" => file.RelativePath.StartsWith("jsonl/", StringComparison.Ordinal),
                        "sqlite" => file.RelativePath == "sqlite/catalog.sqlite",
                        _ => file.RelativePath == "zip/" + name + ".zip"
                    })
                    .OrderBy(file => file.RelativePath, StringComparer.Ordinal)
                    .ToArray();

                if (selected.Length == 0
                    || (name != "jsonl" && selected.Length != 1))
                {
                    throw new InvalidDataException("Manifest has no complete file set for " + name + ".");
                }

                foreach (var file in selected)
                {
                    RequireFile(GroupBundle.Resolve(Path.Combine(root, "base"), file.RelativePath), missing);
                }

                string historyFile = Path.Combine(root, "history", name + ".jsonl");
                RequireFile(historyFile, missing);

                foreach (string profile in options.ReadProfiles)
                {
                    RequireFile(Path.Combine(root, "history", name + "." + profile + ".jsonl"), missing);
                }

                groups.Add(new CorpusGroup(
                    name,
                    updates,
                    selected,
                    selected.Length,
                    selected.Sum(file => file.Fingerprint.Length),
                    manifestHash,
                    ""
                ));
            }
        }

        if (options.Groups.Contains("natural_zstd"))
        {
            RequireFile(options.NaturalScript, missing);

            string naturalRoot = Path.Combine(root, "natural_zstd");
            string commitsFile = Path.Combine(naturalRoot, "commits.json");

            if (!File.Exists(commitsFile))
            {
                throw new FileNotFoundException("Natural-history manifest is missing.", commitsFile);
            }

            using var document = JsonDocument.Parse(File.ReadAllText(commitsFile));
            var manifest = document.RootElement;
            var versions = manifest.GetProperty("versions").EnumerateArray().ToArray();

            if (versions.Length < 2)
            {
                throw new InvalidDataException("Natural history needs at least two states.");
            }

            RequireFile(Path.Combine(naturalRoot, "initial.tar"), missing);

            for (int step = 0; step < versions.Length; step++)
            {
                if (versions[step].GetProperty("step").GetInt32() != step)
                {
                    throw new InvalidDataException("Natural-history steps are not sequential.");
                }

                if (step > 0)
                {
                    string patch = versions[step].GetProperty("patch").GetString() ?? "";
                    RequireFile(GroupBundle.Resolve(naturalRoot, patch), missing);
                }
            }

            string hash = FileOperations.Fingerprint(commitsFile, cancellationToken).Sha256;

            groups.Add(new CorpusGroup(
                "natural_zstd",
                versions.Length - 1,
                [],
                manifest.GetProperty("initial_file_count").GetInt32(),
                manifest.GetProperty("initial_payload_bytes").GetInt64(),
                hash,
                hash
            ));
        }

        if (missing.Count > 0)
        {
            string details = string.Join(Environment.NewLine, missing.Take(30));
            throw new FileNotFoundException($"Corpus is incomplete: {missing.Count} missing files. First paths:\n" + details);
        }

        var python = new ExternalProcess(options.PythonExecutable, options.ProcessTimeoutSeconds);

        string runtime = await python.RunAsync(
            [
                "-c",
                "import json,platform,sqlite3,zlib; " +
                "print(json.dumps({'python':platform.python_version(),'sqlite':sqlite3.sqlite_version,'zlib':zlib.ZLIB_RUNTIME_VERSION}))"
            ],
            cancellationToken
        );

        string corpusHash = controlled
            ? FileOperations.Fingerprint(options.CorpusScript, cancellationToken).Sha256
            : "";

        string naturalHash = options.Groups.Contains("natural_zstd")
            ? FileOperations.Fingerprint(options.NaturalScript, cancellationToken).Sha256
            : "";

        if (controlled)
        {
            string probe = await python.RunAsync(
                [
                    "-c",
                    "import runpy,json,sys; m=runpy.run_path(sys.argv[1]); " +
                    "print(json.dumps({k:m[k] for k in ['JSONL_FILE_COUNT','JSONL_FILE_SIZE','SQLITE_ROW_COUNT'," +
                    "'SQLITE_UPDATE_ROWS','SQLITE_PAYLOAD_SIZE','ZIP_MEMBER_COUNT','ZIP_MEMBER_SIZE','GROUP_STEPS']}))",
                    options.CorpusScript
                ],
                cancellationToken
            );

            using var generator = JsonDocument.Parse(probe);

            for (int index = 0; index < groups.Count; index++)
            {
                var group = groups[index];

                if (group.Name == "natural_zstd")
                {
                    continue;
                }

                if (generator.RootElement.GetProperty("GROUP_STEPS").GetProperty(group.Name).GetInt32() != group.Updates)
                {
                    throw new InvalidDataException("Generator and manifest disagree on group length.");
                }

                if (group.Name == "jsonl"
                    && (group.Files.Count != generator.RootElement.GetProperty("JSONL_FILE_COUNT").GetInt32()
                        || group.Files.Any(file => file.Fingerprint.Length != generator.RootElement.GetProperty("JSONL_FILE_SIZE").GetInt64())
                        || !group.Files.Select(file => file.RelativePath).SequenceEqual(
                            Enumerable.Range(0, group.Files.Count)
                                .Select(number => $"jsonl/records_{number:D4}.jsonl")
                        )))
                {
                    throw new InvalidDataException("Generator and manifest disagree on JSONL layout.");
                }

                foreach (var file in group.Files)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    string path = GroupBundle.Resolve(Path.Combine(root, "base"), file.RelativePath);
                    GroupBundle.EnsureRegularFile(path);
                    FileOperations.RequireMatch(path, file.Fingerprint, cancellationToken);
                }

                string history = Path.Combine(root, "history", group.Name + ".jsonl");
                ValidateOperations(history, group, generator.RootElement);

                foreach (string profile in options.ReadProfiles)
                {
                    _ = CorpusReadSchedule.Load(
                        Path.Combine(root, "history", group.Name + "." + profile + ".jsonl"),
                        group.Name,
                        group.Updates
                    );
                }

                groups[index] = group with
                {
                    HistorySha256 = FileOperations.Fingerprint(history, cancellationToken).Sha256
                };
            }
        }

        if (options.Groups.Contains("natural_zstd"))
        {
            string naturalRoot = Path.Combine(root, "natural_zstd");
            using var document = JsonDocument.Parse(File.ReadAllText(Path.Combine(naturalRoot, "commits.json")));
            var manifest = document.RootElement;

            CheckHash(
                Path.Combine(naturalRoot, "initial.tar"),
                manifest.GetProperty("initial_tar_sha256").GetString()!,
                cancellationToken
            );

            foreach (var version in manifest.GetProperty("versions").EnumerateArray().Skip(1))
            {
                CheckHash(
                    GroupBundle.Resolve(naturalRoot, version.GetProperty("patch").GetString()!),
                    version.GetProperty("patch_sha256").GetString()!,
                    cancellationToken
                );
            }

            _ = await new ExternalProcess("git", options.ProcessTimeoutSeconds).RunAsync(["--version"], cancellationToken);
        }

        foreach (var group in groups)
        {
            if (group.InitialContentBytes > options.MaximumGroupBytes
                || group.InitialFileCount < 1)
            {
                throw new InvalidDataException("Group is empty or exceeds maximumGroupBytes: " + group.Name);
            }
        }

        return new CorpusCatalog(new CorpusValidationReport(
            root,
            runtime,
            corpusHash,
            naturalHash,
            groups
        ));
    }

    private static void ValidateOperations(
        string file,
        CorpusGroup group,
        JsonElement generator)
    {
        int count = 0;

        foreach (string line in File.ReadLines(file))
        {
            using var document = JsonDocument.Parse(line);
            var operation = document.RootElement;

            if (operation.GetProperty("group").GetString() != group.Name
                || operation.GetProperty("step").GetInt32() != ++count)
            {
                throw new InvalidDataException("Invalid operation order: " + file);
            }

            string? kind = operation.GetProperty("operation").GetString();

            if (group.Name == "jsonl")
            {
                int total = generator.GetProperty("JSONL_FILE_COUNT").GetInt32();

                int[] expected = Enumerable.Range(0, JsonlFilesPerUpdate)
                    .Select(offset => (count * JsonlStepMultiplier + offset * JsonlFileStride) % total)
                    .ToArray();

                int[] actual = operation.GetProperty("file_indices")
                    .EnumerateArray()
                    .Select(item => item.GetInt32())
                    .ToArray();

                if (kind != "replace_jsonl_files"
                    || !actual.SequenceEqual(expected))
                {
                    throw new InvalidDataException("JSONL recipe differs from the supported generator.");
                }
            }
            else if (group.Name == "sqlite")
            {
                if (kind != "update_rows"
                    || operation.GetProperty("row_count").GetInt32() != generator.GetProperty("SQLITE_UPDATE_ROWS").GetInt32()
                    || operation.GetProperty("first").GetInt32() != (count * SqliteStepMultiplier) % generator.GetProperty("SQLITE_ROW_COUNT").GetInt32()
                    || operation.GetProperty("stride").GetInt32() != SqliteRowStride)
                {
                    throw new InvalidDataException("SQLite recipe differs from the supported generator.");
                }
            }
            else if (kind != "replace_zip_member"
                || operation.GetProperty("member_index").GetInt32() != (count * ZipStepMultiplier) % generator.GetProperty("ZIP_MEMBER_COUNT").GetInt32())
            {
                throw new InvalidDataException("ZIP recipe differs from the supported generator.");
            }
        }

        if (count != group.Updates)
        {
            throw new InvalidDataException("Update history is incomplete: " + file);
        }
    }

    private static void CheckHash(
        string file,
        string expected,
        CancellationToken cancellationToken)
    {
        if (!FileOperations.Fingerprint(file, cancellationToken).Sha256.Equals(expected, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException("Source SHA-256 mismatch: " + file);
        }
    }

    private static void RequireFile(string file, ICollection<string> missing)
    {
        if (!File.Exists(file))
        {
            missing.Add(file);
        }
    }
}