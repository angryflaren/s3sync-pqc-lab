using System.Text.Json;
using S3SyncLab.Infrastructure;

namespace S3SyncLab.Configuration;

public sealed class ExperimentTwoOptions
{
    public string DatasetDirectory { get; set; } = "data/s3sync_corpus_v1";

    public string CorpusScript { get; set; } = "data/s3sync_corpus_v1/corpus_tool.py";

    public string NaturalScript { get; set; } = "data/s3sync_corpus_v1/natural_tool.py";

    public string PythonExecutable { get; set; } = "python3";

    public string OutputDirectory { get; set; } = "results-experiment2";

    public string[] Groups { get; set; } = ["jsonl", "sqlite", "zip01"];

    public string[] ReadProfiles { get; set; } = ["read_light", "read_heavy"];

    public int? UpdateLimit { get; set; }

    public int Repetitions { get; set; } = 1;

    public int MaximumDeltas { get; set; } = 128;

    public int ChunkBytes { get; set; } = 4096;

    public int ProcessTimeoutSeconds { get; set; } = 600;

    public long MaximumGroupBytes { get; set; } = 256L * 1024 * 1024;

    public double MinimumThreshold { get; set; } = 2;

    public double MaximumThreshold { get; set; } = 4;

    public double InterTrialIdleSeconds { get; set; }

    public bool PruneReplacedChains { get; set; } = true;

    public StorageOptions Storage { get; set; } = new()
    {
        Provider = "s3"
    };

    public NetworkOptions Network { get; set; } = new();

    public static ExperimentTwoOptions Load(string file)
    {
        string absolute = Path.GetFullPath(file);

        var options = JsonSerializer.Deserialize<ExperimentTwoOptions>(
            File.ReadAllText(absolute),
            LabOptions.Json
        ) ?? throw new InvalidDataException("Empty experiment 2 configuration.");

        string root = Path.GetDirectoryName(absolute)!;

        options.DatasetDirectory = Path.GetFullPath(options.DatasetDirectory, root);
        options.CorpusScript = Path.GetFullPath(options.CorpusScript, root);
        options.NaturalScript = Path.GetFullPath(options.NaturalScript, root);
        options.OutputDirectory = Path.GetFullPath(options.OutputDirectory, root);
        options.Storage.LocalDirectory = Path.GetFullPath(options.Storage.LocalDirectory, root);

        options.Validate();

        return options;
    }

    public void Validate()
    {
        string[] knownGroups = ["jsonl", "sqlite", "zip01", "zip02", "zip03", "natural_zstd"];

        if (Groups is null
            || Groups.Length == 0
            || Groups.Distinct().Count() != Groups.Length
            || Groups.Any(group => !knownGroups.Contains(group)))
        {
            throw new InvalidDataException("Groups must be unique: jsonl, sqlite, zip01, zip02, zip03, natural_zstd.");
        }

        if (ReadProfiles is null
            || ReadProfiles.Length == 0
            || ReadProfiles.Distinct().Count() != ReadProfiles.Length
            || ReadProfiles.Any(profile => profile is not ("read_light" or "read_heavy")))
        {
            throw new InvalidDataException("Read profiles must be unique: read_light, read_heavy.");
        }

        if (UpdateLimit is <= 0
            || Repetitions < 1
            || Repetitions > 100
            || ProcessTimeoutSeconds < 1
            || MaximumGroupBytes < 1024
            || MaximumGroupBytes > 3L * 1024 * 1024 * 1024
            || !double.IsFinite(InterTrialIdleSeconds)
            || InterTrialIdleSeconds < 0
            || InterTrialIdleSeconds > 86400)
        {
            throw new InvalidDataException("Invalid experiment 2 limits.");
        }

        // Reuse all common limits and path-isolation checks without changing experiment 1 options.
        CreateTrialOptions(1, 20).Validate();
    }

    public LabOptions CreateTrialOptions(int updates, int readPeriod) => new()
    {
        DatasetDirectory = DatasetDirectory,
        HistoryScript = CorpusScript,
        PythonExecutable = PythonExecutable,
        OutputDirectory = OutputDirectory,
        Updates = updates,
        Repetitions = Repetitions,
        RestoreEvery = readPeriod,
        MaximumDeltas = MaximumDeltas,
        ChunkBytes = ChunkBytes,
        ProcessTimeoutSeconds = ProcessTimeoutSeconds,
        MinimumThreshold = MinimumThreshold,
        MaximumThreshold = MaximumThreshold,
        PruneReplacedChains = PruneReplacedChains,
        Storage = Storage,
        Network = Network
    };
}