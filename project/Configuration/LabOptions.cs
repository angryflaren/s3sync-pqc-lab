using System.Text.Json;
using System.Text.Json.Serialization;

namespace S3SyncLab.Configuration;

public sealed class LabOptions
{
    public string DatasetDirectory { get; set; } = "data/single_file_experiment";
    public string PythonExecutable { get; set; } = "python3";
    public string HistoryScript { get; set; } = "data/single_file_experiment/single_file_tool.py";
    public string OutputDirectory { get; set; } = "results";
    public int Updates { get; set; } = 240;
    public int Repetitions { get; set; } = 3;
    public int RestoreEvery { get; set; } = 20;
    public int MaximumDeltas { get; set; } = 128;
    public int ChunkBytes { get; set; } = 4096;
    public int ProcessTimeoutSeconds { get; set; } = 600;
    public double MinimumThreshold { get; set; } = 2;
    public double MaximumThreshold { get; set; } = 4;
    public bool PruneReplacedChains { get; set; } = true;
    public StorageOptions Storage { get; set; } = new();
    public NetworkOptions Network { get; set; } = new();

    public static JsonSerializerOptions Json { get; } = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public static LabOptions Load(string file)
    {
        string absolute = Path.GetFullPath(file);
        var options = JsonSerializer.Deserialize<LabOptions>(File.ReadAllText(absolute), Json)
            ?? throw new InvalidDataException("Empty configuration.");

        string root = Path.GetDirectoryName(absolute)!;
        options.DatasetDirectory = Resolve(root, options.DatasetDirectory);
        options.HistoryScript = Resolve(root, options.HistoryScript);
        options.OutputDirectory = Resolve(root, options.OutputDirectory);
        options.Storage.LocalDirectory = Resolve(root, options.Storage.LocalDirectory);

        options.Validate();

        return options;
    }

    public void Validate()
    {
        if (Updates < 1
            || Repetitions < 1
            || Repetitions > 100
            || RestoreEvery < 1
            || MaximumDeltas < 0
            || MaximumDeltas > 4096
            || ChunkBytes < 128
            || ChunkBytes > 31744
            || ProcessTimeoutSeconds < 1)
        {
            throw new InvalidDataException("Invalid run limits or Octodiff chunk size.");
        }

        if (!double.IsFinite(MinimumThreshold)
            || !double.IsFinite(MaximumThreshold)
            || MinimumThreshold < 2
            || MaximumThreshold < MinimumThreshold)
        {
            throw new InvalidDataException("Require 2 <= minimumThreshold <= maximumThreshold.");
        }

        if (Storage.Provider is not ("local" or "s3"))
        {
            throw new InvalidDataException("storage.provider must be local or s3.");
        }

        if (Storage.Provider == "s3"
            && (!Uri.TryCreate(Storage.ServiceUrl, UriKind.Absolute, out var uri)
                || uri.Scheme is not ("http" or "https")
                || !string.IsNullOrEmpty(uri.UserInfo)
                || !string.IsNullOrEmpty(uri.Query)
                || string.IsNullOrWhiteSpace(Storage.Bucket)))
        {
            throw new InvalidDataException("Set the S3 endpoint and an existing bucket.");
        }

        Infrastructure.FileOperations.ValidateObjectKey(Storage.Prefix);

        if (Storage.TimeoutSeconds < 1)
        {
            throw new InvalidDataException("Invalid S3 timeout.");
        }

        Network.Validate();

        // A separate workspace prevents a run from modifying the original dataset.
        if (IsWithin(OutputDirectory, DatasetDirectory) || IsWithin(DatasetDirectory, OutputDirectory))
        {
            throw new InvalidDataException("Output and dataset directories must be separate.");
        }

        if (IsWithin(Storage.LocalDirectory, OutputDirectory)
            || IsWithin(OutputDirectory, Storage.LocalDirectory)
            || IsWithin(Storage.LocalDirectory, DatasetDirectory)
            || IsWithin(DatasetDirectory, Storage.LocalDirectory))
        {
            throw new InvalidDataException("Storage, output and dataset directories must be separate.");
        }
    }

    private static string Resolve(string root, string path) => Path.GetFullPath(path, root);

    private static bool IsWithin(string path, string parent)
    {
        var comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;

        string relative = Path.GetRelativePath(parent, path);

        return relative == "."
            || (!relative.StartsWith(".." + Path.DirectorySeparatorChar, comparison)
                && relative != ".."
                && !Path.IsPathRooted(relative));
    }
}

public sealed class StorageOptions
{
    public string Provider { get; set; } = "local";
    public string LocalDirectory { get; set; } = "objects";
    public string ServiceUrl { get; set; } = "http://127.0.0.1:8333";
    public string Bucket { get; set; } = "s3sync-lab";
    public string Region { get; set; } = "us-east-1";
    public string Prefix { get; set; } = "s3sync-lab";
    public int TimeoutSeconds { get; set; } = 1800;
}

public sealed class NetworkOptions
{
    public string DecisionBudgetMode { get; set; } = "virtual";

    // Explicit SI units: bytes and bytes/second, not bits or MiB.
    public double CapacityBytes { get; set; } = 536870912;
    public double RefillBytesPerSecond { get; set; } = 5242880;
    public double PeakBytesPerSecond { get; set; } = 52428800;
    public double IdleSecondsPerUpdate { get; set; } = 0;

    public void Validate()
    {
        _ = new Core.TokenBucketModel(CapacityBytes, RefillBytesPerSecond, PeakBytesPerSecond);

        if (DecisionBudgetMode is not ("virtual" or "wall-clock-estimate"))
        {
            throw new InvalidDataException("decisionBudgetMode must be virtual or wall-clock-estimate.");
        }

        if (!double.IsFinite(IdleSecondsPerUpdate) || IdleSecondsPerUpdate < 0)
        {
            throw new InvalidDataException("Invalid model idle time.");
        }
    }
}