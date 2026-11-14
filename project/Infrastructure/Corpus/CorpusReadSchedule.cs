using System.Text.Json;
using S3SyncLab.Core;

namespace S3SyncLab.Infrastructure.Corpus;

public sealed class CorpusReadSchedule : IColdReadSchedule
{
    private readonly HashSet<int> readVersions;

    public int Updates { get; }

    private CorpusReadSchedule(int updates, HashSet<int> readVersions)
    {
        Updates = updates;
        this.readVersions = readVersions;
    }

    public static CorpusReadSchedule Load(string file, string group, int expectedUpdates)
    {
        var reads = new HashSet<int>();
        int written = 0;

        foreach (string line in File.ReadLines(file))
        {
            using var document = JsonDocument.Parse(line);
            var item = document.RootElement;

            if (item.GetProperty("group").GetString() != group)
            {
                throw new InvalidDataException("Trace group mismatch: " + file);
            }

            int step = item.GetProperty("step").GetInt32();

            switch (item.GetProperty("action").GetString())
            {
                case "write" when step == written + 1:
                    written = step;
                    break;

                case "cold_restore" when step == written && step > 0 && reads.Add(step):
                    break;

                default:
                    throw new InvalidDataException("Trace must contain sequential writes and at most one read per version.");
            }
        }

        if (written != expectedUpdates
            || !reads.Contains(written))
        {
            throw new InvalidDataException("Incomplete read trace: " + file);
        }

        return new CorpusReadSchedule(expectedUpdates, reads);
    }

    public static CorpusReadSchedule Periodic(int updates, int period)
    {
        if (updates < 1
            || period < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(updates));
        }

        return new CorpusReadSchedule(
            updates,
            Enumerable.Range(1, updates)
                .Where(version => version % period == 0 || version == updates)
                .ToHashSet()
        );
    }

    public bool ShouldRead(int version, int finalVersion)
        => version == 0
            || version == finalVersion
            || readVersions.Contains(version);
}