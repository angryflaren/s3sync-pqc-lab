using S3SyncLab.Core;
using S3SyncLab.Infrastructure.Corpus;

namespace S3SyncLab.Application;

public sealed record GroupReadMeasurement(
    int Version,
    int Files,
    long ContentBytes,
    long DownloadBytes,
    int GetCount,
    double RestoreMs,
    double ExtractVerifyMs,
    double TotalGroupReadMs);

public sealed class GroupReadObserver : IColdReadObserver, IDisposable
{
    private readonly string scratchParent;
    private readonly long maximumBytes;
    private readonly CsvLog csv;

    public List<GroupReadMeasurement> Measurements { get; } = new();

    public GroupReadObserver(string runDirectory, long maximumBytes)
    {
        scratchParent = runDirectory;
        this.maximumBytes = maximumBytes;

        csv = new CsvLog(
            Path.Combine(runDirectory, "group_reads.csv"),
            "version,files,content_bytes,download_body_bytes,get_count,restore_ms,extract_verify_ms,total_group_read_ms"
        );
    }

    public Task AfterRestoreAsync(
        int version,
        string restoredFile,
        RestoreResult result,
        CancellationToken cancellationToken)
    {
        string destination = Path.Combine(scratchParent, "extracted-" + Guid.NewGuid().ToString("N"));

        try
        {
            var timer = Stopwatch.StartNew();
            var info = GroupBundle.Extract(restoredFile, destination, maximumBytes, cancellationToken);
            double extractMs = timer.Elapsed.TotalMilliseconds;

            var row = new GroupReadMeasurement(
                version,
                info.Files,
                info.ContentBytes,
                result.DownloadBytes,
                result.ObjectCount,
                result.TotalMs,
                extractMs,
                result.TotalMs + extractMs
            );

            Measurements.Add(row);

            csv.Append(
                row.Version,
                row.Files,
                row.ContentBytes,
                row.DownloadBytes,
                row.GetCount,
                row.RestoreMs,
                row.ExtractVerifyMs,
                row.TotalGroupReadMs
            );

            return Task.CompletedTask;
        }
        finally
        {
            if (Directory.Exists(destination))
            {
                Directory.Delete(destination, recursive: true);
            }
        }
    }

    public void Dispose() => csv.Dispose();
}