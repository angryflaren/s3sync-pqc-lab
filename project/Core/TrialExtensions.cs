namespace S3SyncLab.Core;

// Optional metrics for histories that also perform real client-side packaging.
public interface IPreparedVersionMetrics
{
    double DataGenerationMs { get; }
    double ClientPreparationMs { get; }
}

public interface IColdReadSchedule
{
    bool ShouldRead(int version, int finalVersion);
}

public interface IColdReadObserver
{
    Task AfterRestoreAsync(
        int version,
        string restoredFile,
        RestoreResult result,
        CancellationToken cancellationToken);
}