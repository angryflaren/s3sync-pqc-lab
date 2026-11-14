using System.Globalization;
using S3SyncLab.Core;

namespace S3SyncLab.Application;

public sealed record WriteMeasurement(
    int Version,
    string Kind,
    string Reason,
    double Threshold,
    double DecisionTokens,
    long FullCandidateBytes,
    long DeltaCandidateBytes,
    long PacketBytes,
    long HeadBytes,
    long ChainBytes,
    int DeltaCount,
    double PrepareMs,
    double FingerprintMs,
    double DeltaMs,
    double ProtectionMs,
    double PacketPutMs,
    double HeadPutMs,
    double WriteMs,
    double MaintenanceMs,
    double ModelTransferSeconds,
    double ModelTokensAfter,
    double DecisionBudgetTokensAfter,
    double BudgetExcessBytes,
    string TargetSha256
);

public sealed record ReadMeasurement(
    int Version,
    long ChainBytes,
    int DeltaCount,
    RestoreResult Result
);

public sealed record RunSummary(
    string Run,
    string Policy,
    int Repetition,
    int Updates,
    int FullUpdates,
    int DeltaUpdates,
    long InitialUploadBytes,
    long UpdateUploadBytes,
    long TotalUploadBytes,
    long MaximumChainBytes,
    int MaximumDeltaCount,
    double UpdateWriteP50Ms,
    double UpdateWriteP95Ms,
    double UpdateWriteMeanMs,
    double UpdateModelTransferSeconds,
    int RestoreCount,
    double RestoreP50Ms,
    double RestoreP95Ms,
    double TotalBudgetExcessBytes,
    long FinalFileBytes,
    string FinalSha256
);

internal sealed class CsvLog : IDisposable
{
    private readonly StreamWriter writer;

    public CsvLog(string file, string header)
    {
        writer = new StreamWriter(
            new FileStream(file, FileMode.CreateNew, FileAccess.Write, FileShare.Read),
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false)
        );

        writer.WriteLine(header);
        writer.Flush();
    }

    public void Append(params object[] values)
    {
        writer.WriteLine(string.Join(",", values.Select(value => Escape(
            value is IFormattable number
                ? number.ToString(null, CultureInfo.InvariantCulture)
                : value.ToString() ?? ""
        ))));

        writer.Flush();
    }

    private static string Escape(string value) =>
        value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) < 0
            ? value
            : "\"" + value.Replace("\"", "\"\"") + "\"";

    public void Dispose() => writer.Dispose();
}

public static class MeasurementSummary
{
    public static RunSummary Create(
        string run,
        string policy,
        int repetition,
        IReadOnlyList<WriteMeasurement> writes,
        IReadOnlyList<ReadMeasurement> reads,
        FileFingerprint final)
    {
        var updates = writes.Where(row => row.Version > 0).ToArray();

        return new RunSummary(
            run,
            policy,
            repetition,
            updates.Length,
            updates.Count(row => row.Kind == nameof(PacketKind.Full)),
            updates.Count(row => row.Kind == nameof(PacketKind.Delta)),
            writes[0].PacketBytes + writes[0].HeadBytes,
            updates.Sum(row => row.PacketBytes + row.HeadBytes),
            writes.Sum(row => row.PacketBytes + row.HeadBytes),
            writes.Max(row => row.ChainBytes),
            writes.Max(row => row.DeltaCount),
            Quantile(updates.Select(row => row.WriteMs), 0.5),
            Quantile(updates.Select(row => row.WriteMs), 0.95),
            updates.Average(row => row.WriteMs),
            updates.Sum(row => row.ModelTransferSeconds),
            reads.Count,
            Quantile(reads.Select(row => row.Result.TotalMs), 0.5),
            Quantile(reads.Select(row => row.Result.TotalMs), 0.95),
            writes.Sum(row => row.BudgetExcessBytes),
            final.Length,
            final.Sha256
        );
    }

    // Linear interpolation between adjacent sorted observations (type 7 quantile).
    public static double Quantile(IEnumerable<double> values, double probability)
    {
        var ordered = values.OrderBy(value => value).ToArray();

        if (ordered.Length == 0 || probability < 0 || probability > 1)
        {
            throw new ArgumentOutOfRangeException(nameof(values));
        }

        double index = (ordered.Length - 1) * probability;
        int lower = (int)Math.Floor(index);
        int upper = (int)Math.Ceiling(index);

        return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower);
    }
}