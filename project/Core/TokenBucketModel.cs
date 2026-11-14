namespace S3SyncLab.Core;

// This is a virtual-time model. It neither sleeps nor limits the real network.
public sealed class TokenBucketModel
{
    public double Capacity { get; }
    public double RefillBytesPerSecond { get; }
    public double PeakBytesPerSecond { get; }
    public double Tokens { get; private set; }
    public double ElapsedSeconds { get; private set; }

    public TokenBucketModel(double capacity, double refill, double peak)
    {
        if (!double.IsFinite(capacity)
            || !double.IsFinite(refill)
            || !double.IsFinite(peak)
            || capacity <= 0
            || refill <= 0
            || peak < refill)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity));
        }

        Capacity = capacity;
        RefillBytesPerSecond = refill;
        PeakBytesPerSecond = peak;
        Tokens = capacity;
    }

    public void Idle(double seconds)
    {
        if (!double.IsFinite(seconds) || seconds < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(seconds));
        }

        Tokens = Math.Min(Capacity, Tokens + RefillBytesPerSecond * seconds);
        ElapsedSeconds += seconds;
    }

    public TransferEstimate Transfer(long bytes)
    {
        if (bytes < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bytes));
        }

        double before = Tokens;
        double seconds = Math.Max(
            bytes / PeakBytesPerSecond,
            Math.Max(0, bytes - before) / RefillBytesPerSecond
        );

        // Refill continues during transmission, including transfers larger than B.
        Tokens = Math.Clamp(before + RefillBytesPerSecond * seconds - bytes, 0, Capacity);
        ElapsedSeconds += seconds;

        return new TransferEstimate(bytes, before, seconds, Tokens);
    }
}