namespace S3SyncLab.Core;

// An estimate for a separately configured, controlled channel. S3 does not expose
// a provider's token balance. Request-body bytes also omit HTTP/TCP overhead.
public sealed class ObservedTokenBudget
{
    private readonly double capacity;
    private readonly double refill;
    private readonly Func<double> seconds;

    private double last;
    private double tokens;
    private double started;
    private bool active;

    public ObservedTokenBudget(double capacity, double refill, Func<double>? seconds = null)
    {
        if (!double.IsFinite(capacity)
            || !double.IsFinite(refill)
            || capacity <= 0
            || refill <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity));
        }

        this.capacity = capacity;
        this.refill = refill;
        this.seconds = seconds ?? (() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency);

        last = this.seconds();
        tokens = capacity;
    }

    public double Balance
    {
        get
        {
            if (active)
            {
                throw new InvalidOperationException("Cannot sample a transfer in progress.");
            }

            double now = seconds();
            tokens = Math.Min(capacity, tokens + refill * Math.Max(0, now - last));
            last = now;

            return tokens;
        }
    }

    public void StartTransfer()
    {
        _ = Balance;
        started = last;
        active = true;
    }

    // A positive return value flags excess bytes relative to the configured
    // budget. It is evidence of a mismatched model, not a negative token balance.
    public double CompleteTransfer(long bytes)
    {
        if (!active || bytes < 0)
        {
            throw new InvalidOperationException("No matching transfer start.");
        }

        double now = seconds();
        double after = tokens + refill * Math.Max(0, now - started) - bytes;

        tokens = Math.Clamp(after, 0, capacity);
        last = now;
        active = false;

        return Math.Max(0, -after);
    }
}