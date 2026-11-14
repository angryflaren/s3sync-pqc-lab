namespace S3SyncLab.Core;

public abstract class ThresholdPolicy : ISnapshotPolicy
{
    public abstract string Name { get; }

    protected abstract double GetThreshold(DecisionInput input);

    public PolicyDecision Decide(DecisionInput input)
    {
        if (input.FullBytes <= 0
            || input.DeltaBytes < 0
            || input.Capacity <= 0
            || input.Chain.Bytes < 0
            || input.Chain.DeltaCount < 0
            || input.MaximumDeltas < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(input));
        }

        double threshold = GetThreshold(input);

        if (input.Chain.Bytes == 0)
        {
            return new(PacketKind.Full, threshold, "initial");
        }

        if (input.DeltaBytes >= input.FullBytes)
        {
            return new(PacketKind.Full, threshold, "delta-not-smaller");
        }

        if ((double)input.Chain.Bytes + input.DeltaBytes >= threshold * input.FullBytes)
        {
            return new(PacketKind.Full, threshold, "chain-bytes");
        }

        if (input.Chain.DeltaCount >= input.MaximumDeltas)
        {
            return new(PacketKind.Full, threshold, "chain-length");
        }

        return new(PacketKind.Delta, threshold, "delta");
    }
}

public sealed class FixedThresholdPolicy : ThresholdPolicy
{
    private readonly double threshold;

    public FixedThresholdPolicy(double threshold)
    {
        if (!double.IsFinite(threshold) || threshold < 2)
        {
            throw new ArgumentOutOfRangeException(nameof(threshold));
        }

        this.threshold = threshold;
    }

    public override string Name => "fixed-" + threshold.ToString(System.Globalization.CultureInfo.InvariantCulture);

    protected override double GetThreshold(DecisionInput input) => threshold;
}

public sealed class AdaptiveThresholdPolicy : ThresholdPolicy
{
    private readonly double minimum;
    private readonly double maximum;

    public AdaptiveThresholdPolicy(double minimum, double maximum)
    {
        if (!double.IsFinite(minimum)
            || !double.IsFinite(maximum)
            || minimum < 2
            || maximum < minimum)
        {
            throw new ArgumentOutOfRangeException(nameof(minimum));
        }

        this.minimum = minimum;
        this.maximum = maximum;
    }

    public override string Name => "adaptive";

    protected override double GetThreshold(DecisionInput input)
    {
        double fraction = Math.Clamp(input.AvailableTokens / input.Capacity, 0, 1);

        return minimum + (maximum - minimum) * (1 - fraction);
    }
}