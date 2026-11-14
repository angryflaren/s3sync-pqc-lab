namespace S3SyncLab.Core;

public enum PacketKind : byte
{
    Full = 1,
    Delta = 2
}

public sealed record FileFingerprint(
    long Length,
    string Sha256
);

public sealed record ChainState(
    long Bytes,
    int DeltaCount
)
{
    public static ChainState Empty { get; } = new(0, 0);

    public ChainState Add(PacketKind kind, long packetBytes) =>
        kind == PacketKind.Full
            ? new ChainState(packetBytes, 0)
            : new ChainState(checked(Bytes + packetBytes), checked(DeltaCount + 1));
}

public sealed record DecisionInput(
    long FullBytes,
    long DeltaBytes,
    ChainState Chain,
    double AvailableTokens,
    double Capacity,
    int MaximumDeltas
);

public sealed record PolicyDecision(
    PacketKind Kind,
    double Threshold,
    string Reason
);

public sealed record TransferEstimate(
    long Bytes,
    double TokensBefore,
    double Seconds,
    double TokensAfter
);

public sealed record ChainObject(
    string Key,
    string Root,
    long Bytes,
    int Version,
    PacketKind Kind
);

public sealed record HeadDocument(
    int Format,
    string Group,
    int Version,
    string Root,
    IReadOnlyList<ChainObject> Objects
);

public sealed record RestoreResult(
    long DownloadBytes,
    int ObjectCount,
    double DownloadMs,
    double VerifyDecryptMs,
    double ApplyMs,
    double TotalMs,
    FileFingerprint File
);

public sealed record DatasetInfo(
    string Name,
    int Updates,
    FileFingerprint Initial,
    string ManifestSha256,
    string HistorySha256,
    string Runtime
);

public sealed record PacketRequest(
    int Version,
    PacketKind Kind,
    string PreviousRoot,
    FileFingerprint Target,
    FileFingerprint Basis,
    string PayloadFile
);

public sealed record ProtectedPacket(
    string Root,
    long Bytes,
    Guid Epoch
);

public sealed record OpenedPacket(
    Guid Group,
    int Version,
    PacketKind Kind,
    string Root,
    string PreviousRoot,
    FileFingerprint Target,
    string BasisSha256
);