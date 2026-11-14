namespace S3SyncLab.Core;

public interface ISnapshotPolicy
{
    string Name { get; }

    PolicyDecision Decide(DecisionInput input);
}

public interface IDeltaCodec
{
    void Create(
        string basisFile,
        string targetFile,
        string deltaFile,
        CancellationToken cancellationToken
    );

    void Apply(
        string basisFile,
        string deltaFile,
        string outputFile,
        CancellationToken cancellationToken
    );
}

public interface IVersionHistory
{
    string CurrentFile { get; }

    Task<DatasetInfo> InitializeAsync(CancellationToken cancellationToken);

    Task AdvanceAsync(int expectedVersion, CancellationToken cancellationToken);
}

public interface IObjectStore : IAsyncDisposable
{
    string Description { get; }

    Task PutFileAsync(string key, string sourceFile, CancellationToken cancellationToken);

    Task GetFileAsync(
        string key,
        string destinationFile,
        long maximumBytes,
        CancellationToken cancellationToken
    );

    Task DeleteAsync(string key, CancellationToken cancellationToken);
}

public interface IExperiment
{
    Task RunAsync(CancellationToken cancellationToken);
}

public interface IPacketWriter : IDisposable
{
    long Measure(long payloadBytes, PacketKind kind);

    ProtectedPacket Protect(
        PacketRequest request,
        string outputFile,
        CancellationToken cancellationToken
    );
}

public interface IPacketReader : IDisposable
{
    OpenedPacket Open(
        string packetFile,
        string payloadFile,
        string expectedRoot,
        CancellationToken cancellationToken
    );
}