using S3SyncLab.Core;

namespace S3SyncLab.Application;

public sealed record RunIdentity(
    int Format,
    string Group,
    string ObjectPrefix,
    string HeadKey
);

public sealed record RunCheckpoint(
    int Format,
    string Status,
    string Group,
    string ObjectPrefix,
    string HeadKey,
    int Version,
    string Root,
    FileFingerprint File,
    HeadDocument Head
);

internal sealed class ObjectJournal : IDisposable
{
    private readonly StreamWriter writer;

    public ObjectJournal(string path)
    {
        writer = new StreamWriter(
            new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.Read),
            new UTF8Encoding(false)
        );
    }

    public void Record(string key)
    {
        Infrastructure.FileOperations.ValidateObjectKey(key);

        writer.WriteLine(key);
        writer.Flush();

        ((FileStream)writer.BaseStream).Flush(flushToDisk: true);
    }

    public void Dispose() => writer.Dispose();
}