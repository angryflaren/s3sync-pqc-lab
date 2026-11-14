using S3SyncLab.Core;

namespace S3SyncLab.Infrastructure.Storage;

public sealed class LocalObjectStore : IObjectStore
{
    private readonly string directory;

    public LocalObjectStore(string directory)
    {
        this.directory = Path.GetFullPath(directory);
        Directory.CreateDirectory(this.directory);
    }

    public string Description => "local-filesystem";

    public Task PutFileAsync(
        string key,
        string sourceFile,
        CancellationToken cancellationToken)
        => FileOperations.CopyAtomicAsync(sourceFile, Resolve(key), cancellationToken);

    public Task GetFileAsync(
        string key,
        string destinationFile,
        long maximumBytes,
        CancellationToken cancellationToken)
    {
        string source = Resolve(key);

        if (new FileInfo(source).Length > maximumBytes)
        {
            throw new InvalidDataException("Object exceeds its permitted length.");
        }

        return FileOperations.CopyAtomicAsync(source, destinationFile, cancellationToken);
    }

    public Task DeleteAsync(string key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        FileOperations.DeleteIfExists(Resolve(key));

        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private string Resolve(string key) => Path.Combine(
        directory,
        FileOperations.ValidateObjectKey(key).Replace('/', Path.DirectorySeparatorChar));
}