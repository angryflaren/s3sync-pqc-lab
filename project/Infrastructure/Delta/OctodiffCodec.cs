using Octodiff.Core;
using Octodiff.Diagnostics;
using S3SyncLab.Core;

namespace S3SyncLab.Infrastructure.Delta;

public sealed class OctodiffCodec : IDeltaCodec
{
    private readonly short chunkBytes;

    public OctodiffCodec(int chunkBytes) => this.chunkBytes = checked((short)chunkBytes);

    public void Create(
        string basisFile,
        string targetFile,
        string deltaFile,
        CancellationToken cancellationToken)
    {
        string signature = deltaFile + ".signature-" + Guid.NewGuid().ToString("N");
        var progress = new CancellationProgress(cancellationToken);
        bool createdDelta = false;

        try
        {
            using (var basis = FileOperations.OpenRead(basisFile))
            using (var output = new FileStream(signature, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                new SignatureBuilder
                {
                    ChunkSize = chunkBytes,
                    ProgressReporter = progress
                }.Build(basis, new SignatureWriter(output));
            }

            using var target = FileOperations.OpenRead(targetFile);
            using var input = FileOperations.OpenRead(signature);
            using var delta = new FileStream(deltaFile, FileMode.CreateNew, FileAccess.Write, FileShare.None);

            createdDelta = true;

            new DeltaBuilder
            {
                ProgressReporter = progress
            }.BuildDelta(
                target,
                new SignatureReader(input, progress),
                new AggregateCopyOperationsDecorator(new BinaryDeltaWriter(delta)));

            cancellationToken.ThrowIfCancellationRequested();
        }
        catch
        {
            if (createdDelta)
            {
                FileOperations.DeleteIfExists(deltaFile);
            }

            throw;
        }
        finally
        {
            FileOperations.DeleteIfExists(signature);
        }
    }

    public void Apply(
        string basisFile,
        string deltaFile,
        string outputFile,
        CancellationToken cancellationToken)
    {
        var progress = new CancellationProgress(cancellationToken);
        bool createdOutput = false;

        try
        {
            using var basis = FileOperations.OpenRead(basisFile);
            using var delta = FileOperations.OpenRead(deltaFile);
            using var output = new FileStream(outputFile, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);

            createdOutput = true;

            new DeltaApplier
            {
                SkipHashCheck = false
            }.Apply(basis, new BinaryDeltaReader(delta, progress), output);

            cancellationToken.ThrowIfCancellationRequested();
        }
        catch
        {
            if (createdOutput)
            {
                FileOperations.DeleteIfExists(outputFile);
            }

            throw;
        }
    }

    private sealed class CancellationProgress(CancellationToken cancellationToken) : IProgressReporter
    {
        public void ReportProgress(string operation, long currentPosition, long total)
            => cancellationToken.ThrowIfCancellationRequested();
    }
}