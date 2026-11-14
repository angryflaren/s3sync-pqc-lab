using S3SyncLab.Core;
using S3SyncLab.Infrastructure;

namespace S3SyncLab.Application;

public sealed class RestoreEngine
{
    private const int MaximumHeadBytes = 4 * 1024 * 1024;

    private readonly IObjectStore store;
    private readonly IDeltaCodec delta;
    private readonly Func<IPacketReader> readerFactory;
    private readonly Guid group;
    private readonly string objectPrefix;
    private readonly int maximumDeltas;

    public RestoreEngine(
        IObjectStore store,
        IDeltaCodec delta,
        Func<IPacketReader> readerFactory,
        Guid group,
        string objectPrefix,
        int maximumDeltas)
    {
        this.store = store;
        this.delta = delta;
        this.readerFactory = readerFactory;
        this.group = group;
        this.objectPrefix = objectPrefix;
        this.maximumDeltas = maximumDeltas;
    }

    public async Task<RestoreResult> RestoreAsync(
        string headKey,
        string pinnedRoot,
        string destinationFile,
        string scratchParent,
        CancellationToken cancellationToken)
    {
        if (File.Exists(destinationFile))
        {
            throw new IOException("Restoration requires a new destination file.");
        }

        string scratch = Path.Combine(scratchParent, "restore-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(scratch);

        var total = Stopwatch.StartNew();
        double downloadMs = 0;
        double verifyMs = 0;
        double applyMs = 0;
        long downloadBytes = 0;

        try
        {
            string headFile = Path.Combine(scratch, "head.json");
            var timer = Stopwatch.StartNew();

            await store.GetFileAsync(headKey, headFile, MaximumHeadBytes, cancellationToken);
            downloadMs += timer.Elapsed.TotalMilliseconds;
            downloadBytes += new FileInfo(headFile).Length;

            var head = FileOperations.ReadJson<HeadDocument>(headFile);
            ValidateHead(head, pinnedRoot);

            using var reader = readerFactory(); // No cached epoch key or cached plaintext.

            string current = Path.Combine(scratch, "current.sqlite");
            FileFingerprint? fingerprint = null;
            string? previousRoot = null;
            int previousVersion = -1;

            foreach (var item in head.Objects)
            {
                cancellationToken.ThrowIfCancellationRequested();

                string packetFile = Path.Combine(scratch, "packet.bin");
                string payloadFile = Path.Combine(scratch, "payload.bin");
                string nextFile = Path.Combine(scratch, "next.sqlite");

                timer.Restart();
                await store.GetFileAsync(item.Key, packetFile, item.Bytes, cancellationToken);
                downloadMs += timer.Elapsed.TotalMilliseconds;

                if (new FileInfo(packetFile).Length != item.Bytes)
                {
                    throw new InvalidDataException("Object length mismatch.");
                }

                downloadBytes += item.Bytes;

                timer.Restart();
                var opened = reader.Open(packetFile, payloadFile, item.Root, cancellationToken);
                verifyMs += timer.Elapsed.TotalMilliseconds;

                if (opened.Group != group
                    || opened.Version != item.Version
                    || opened.Kind != item.Kind
                    || (previousRoot is not null && (opened.PreviousRoot != previousRoot || opened.Version != previousVersion + 1)))
                {
                    throw new InvalidDataException("Broken version chain.");
                }

                timer.Restart();

                if (opened.Kind == PacketKind.Full)
                {
                    if (fingerprint is not null)
                    {
                        throw new InvalidDataException("Unexpected full snapshot inside a chain.");
                    }

                    File.Move(payloadFile, nextFile);
                }
                else
                {
                    if (fingerprint is null || fingerprint.Sha256 != opened.BasisSha256)
                    {
                        throw new InvalidDataException("Delta does not reference the previous file.");
                    }

                    delta.Apply(current, payloadFile, nextFile, cancellationToken);
                    FileOperations.RequireMatch(nextFile, opened.Target, cancellationToken);
                }

                File.Move(nextFile, current, overwrite: true);

                fingerprint = opened.Target;
                previousRoot = item.Root;
                previousVersion = item.Version;

                applyMs += timer.Elapsed.TotalMilliseconds;

                FileOperations.DeleteIfExists(packetFile);
                FileOperations.DeleteIfExists(payloadFile);
            }

            if (fingerprint is null || previousVersion != head.Version || previousRoot != pinnedRoot)
            {
                throw new InvalidDataException("Restoration did not reach the pinned target.");
            }

            await FileOperations.CopyAtomicAsync(current, destinationFile, cancellationToken, overwrite: false);

            return new RestoreResult(
                downloadBytes,
                head.Objects.Count + 1,
                downloadMs,
                verifyMs,
                applyMs,
                total.Elapsed.TotalMilliseconds,
                fingerprint
            );
        }
        finally
        {
            Directory.Delete(scratch, recursive: true);
        }
    }

    private void ValidateHead(HeadDocument head, string pinnedRoot)
    {
        if (head.Format != 1
            || head.Group != group.ToString("N")
            || head.Root != pinnedRoot
            || head.Objects is null
            || head.Objects.Count < 1
            || head.Objects.Count > maximumDeltas + 1
            || head.Objects[0].Kind != PacketKind.Full
            || head.Objects[^1].Root != head.Root
            || head.Objects[^1].Version != head.Version
            || head.Objects.Select(item => item.Key).Distinct().Count() != head.Objects.Count)
        {
            throw new InvalidDataException("Invalid HEAD or unexpected target root.");
        }

        for (int index = 0; index < head.Objects.Count; index++)
        {
            var item = head.Objects[index];
            FileOperations.ValidateObjectKey(item.Key);

            if (!item.Key.StartsWith(objectPrefix + "/objects/", StringComparison.Ordinal)
                || item.Bytes < 1
                || item.Bytes > 9L * 1024 * 1024 * 1024
                || (index > 0 && (item.Kind != PacketKind.Delta || item.Version != head.Objects[index - 1].Version + 1)))
            {
                throw new InvalidDataException("HEAD contains an invalid object reference.");
            }
        }
    }
}