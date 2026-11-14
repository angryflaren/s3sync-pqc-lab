using System.Security.Cryptography;
using S3SyncLab.Core;

namespace S3SyncLab.Infrastructure.Crypto;

public sealed class HybridPacketWriter : IPacketWriter
{
    public static string ZeroRoot { get; } = new('0', 96);

    public static FileFingerprint NoBasis { get; } = new(0, new string('0', 64));

    private readonly Guid group;
    private readonly LabKeys keys;
    private readonly object gate = new();
    private HybridEpoch? epoch;
    private int lastVersion = -1;
    private bool faulted;
    private bool disposed;

    public HybridPacketWriter(Guid group, LabKeys keys)
    {
        this.group = group;
        this.keys = keys;
    }

    public long Measure(long payloadBytes, PacketKind kind) => PacketFormat.Measure(payloadBytes, kind);

    public ProtectedPacket Protect(
        PacketRequest request,
        string outputFile,
        CancellationToken cancellationToken)
    {
        lock (gate)
        {
            return ProtectCore(request, outputFile, cancellationToken);
        }
    }

    private ProtectedPacket ProtectCore(
        PacketRequest request,
        string outputFile,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (faulted)
        {
            throw new InvalidOperationException("A failed writer cannot reuse an epoch. Start a new run.");
        }

        if (request.Version != lastVersion + 1
            || (epoch is null && request.Kind != PacketKind.Full))
        {
            throw new InvalidOperationException("Packets must start with full version 0 and advance sequentially.");
        }

        long payloadBytes = new FileInfo(request.PayloadFile).Length;
        long expectedBytes = Measure(payloadBytes, request.Kind);

        faulted = true; // Fail closed: a partial encryption must never reuse its nonce.

        if (request.Kind == PacketKind.Full)
        {
            epoch?.Dispose();
            epoch = HybridEpoch.Create(group, keys);
        }

        var active = epoch ?? throw new InvalidOperationException("Missing encryption epoch.");
        byte[] nonce = PacketCipher.Nonce(request.Version);

        var header = new PacketHeader(
            group,
            active.Id,
            request.Version,
            request.Kind,
            CryptoEncoding.ParseHex(request.PreviousRoot, 48),
            active.DescriptorHash,
            keys.SignerId,
            nonce,
            PacketCipher.Commitment(active.Key, nonce),
            checked(payloadBytes + PacketFormat.InnerBytes + PacketFormat.TagBytes),
            request.Kind == PacketKind.Full ? active.Descriptor : Array.Empty<byte>());

        byte[] encoded = PacketFormat.EncodeHeader(header);
        var cipher = PacketCipher.Create(true, active.Key, nonce, encoded);
        var digest = PacketCipher.RootDigest(encoded);

        byte[] inner = PacketFormat.EncodeInner(request.Target, request.Basis);
        var buffer = new byte[FileOperations.BufferBytes];
        var encrypted = new byte[FileOperations.BufferBytes + 32];
        bool createdOutput = false;

        try
        {
            using var input = FileOperations.OpenRead(request.PayloadFile);
            using var output = new FileStream(
                outputFile,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                FileOperations.BufferBytes,
                FileOptions.SequentialScan);

            createdOutput = true;
            output.Write(encoded);

            int written = cipher.ProcessBytes(inner, 0, inner.Length, encrypted, 0);
            WriteEncrypted(written);

            long readBytes = 0;
            int count;

            while ((count = input.Read(buffer, 0, buffer.Length)) != 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

                readBytes += count;
                written = cipher.ProcessBytes(buffer, 0, count, encrypted, 0);
                WriteEncrypted(written);
            }

            if (readBytes != payloadBytes)
            {
                throw new IOException("Payload changed while encrypting.");
            }

            written = cipher.DoFinal(encrypted, 0);
            WriteEncrypted(written);

            byte[] root = PacketCipher.FinishRoot(digest);
            var signatures = keys.Sign(root);

            if (signatures.Ed.Length != PacketFormat.EdSignatureBytes
                || signatures.Dsa.Length != PacketFormat.DsaSignatureBytes)
            {
                throw new CryptographicException("Unexpected signature size for the configured suite.");
            }

            output.Write(root);
            output.Write(signatures.Ed);
            output.Write(signatures.Dsa);

            if (output.Length != expectedBytes)
            {
                throw new InvalidDataException("Packet size estimator disagrees with serialization.");
            }

            output.Flush(flushToDisk: true);

            lastVersion = request.Version;
            faulted = false;

            return new ProtectedPacket(CryptoEncoding.Hex(root), output.Length, active.Id);

            void WriteEncrypted(int length)
            {
                output.Write(encrypted, 0, length);
                digest.BlockUpdate(encrypted, 0, length);
            }
        }
        catch
        {
            if (createdOutput)
            {
                FileOperations.DeleteIfExists(outputFile);
            }

            throw;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(buffer);
            CryptographicOperations.ZeroMemory(inner);
        }
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (disposed)
            {
                return;
            }

            epoch?.Dispose();
            disposed = true;
        }
    }
}