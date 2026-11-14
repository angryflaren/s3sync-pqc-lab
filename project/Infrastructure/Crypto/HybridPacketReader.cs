using System.Security.Cryptography;
using S3SyncLab.Core;

namespace S3SyncLab.Infrastructure.Crypto;

public sealed class HybridPacketReader : IPacketReader
{
    private readonly Guid group;
    private readonly LabKeys keys;
    private readonly object gate = new();
    private HybridEpoch? epoch;
    private bool disposed;

    public HybridPacketReader(Guid group, LabKeys keys)
    {
        this.group = group;
        this.keys = keys;
    }

    public OpenedPacket Open(
        string packetFile,
        string payloadFile,
        string expectedRoot,
        CancellationToken cancellationToken)
    {
        lock (gate)
        {
            return OpenCore(packetFile, payloadFile, expectedRoot, cancellationToken);
        }
    }

    private OpenedPacket OpenCore(
        string packetFile,
        string payloadFile,
        string expectedRoot,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        string temporary = payloadFile + ".verified-inner-" + Guid.NewGuid().ToString("N");
        bool createdPayload = false;

        try
        {
            using var input = FileOperations.OpenRead(packetFile);
            using var reader = new BinaryReader(input, Encoding.UTF8, leaveOpen: true);

            var (header, encoded) = PacketFormat.ReadHeader(reader);

            if (header.Group != group
                || !CryptoEncoding.Equal(header.SignerId, keys.SignerId)
                || !CryptoEncoding.Equal(header.Nonce, PacketCipher.Nonce(header.Version)))
            {
                throw new CryptographicException("Packet context or trusted signing key does not match.");
            }

            long trailer = PacketFormat.RootBytes + PacketFormat.EdSignatureBytes + PacketFormat.DsaSignatureBytes;

            if (input.Length != encoded.Length + header.CipherBytes + trailer)
            {
                throw new InvalidDataException("Truncated packet or unexpected trailing bytes.");
            }

            // Verify the complete encrypted packet before spending effort on KEM or decryption.
            var digest = PacketCipher.RootDigest(encoded);
            var buffer = new byte[FileOperations.BufferBytes];
            long remaining = header.CipherBytes;

            while (remaining > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int count = input.Read(buffer, 0, (int)Math.Min(buffer.Length, remaining));

                if (count == 0)
                {
                    throw new EndOfStreamException();
                }

                digest.BlockUpdate(buffer, 0, count);
                remaining -= count;
            }

            byte[] root = PacketCipher.FinishRoot(digest);
            byte[] storedRoot = CryptoEncoding.ReadExactly(reader, PacketFormat.RootBytes);
            byte[] ed = CryptoEncoding.ReadExactly(reader, PacketFormat.EdSignatureBytes);
            byte[] dsa = CryptoEncoding.ReadExactly(reader, PacketFormat.DsaSignatureBytes);

            if (!CryptoEncoding.Equal(root, storedRoot)
                || !CryptoEncoding.Equal(root, CryptoEncoding.ParseHex(expectedRoot, 48)))
            {
                throw new CryptographicException("Packet root mismatch.");
            }

            keys.Verify(root, ed, dsa);

            if (header.Kind == PacketKind.Full)
            {
                if (!CryptoEncoding.Equal(CryptoEncoding.Sha3(header.Descriptor), header.EpochHash))
                {
                    throw new CryptographicException("Epoch descriptor hash mismatch.");
                }

                epoch?.Dispose();
                epoch = HybridEpoch.Open(group, header.Epoch, header.Descriptor, keys);
            }

            var active = epoch ?? throw new CryptographicException("Delta arrived without its full snapshot.");

            if (active.Id != header.Epoch
                || !CryptoEncoding.Equal(active.DescriptorHash, header.EpochHash)
                || !CryptoEncoding.Equal(PacketCipher.Commitment(active.Key, header.Nonce), header.Commitment))
            {
                throw new CryptographicException("Epoch or key commitment mismatch.");
            }

            var cipher = PacketCipher.Create(false, active.Key, header.Nonce, encoded);
            input.Position = encoded.Length;

            var plaintext = new byte[FileOperations.BufferBytes + 32];

            try
            {
                using var output = PacketCipher.CreatePrivate(temporary);
                remaining = header.CipherBytes;

                while (remaining > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    int count = input.Read(buffer, 0, (int)Math.Min(buffer.Length, remaining));

                    if (count == 0)
                    {
                        throw new EndOfStreamException();
                    }

                    int produced = cipher.ProcessBytes(buffer, 0, count, plaintext, 0);
                    output.Write(plaintext, 0, produced);
                    remaining -= count;
                }

                int finalBytes = cipher.DoFinal(plaintext, 0); // Rejects any invalid AES-GCM tag.
                output.Write(plaintext, 0, finalBytes);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(plaintext);
            }

            using var innerStream = FileOperations.OpenRead(temporary);
            using var innerReader = new BinaryReader(innerStream, Encoding.UTF8, leaveOpen: true);

            var inner = PacketFormat.ReadInner(innerReader);

            using (var payload = PacketCipher.CreatePrivate(payloadFile))
            {
                createdPayload = true;
                int count;

                while ((count = innerStream.Read(buffer, 0, buffer.Length)) != 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    payload.Write(buffer, 0, count);
                }
            }

            CryptographicOperations.ZeroMemory(buffer);

            if (header.Kind == PacketKind.Full)
            {
                FileOperations.RequireMatch(payloadFile, inner.Target, cancellationToken);
            }

            return new OpenedPacket(
                group,
                header.Version,
                header.Kind,
                CryptoEncoding.Hex(root),
                CryptoEncoding.Hex(header.PreviousRoot),
                inner.Target,
                inner.BasisHash);
        }
        catch
        {
            if (createdPayload)
            {
                FileOperations.DeleteIfExists(payloadFile);
            }

            epoch?.Dispose();
            epoch = null;

            throw;
        }
        finally
        {
            FileOperations.DeleteIfExists(temporary);
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