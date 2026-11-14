using S3SyncLab.Core;

namespace S3SyncLab.Infrastructure.Crypto;

internal sealed record PacketHeader(
    Guid Group,
    Guid Epoch,
    int Version,
    PacketKind Kind,
    byte[] PreviousRoot,
    byte[] EpochHash,
    byte[] SignerId,
    byte[] Nonce,
    byte[] Commitment,
    long CipherBytes,
    byte[] Descriptor);

internal static class PacketFormat
{
    public const int RootBytes = 48;

    public const int EdSignatureBytes = 64;

    public const int DsaSignatureBytes = 3309;

    public const int TagBytes = 16;

    public const int InnerBytes = sizeof(int) + sizeof(long) + 32 + 32;

    // Below the single-PUT S3 limit; the planned file is approximately 500 MB.
    public const long MaximumCipherBytes = 4L * 1024 * 1024 * 1024;

    public static readonly byte[] Magic = Encoding.ASCII.GetBytes("S3LAB001");

    private static readonly byte[] Suite = Encoding.ASCII.GetBytes("H1-X25519-MLKEM768-AES256GCM-ED25519-MLDSA65");

    public static byte[] EncodeHeader(PacketHeader header)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        writer.Write(Magic);
        writer.Write(0); // Filled below. This length is also covered by AAD and the root.
        CryptoEncoding.WriteBytes(writer, Suite);
        writer.Write(header.Group.ToByteArray());
        writer.Write(header.Epoch.ToByteArray());
        writer.Write(header.Version);
        writer.Write((byte)header.Kind);
        writer.Write(header.PreviousRoot);
        writer.Write(header.EpochHash);
        writer.Write(header.SignerId);
        writer.Write(header.Nonce);
        writer.Write(header.Commitment);
        writer.Write(header.CipherBytes);
        CryptoEncoding.WriteBytes(writer, header.Descriptor);
        writer.Flush();

        long length = stream.Length;
        stream.Position = Magic.Length;
        writer.Write(checked((int)length));
        writer.Flush();

        return stream.ToArray();
    }

    public static (PacketHeader Header, byte[] Encoded) ReadHeader(BinaryReader reader)
    {
        if (!CryptoEncoding.Equal(CryptoEncoding.ReadExactly(reader, Magic.Length), Magic))
        {
            throw new InvalidDataException("Unknown packet format.");
        }

        int length = reader.ReadInt32();

        if (length < 256
            || length > 4096)
        {
            throw new InvalidDataException("Invalid packet header length.");
        }

        reader.BaseStream.Position = 0;
        byte[] encoded = CryptoEncoding.ReadExactly(reader, length);

        using var data = new BinaryReader(new MemoryStream(encoded), Encoding.UTF8);
        data.BaseStream.Position = Magic.Length + sizeof(int);

        int suiteLength = data.ReadInt32();

        if (suiteLength != Suite.Length
            || !CryptoEncoding.Equal(CryptoEncoding.ReadExactly(data, suiteLength), Suite))
        {
            throw new InvalidDataException("Unsupported algorithm suite.");
        }

        Guid group = new(CryptoEncoding.ReadExactly(data, 16));
        Guid epoch = new(CryptoEncoding.ReadExactly(data, 16));
        int version = data.ReadInt32();
        var kind = (PacketKind)data.ReadByte();
        byte[] parent = CryptoEncoding.ReadExactly(data, 48);
        byte[] epochHash = CryptoEncoding.ReadExactly(data, 48);
        byte[] signer = CryptoEncoding.ReadExactly(data, 48);
        byte[] nonce = CryptoEncoding.ReadExactly(data, 12);
        byte[] commitment = CryptoEncoding.ReadExactly(data, 48);
        long cipherBytes = data.ReadInt64();
        int descriptorLength = data.ReadInt32();

        if (kind is not (PacketKind.Full or PacketKind.Delta)
            || version < 0
            || cipherBytes < InnerBytes + TagBytes
            || cipherBytes > MaximumCipherBytes
            || descriptorLength != (kind == PacketKind.Full ? HybridEpoch.DescriptorBytes : 0))
        {
            throw new InvalidDataException("Invalid packet metadata.");
        }

        byte[] descriptor = CryptoEncoding.ReadExactly(data, descriptorLength);

        if (data.BaseStream.Position != length)
        {
            throw new InvalidDataException("Unexpected header bytes.");
        }

        return (
            new PacketHeader(
                group,
                epoch,
                version,
                kind,
                parent,
                epochHash,
                signer,
                nonce,
                commitment,
                cipherBytes,
                descriptor),
            encoded);
    }

    public static long Measure(long payloadBytes, PacketKind kind)
    {
        if (payloadBytes < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(payloadBytes));
        }

        long cipher = checked(InnerBytes + payloadBytes + TagBytes);

        if (cipher > MaximumCipherBytes)
        {
            throw new InvalidDataException("Payload exceeds the experiment packet limit.");
        }

        var placeholder = new PacketHeader(
            Guid.Empty,
            Guid.Empty,
            0,
            kind,
            new byte[48],
            new byte[48],
            new byte[48],
            new byte[12],
            new byte[48],
            cipher,
            kind == PacketKind.Full ? new byte[HybridEpoch.DescriptorBytes] : Array.Empty<byte>());

        return checked(EncodeHeader(placeholder).Length + cipher + RootBytes + EdSignatureBytes + DsaSignatureBytes);
    }

    public static byte[] EncodeInner(FileFingerprint target, FileFingerprint basis)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(1);
        writer.Write(target.Length);
        writer.Write(CryptoEncoding.ParseHex(target.Sha256, 32));
        writer.Write(CryptoEncoding.ParseHex(basis.Sha256, 32));

        return stream.ToArray();
    }

    public static (FileFingerprint Target, string BasisHash) ReadInner(BinaryReader reader)
    {
        if (reader.ReadInt32() != 1)
        {
            throw new InvalidDataException("Unknown encrypted manifest format.");
        }

        long length = reader.ReadInt64();

        if (length < 0
            || length > MaximumCipherBytes)
        {
            throw new InvalidDataException("Invalid target length.");
        }

        return (
            new FileFingerprint(length, CryptoEncoding.Hex(CryptoEncoding.ReadExactly(reader, 32))),
            CryptoEncoding.Hex(CryptoEncoding.ReadExactly(reader, 32)));
    }
}