using System.Security.Cryptography;
using Org.BouncyCastle.Crypto.Digests;
using Org.BouncyCastle.Crypto.Engines;
using Org.BouncyCastle.Crypto.Modes;
using Org.BouncyCastle.Crypto.Parameters;

namespace S3SyncLab.Infrastructure.Crypto;

internal static class PacketCipher
{
    private static readonly byte[] EncryptionLabel = Encoding.ASCII.GetBytes("Commit")
        .Concat(new byte[] { 4, 1 })
        .ToArray();

    private static readonly byte[] CommitmentLabel = Encoding.ASCII.GetBytes("Commit")
        .Concat(new byte[] { 4, 2 })
        .ToArray();

    public static byte[] Commitment(byte[] epochKey, byte[] nonce)
        => SHA384.HashData(epochKey.Concat(CommitmentLabel).Concat(nonce).ToArray());

    public static byte[] EncryptionKey(byte[] epochKey, byte[] nonce)
        => SHA256.HashData(epochKey.Concat(EncryptionLabel).Concat(nonce).ToArray());

    public static byte[] Nonce(int version)
    {
        var nonce = new byte[12];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(nonce.AsSpan(4), version);

        return nonce;
    }

    public static GcmBlockCipher Create(
        bool encrypt,
        byte[] epochKey,
        byte[] nonce,
        byte[] aad)
    {
        byte[] key = EncryptionKey(epochKey, nonce);

        try
        {
            var cipher = new GcmBlockCipher(new AesEngine());
            cipher.Init(
                encrypt,
                new AeadParameters(new KeyParameter(key), PacketFormat.TagBytes * 8, nonce, aad));

            return cipher;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
        }
    }

    public static Sha3Digest RootDigest(byte[] header)
    {
        var digest = new Sha3Digest(384);
        byte[] prefix = CryptoEncoding.Frame("S3LAB/v1/packet-root", header);
        digest.BlockUpdate(prefix, 0, prefix.Length);

        return digest;
    }

    public static byte[] FinishRoot(Sha3Digest digest)
    {
        var root = new byte[PacketFormat.RootBytes];
        digest.DoFinal(root, 0);

        return root;
    }

    public static FileStream CreatePrivate(string file)
    {
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            BufferSize = FileOperations.BufferBytes,
            Options = FileOptions.SequentialScan
        };

        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        }

        return new FileStream(file, options);
    }
}