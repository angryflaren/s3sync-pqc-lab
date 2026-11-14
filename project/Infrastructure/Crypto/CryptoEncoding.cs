using System.Security.Cryptography;
using Org.BouncyCastle.Crypto.Digests;

namespace S3SyncLab.Infrastructure.Crypto;

internal static class CryptoEncoding
{
    public static byte[] Frame(string domain, params byte[][] fields)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        WriteBytes(writer, Encoding.ASCII.GetBytes(domain));

        foreach (byte[] field in fields)
        {
            WriteBytes(writer, field);
        }

        writer.Flush();

        return stream.ToArray();
    }

    public static void WriteBytes(BinaryWriter writer, byte[] value)
    {
        writer.Write(value.Length);
        writer.Write(value);
    }

    public static byte[] Sha3(byte[] value)
    {
        var digest = new Sha3Digest(384);
        digest.BlockUpdate(value, 0, value.Length);

        var result = new byte[48];
        digest.DoFinal(result, 0);

        return result;
    }

    public static byte[] ParseHex(string text, int length)
    {
        byte[] bytes = Convert.FromHexString(text);

        if (bytes.Length != length)
        {
            throw new InvalidDataException("Invalid digest length.");
        }

        return bytes;
    }

    public static string Hex(byte[] bytes) => Convert.ToHexString(bytes).ToLowerInvariant();

    public static bool Equal(byte[] left, byte[] right) => CryptographicOperations.FixedTimeEquals(left, right);

    public static byte[] ReadExactly(BinaryReader reader, int count)
    {
        byte[] bytes = reader.ReadBytes(count);

        if (bytes.Length != count)
        {
            throw new EndOfStreamException();
        }

        return bytes;
    }
}