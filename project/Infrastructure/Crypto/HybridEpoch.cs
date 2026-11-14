using System.Security.Cryptography;
using Org.BouncyCastle.Crypto.Digests;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Kems;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Security;

namespace S3SyncLab.Infrastructure.Crypto;

internal sealed class HybridEpoch : IDisposable
{
    public const int DescriptorBytes = 32 + 1088 + 32;

    public Guid Id { get; }

    public byte[] Descriptor { get; }

    public byte[] DescriptorHash { get; }

    public byte[] Key { get; }

    private HybridEpoch(Guid id, byte[] descriptor, byte[] key)
    {
        Id = id;
        Descriptor = descriptor;
        Key = key;

        DescriptorHash = CryptoEncoding.Sha3(descriptor);
    }

    public static HybridEpoch Create(Guid group, LabKeys keys)
    {
        var random = new SecureRandom();
        var x = new X25519PrivateKeyParameters(random);

        var encapsulator = new MLKemEncapsulator(MLKemParameters.ml_kem_768);
        encapsulator.Init(new ParametersWithRandom(keys.RecipientKem.GetPublicKey(), random));

        var ciphertext = new byte[encapsulator.EncapsulationLength];
        var pqSecret = new byte[encapsulator.SecretLength];
        var xSecret = new byte[32];
        var salt = new byte[32];
        random.NextBytes(salt);

        Guid id = Guid.NewGuid();

        try
        {
            encapsulator.Encapsulate(ciphertext, 0, ciphertext.Length, pqSecret, 0, pqSecret.Length);
            x.GenerateSecret(keys.RecipientX.GeneratePublicKey(), xSecret, 0);

            byte[] descriptor = x.GeneratePublicKey()
                .GetEncoded()
                .Concat(ciphertext)
                .Concat(salt)
                .ToArray();

            return new HybridEpoch(
                id,
                descriptor,
                Derive(group, id, descriptor, keys.RecipientId, xSecret, pqSecret));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(xSecret);
            CryptographicOperations.ZeroMemory(pqSecret);
        }
    }

    public static HybridEpoch Open(Guid group, Guid id, byte[] descriptor, LabKeys keys)
    {
        if (descriptor.Length != DescriptorBytes)
        {
            throw new InvalidDataException("Invalid hybrid key descriptor.");
        }

        var decapsulator = new MLKemDecapsulator(MLKemParameters.ml_kem_768);
        decapsulator.Init(keys.RecipientKem);

        var pqSecret = new byte[decapsulator.SecretLength];
        var xSecret = new byte[32];

        try
        {
            decapsulator.Decapsulate(descriptor, 32, 1088, pqSecret, 0, pqSecret.Length);
            keys.RecipientX.GenerateSecret(new X25519PublicKeyParameters(descriptor, 0), xSecret, 0);

            return new HybridEpoch(
                id,
                descriptor,
                Derive(group, id, descriptor, keys.RecipientId, xSecret, pqSecret));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(xSecret);
            CryptographicOperations.ZeroMemory(pqSecret);
        }
    }

    private static byte[] Derive(
        Guid group,
        Guid epoch,
        byte[] descriptor,
        byte[] recipientId,
        byte[] xSecret,
        byte[] pqSecret)
    {
        if (xSecret.All(value => value == 0))
        {
            throw new CryptographicException("Invalid all-zero X25519 secret.");
        }

        byte[] input = xSecret.Concat(pqSecret).ToArray();

        try
        {
            var hkdf = new HkdfBytesGenerator(new Sha384Digest());
            hkdf.Init(new HkdfParameters(
                input,
                descriptor.AsSpan(1120, 32).ToArray(),
                CryptoEncoding.Frame(
                    "S3LAB/v1/X25519+ML-KEM-768/HKDF-SHA384",
                    group.ToByteArray(),
                    epoch.ToByteArray(),
                    recipientId,
                    descriptor)));

            var result = new byte[32];
            hkdf.GenerateBytes(result, 0, result.Length);

            return result;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(input);
        }
    }

    public void Dispose() => CryptographicOperations.ZeroMemory(Key);
}