using System.Security.Cryptography;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Crypto.Signers;
using Org.BouncyCastle.Security;

namespace S3SyncLab.Infrastructure.Crypto;

// Local research keys; never load a replacement trust key from object storage.
public sealed record LabKeyFile(
    int Format,
    string X25519Private,
    string MlKemPrivate,
    string Ed25519Private,
    string MlDsaPrivate);

public sealed class LabKeys
{
    internal X25519PrivateKeyParameters RecipientX { get; }

    internal MLKemPrivateKeyParameters RecipientKem { get; }

    internal Ed25519PrivateKeyParameters SenderEd { get; }

    internal MLDsaPrivateKeyParameters SenderDsa { get; }

    internal byte[] SignerId { get; }

    internal byte[] RecipientId { get; }

    private static readonly byte[] SignatureContext = Encoding.ASCII.GetBytes("S3LAB/v1/root");

    private LabKeys(
        X25519PrivateKeyParameters x,
        MLKemPrivateKeyParameters kem,
        Ed25519PrivateKeyParameters ed,
        MLDsaPrivateKeyParameters dsa)
    {
        RecipientX = x;
        RecipientKem = kem;
        SenderEd = ed;
        SenderDsa = dsa;

        SignerId = CryptoEncoding.Sha3(CryptoEncoding.Frame(
            "S3LAB/v1/signers",
            ed.GeneratePublicKey().GetEncoded(),
            dsa.GetPublicKey().GetEncoded()));

        RecipientId = CryptoEncoding.Sha3(CryptoEncoding.Frame(
            "S3LAB/v1/recipient",
            x.GeneratePublicKey().GetEncoded(),
            kem.GetPublicKey().GetEncoded()));
    }

    public static LabKeys Generate()
    {
        var random = new SecureRandom();

        var kemGenerator = new MLKemKeyPairGenerator();
        kemGenerator.Init(new MLKemKeyGenerationParameters(random, MLKemParameters.ml_kem_768));

        var dsaGenerator = new MLDsaKeyPairGenerator();
        dsaGenerator.Init(new MLDsaKeyGenerationParameters(random, MLDsaParameters.ml_dsa_65));

        return new LabKeys(
            new X25519PrivateKeyParameters(random),
            (MLKemPrivateKeyParameters)kemGenerator.GenerateKeyPair().Private,
            new Ed25519PrivateKeyParameters(random),
            (MLDsaPrivateKeyParameters)dsaGenerator.GenerateKeyPair().Private);
    }

    public void Save(string path)
    {
        if (File.Exists(path))
        {
            throw new IOException("Refusing to overwrite an existing key file.");
        }

        FileOperations.WriteJsonAtomic(
            path,
            new LabKeyFile(
                1,
                Convert.ToBase64String(RecipientX.GetEncoded()),
                Convert.ToBase64String(RecipientKem.GetEncoded()),
                Convert.ToBase64String(SenderEd.GetEncoded()),
                Convert.ToBase64String(SenderDsa.GetEncoded())),
            privateFile: true);
    }

    public static LabKeys Load(string path)
    {
        var file = FileOperations.ReadJson<LabKeyFile>(path);

        if (file.Format != 1)
        {
            throw new InvalidDataException("Unsupported local key format.");
        }

        byte[] x = Convert.FromBase64String(file.X25519Private);
        byte[] kem = Convert.FromBase64String(file.MlKemPrivate);
        byte[] ed = Convert.FromBase64String(file.Ed25519Private);
        byte[] dsa = Convert.FromBase64String(file.MlDsaPrivate);

        try
        {
            return new LabKeys(
                new X25519PrivateKeyParameters(x, 0),
                MLKemPrivateKeyParameters.FromEncoding(MLKemParameters.ml_kem_768, kem),
                new Ed25519PrivateKeyParameters(ed, 0),
                MLDsaPrivateKeyParameters.FromEncoding(MLDsaParameters.ml_dsa_65, dsa));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(x);
            CryptographicOperations.ZeroMemory(kem);
            CryptographicOperations.ZeroMemory(ed);
            CryptographicOperations.ZeroMemory(dsa);
        }
    }

    internal (byte[] Ed, byte[] Dsa) Sign(byte[] root)
    {
        var ed = new Ed25519Signer();
        ed.Init(true, SenderEd);
        ed.BlockUpdate(root, 0, root.Length);

        var dsa = new MLDsaSigner(MLDsaParameters.ml_dsa_65, deterministic: false);
        dsa.Init(
            true,
            new ParametersWithContext(
                new ParametersWithRandom(SenderDsa, new SecureRandom()),
                SignatureContext));
        dsa.BlockUpdate(root, 0, root.Length);

        return (ed.GenerateSignature(), dsa.GenerateSignature());
    }

    internal void Verify(
        byte[] root,
        byte[] edSignature,
        byte[] dsaSignature)
    {
        var ed = new Ed25519Signer();
        ed.Init(false, SenderEd.GeneratePublicKey());
        ed.BlockUpdate(root, 0, root.Length);

        var dsa = new MLDsaSigner(MLDsaParameters.ml_dsa_65, deterministic: false);
        dsa.Init(false, new ParametersWithContext(SenderDsa.GetPublicKey(), SignatureContext));
        dsa.BlockUpdate(root, 0, root.Length);

        bool edOk = ed.VerifySignature(edSignature);
        bool dsaOk = dsa.VerifySignature(dsaSignature);

        if (!edOk
            || !dsaOk)
        {
            throw new CryptographicException("Both Ed25519 and ML-DSA-65 signatures must verify.");
        }
    }
}