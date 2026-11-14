using System.Security.Cryptography;
using S3SyncLab.Application;
using S3SyncLab.Core;
using S3SyncLab.Infrastructure;
using S3SyncLab.Infrastructure.Crypto;
using S3SyncLab.Infrastructure.Delta;
using S3SyncLab.Infrastructure.Storage;

namespace S3SyncLab.Verification;

public static class SelfTests
{
    public static async Task RunAsync(CancellationToken cancellationToken)
    {
        CheckPolicies();
        CheckTokenBucket();

        string root = Path.Combine(Path.GetTempPath(), "s3sync-selftest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);

        try
        {
            await using var store = new LocalObjectStore(Path.Combine(root, "objects"));

            await CheckStoreAsync(store, "self-test", cancellationToken);
            await CheckPacketsAsync(root, store, cancellationToken);

            Console.WriteLine("PASS: policies, refill during transmission, delta roundtrip, hybrid keys, both signatures,");
            Console.WriteLine("      ciphertext/tag/commitment tampering, truncation, cold restore, snapshot replacement, output preservation.");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static void CheckPolicies()
    {
        var fixedTwo = new FixedThresholdPolicy(2);
        var input = new DecisionInput(100, 10, new ChainState(190, 1), 100, 100, 128);

        Require(fixedTwo.Decide(input).Kind == PacketKind.Full, "Equality must trigger a full snapshot.");
        Require(fixedTwo.Decide(input with { Chain = new ChainState(189, 1) }).Kind == PacketKind.Delta, "Below threshold.");
        Require(fixedTwo.Decide(input with { DeltaBytes = 100 }).Reason == "delta-not-smaller", "Large delta fallback.");
        Require(fixedTwo.Decide(input with { Chain = new ChainState(100, 128) }).Reason == "chain-length", "Chain cap.");
        Require(fixedTwo.Decide(input with { Chain = ChainState.Empty }).Reason == "initial", "Initial full snapshot.");

        var adaptive = new AdaptiveThresholdPolicy(2, 4);

        Require(adaptive.Decide(input).Threshold == 2, "Full bucket threshold.");
        Require(adaptive.Decide(input with { AvailableTokens = 0 }).Threshold == 4, "Empty bucket threshold.");
        Require(adaptive.Decide(input with { AvailableTokens = 50 }).Threshold == 3, "Intermediate threshold.");
        Require(MeasurementSummary.Quantile(new double[] { 1, 3 }, 0.5) == 2, "Quantile interpolation.");
    }

    private static void CheckTokenBucket()
    {
        var bucket = new TokenBucketModel(100, 10, 100);
        var first = bucket.Transfer(150);

        Require(
            Math.Abs(first.Seconds - 5) < 1e-9
            && Math.Abs(first.TokensAfter) < 1e-9,
            "Transfer larger than capacity.");

        bucket.Idle(2);
        var next = bucket.Transfer(10);

        Require(
            Math.Abs(next.Seconds - 0.1) < 1e-9
            && Math.Abs(next.TokensAfter - 11) < 1e-9,
            "Refill during transfer.");

        bucket.Idle(100);

        Require(bucket.Tokens == 100, "Capacity clamp.");

        double now = 0;
        var observed = new ObservedTokenBudget(100, 10, () => now);

        observed.StartTransfer();
        now = 5;

        Require(
            observed.CompleteTransfer(150) == 0
            && observed.Balance == 0,
            "Observed refill during a large transfer.");

        now = 7;

        Require(observed.Balance == 20, "Observed idle refill.");

        observed.StartTransfer();
        now = 7.1;

        Require(observed.CompleteTransfer(100) > 78, "Observed mismatch must be reported.");
    }

    public static async Task CheckStoreAsync(
        IObjectStore store,
        string prefix,
        CancellationToken cancellationToken)
    {
        string scratch = Path.Combine(Path.GetTempPath(), "s3sync-storage-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(scratch);

        string key = prefix + "/storage-test/" + Guid.NewGuid().ToString("N") + ".bin";

        try
        {
            string source = Path.Combine(scratch, "source.bin");
            string copy = Path.Combine(scratch, "copy.bin");

            var bytes = new byte[1024 * 1024 + 37];
            new Random(12345).NextBytes(bytes);

            await File.WriteAllBytesAsync(source, bytes, cancellationToken);
            await store.PutFileAsync(key, source, cancellationToken);
            await store.GetFileAsync(key, copy, bytes.Length, cancellationToken);

            Require(
                FileOperations.Fingerprint(source, cancellationToken) == FileOperations.Fingerprint(copy, cancellationToken),
                "Storage roundtrip.");

            bool rejected = false;

            try
            {
                await store.GetFileAsync(key, copy, bytes.Length - 1, cancellationToken);
            }
            catch (InvalidDataException)
            {
                rejected = true;
            }

            Require(rejected, "Store must enforce the download size limit.");

            bytes[0] ^= 1;

            await File.WriteAllBytesAsync(source, bytes, cancellationToken);
            await store.PutFileAsync(key, source, cancellationToken);
            await store.GetFileAsync(key, copy, bytes.Length, cancellationToken);

            Require(
                FileOperations.Fingerprint(source, cancellationToken) == FileOperations.Fingerprint(copy, cancellationToken),
                "Mutable pointer replacement.");

            Console.WriteLine("PASS: PUT/GET/replace/size bound, " + store.Description);
        }
        finally
        {
            try
            {
                await store.DeleteAsync(key, CancellationToken.None);
            }
            finally
            {
                Directory.Delete(scratch, recursive: true);
            }
        }
    }

    private static async Task CheckPacketsAsync(
        string root,
        IObjectStore store,
        CancellationToken cancellationToken)
    {
        string basis = Path.Combine(root, "basis.bin");
        string target = Path.Combine(root, "target.bin");
        string patch = Path.Combine(root, "update.delta");
        string applied = Path.Combine(root, "applied.bin");

        byte[] bytes = new byte[2 * 1024 * 1024];
        new Random(42).NextBytes(bytes);

        File.WriteAllBytes(basis, bytes);

        for (int i = 8000; i < 14000; i++)
        {
            bytes[i] ^= 0x5a;
        }

        File.WriteAllBytes(target, bytes);

        var codec = new OctodiffCodec(4096);
        codec.Create(basis, target, patch, cancellationToken);

        Require(new FileInfo(patch).Length < bytes.Length, "The fixture must produce a smaller delta.");

        codec.Apply(basis, patch, applied, cancellationToken);

        var basisHash = FileOperations.Fingerprint(basis, cancellationToken);
        var targetHash = FileOperations.Fingerprint(target, cancellationToken);

        FileOperations.RequireMatch(applied, targetHash, cancellationToken);

        ExpectFailure(() => codec.Apply(basis, patch, applied, cancellationToken));

        FileOperations.RequireMatch(applied, targetHash, cancellationToken);

        var keys = LabKeys.Generate();
        string keyFile = Path.Combine(root, "keys.json");
        keys.Save(keyFile);

        var reloaded = LabKeys.Load(keyFile);
        Guid group = Guid.NewGuid();

        using var writer = new HybridPacketWriter(group, keys);
        string fullFile = Path.Combine(root, "full.bin");

        var full = writer.Protect(
            new PacketRequest(
                0,
                PacketKind.Full,
                HybridPacketWriter.ZeroRoot,
                basisHash,
                HybridPacketWriter.NoBasis,
                basis),
            fullFile,
            cancellationToken);

        Require(full.Bytes == writer.Measure(basisHash.Length, PacketKind.Full), "Exact full packet length.");

        string deltaFile = Path.Combine(root, "delta.bin");

        var delta = writer.Protect(
            new PacketRequest(
                1,
                PacketKind.Delta,
                full.Root,
                targetHash,
                basisHash,
                patch),
            deltaFile,
            cancellationToken);

        Require(delta.Bytes == writer.Measure(new FileInfo(patch).Length, PacketKind.Delta), "Exact delta packet length.");

        string payload = Path.Combine(root, "payload.bin");

        using (var reader = new HybridPacketReader(group, reloaded))
        {
            reader.Open(fullFile, payload, full.Root, cancellationToken);
            FileOperations.RequireMatch(payload, basisHash, cancellationToken);
            File.Delete(payload);

            reader.Open(deltaFile, payload, delta.Root, cancellationToken);

            Require(
                FileOperations.Fingerprint(payload, cancellationToken) == FileOperations.Fingerprint(patch, cancellationToken),
                "Encrypted delta payload.");

            File.Delete(payload);
        }

        long cipherOffset;
        long cipherLength;

        using (var file = FileOperations.OpenRead(fullFile))
        using (var input = new BinaryReader(file))
        {
            var parsed = PacketFormat.ReadHeader(input);
            cipherOffset = parsed.Encoded.Length;
            cipherLength = parsed.Header.CipherBytes;
        }

        CheckTamper(cipherOffset + 100, resign: false, "ciphertext");
        CheckTamper(new FileInfo(fullFile).Length - PacketFormat.DsaSignatureBytes - 1, resign: false, "Ed25519");
        CheckTamper(new FileInfo(fullFile).Length - 1, resign: false, "ML-DSA");
        CheckTamper(cipherOffset + cipherLength - 1, resign: true, "GCM tag with valid signatures");

        // Commitment occupies the 48 bytes before cipher length and descriptor length/data.
        CheckTamper(
            cipherOffset - HybridEpoch.DescriptorBytes - sizeof(int) - sizeof(long) - 1,
            resign: true,
            "key commitment with valid signatures");

        string truncated = Path.Combine(root, "truncated.bin");
        File.Copy(fullFile, truncated);

        using (var file = new FileStream(truncated, FileMode.Open, FileAccess.Write))
        {
            file.SetLength(file.Length - 1);
        }

        using (var reader = new HybridPacketReader(group, keys))
        {
            ExpectFailure(() => reader.Open(truncated, payload, full.Root, cancellationToken));
        }

        Require(!File.Exists(payload), "Rejected packets must not expose plaintext.");

        File.WriteAllText(payload, "keep-existing-output");

        using (var reader = new HybridPacketReader(group, keys))
        {
            ExpectFailure(() => reader.Open(fullFile, payload, full.Root, cancellationToken));
        }

        Require(File.ReadAllText(payload) == "keep-existing-output", "Reader must preserve an existing output.");
        File.Delete(payload);

        string prefix = "self-test/" + group.ToString("N");
        string fullKey = prefix + "/objects/" + full.Root + ".bin";
        string deltaKey = prefix + "/objects/" + delta.Root + ".bin";

        await store.PutFileAsync(fullKey, fullFile, cancellationToken);
        await store.PutFileAsync(deltaKey, deltaFile, cancellationToken);

        string headKey = prefix + "/HEAD.json";
        string headFile = Path.Combine(root, "head.json");

        var head = new HeadDocument(
            1,
            group.ToString("N"),
            1,
            delta.Root,
            new[]
            {
                new ChainObject(fullKey, full.Root, full.Bytes, 0, PacketKind.Full),
                new ChainObject(deltaKey, delta.Root, delta.Bytes, 1, PacketKind.Delta)
            });

        FileOperations.WriteJsonAtomic(headFile, head);
        await store.PutFileAsync(headKey, headFile, cancellationToken);

        var engine = new RestoreEngine(
            store,
            codec,
            () => new HybridPacketReader(group, reloaded),
            group,
            prefix,
            128);

        string restored = Path.Combine(root, "restored.bin");
        var result = await engine.RestoreAsync(headKey, delta.Root, restored, root, cancellationToken);

        Require(
            result.File == targetHash
            && result.ObjectCount == 3,
            "Cold full+delta restoration.");

        string compactFile = Path.Combine(root, "compact.bin");

        var compact = writer.Protect(
            new PacketRequest(
                2,
                PacketKind.Full,
                delta.Root,
                targetHash,
                HybridPacketWriter.NoBasis,
                target),
            compactFile,
            cancellationToken);

        string compactKey = prefix + "/objects/" + compact.Root + ".bin";
        await store.PutFileAsync(compactKey, compactFile, cancellationToken);

        head = new HeadDocument(
            1,
            group.ToString("N"),
            2,
            compact.Root,
            new[]
            {
                new ChainObject(compactKey, compact.Root, compact.Bytes, 2, PacketKind.Full)
            });

        FileOperations.WriteJsonAtomic(headFile, head);
        await store.PutFileAsync(headKey, headFile, cancellationToken);

        await store.DeleteAsync(fullKey, cancellationToken);
        await store.DeleteAsync(deltaKey, cancellationToken);

        await ExpectFailureAsync(() => engine.RestoreAsync(headKey, compact.Root, restored, root, cancellationToken));
        FileOperations.RequireMatch(restored, targetHash, cancellationToken);
        File.Delete(restored);

        result = await engine.RestoreAsync(headKey, compact.Root, restored, root, cancellationToken);

        Require(
            result.File == targetHash
            && result.ObjectCount == 2,
            "Cold restore from a replacement snapshot.");

        await ExpectFailureAsync(() => engine.RestoreAsync(
            headKey,
            delta.Root,
            Path.Combine(root, "stale.bin"),
            root,
            cancellationToken));

        // A valid signature must not make a delta with the wrong parent acceptable.
        using var badWriter = new HybridPacketWriter(group, keys);
        string badFullFile = Path.Combine(root, "bad-full.bin");
        string badDeltaFile = Path.Combine(root, "bad-delta.bin");

        var badFull = badWriter.Protect(
            new PacketRequest(
                0,
                PacketKind.Full,
                HybridPacketWriter.ZeroRoot,
                basisHash,
                HybridPacketWriter.NoBasis,
                basis),
            badFullFile,
            cancellationToken);

        var badDelta = badWriter.Protect(
            new PacketRequest(
                1,
                PacketKind.Delta,
                HybridPacketWriter.ZeroRoot,
                targetHash,
                basisHash,
                patch),
            badDeltaFile,
            cancellationToken);

        string badFullKey = prefix + "/objects/" + badFull.Root + ".bin";
        string badDeltaKey = prefix + "/objects/" + badDelta.Root + ".bin";

        await store.PutFileAsync(badFullKey, badFullFile, cancellationToken);
        await store.PutFileAsync(badDeltaKey, badDeltaFile, cancellationToken);

        var broken = new HeadDocument(
            1,
            group.ToString("N"),
            1,
            badDelta.Root,
            new[]
            {
                new ChainObject(badFullKey, badFull.Root, badFull.Bytes, 0, PacketKind.Full),
                new ChainObject(badDeltaKey, badDelta.Root, badDelta.Bytes, 1, PacketKind.Delta)
            });

        FileOperations.WriteJsonAtomic(headFile, broken);
        await store.PutFileAsync(headKey, headFile, cancellationToken);

        string brokenOutput = Path.Combine(root, "broken.bin");
        await ExpectFailureAsync(() => engine.RestoreAsync(headKey, badDelta.Root, brokenOutput, root, cancellationToken));

        Require(!File.Exists(brokenOutput), "Broken parent chain must not publish a file.");

        using var concurrentWriter = new HybridPacketWriter(Guid.NewGuid(), keys);
        using var start = new ManualResetEventSlim(false);

        Task<bool>[] concurrentCalls = Enumerable.Range(0, 2)
            .Select(index => Task.Run(() =>
            {
                start.Wait(cancellationToken);

                try
                {
                    concurrentWriter.Protect(
                        new PacketRequest(
                            0,
                            PacketKind.Full,
                            HybridPacketWriter.ZeroRoot,
                            basisHash,
                            HybridPacketWriter.NoBasis,
                            basis),
                        Path.Combine(root, $"concurrent-{index}.bin"),
                        cancellationToken);

                    return true;
                }
                catch (InvalidOperationException)
                {
                    return false;
                }
            }, cancellationToken))
            .ToArray();

        start.Set();

        bool[] accepted = await Task.WhenAll(concurrentCalls);
        Require(accepted.Count(value => value) == 1, "Concurrent calls must not reuse the same version/nonce.");

        void CheckTamper(long offset, bool resign, string description)
        {
            string damaged = Path.Combine(root, "damaged.bin");
            byte[] data = File.ReadAllBytes(fullFile);
            data[checked((int)offset)] ^= 1;

            string expectedRoot = full.Root;

            if (resign)
            {
                using var input = new BinaryReader(new MemoryStream(data));
                var parsed = PacketFormat.ReadHeader(input);

                var digest = PacketCipher.RootDigest(parsed.Encoded);
                digest.BlockUpdate(data, parsed.Encoded.Length, checked((int)parsed.Header.CipherBytes));

                byte[] changedRoot = PacketCipher.FinishRoot(digest);
                var signatures = keys.Sign(changedRoot);

                int trailerOffset = checked(parsed.Encoded.Length + (int)parsed.Header.CipherBytes);
                changedRoot.CopyTo(data, trailerOffset);
                signatures.Ed.CopyTo(data, trailerOffset + PacketFormat.RootBytes);
                signatures.Dsa.CopyTo(data, trailerOffset + PacketFormat.RootBytes + PacketFormat.EdSignatureBytes);

                expectedRoot = CryptoEncoding.Hex(changedRoot);
            }

            File.WriteAllBytes(damaged, data);

            using var reader = new HybridPacketReader(group, keys);
            ExpectFailure(() => reader.Open(damaged, payload, expectedRoot, cancellationToken));

            Require(!File.Exists(payload), "No plaintext after " + description);
        }
    }

    private static void ExpectFailure(Action action)
    {
        bool failed = false;

        try
        {
            action();
        }
        catch (Exception error) when (
            error is IOException
            or InvalidDataException
            or CryptographicException
            or Org.BouncyCastle.Crypto.InvalidCipherTextException)
        {
            failed = true;
        }

        Require(failed, "Expected an operation to reject invalid input.");
    }

    private static async Task ExpectFailureAsync(Func<Task> action)
    {
        bool failed = false;

        try
        {
            await action();
        }
        catch (Exception error) when (
            error is IOException
            or InvalidDataException
            or CryptographicException)
        {
            failed = true;
        }

        Require(failed, "Expected the asynchronous operation to reject invalid input.");
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException("Self-test failed: " + message);
        }
    }
}