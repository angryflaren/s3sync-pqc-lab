using System.Security.Cryptography;
using System.Text.Json;
using S3SyncLab.Configuration;
using S3SyncLab.Core;

namespace S3SyncLab.Infrastructure;

public static class FileOperations
{
    public const int BufferBytes = 128 * 1024;

    public static FileStream OpenRead(string path) => new(
        path,
        FileMode.Open,
        FileAccess.Read,
        FileShare.Read,
        BufferBytes,
        FileOptions.SequentialScan);

    public static FileFingerprint Fingerprint(string path, CancellationToken cancellationToken)
    {
        using var stream = OpenRead(path);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        var buffer = new byte[BufferBytes];
        int count;

        while ((count = stream.Read(buffer, 0, buffer.Length)) != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            hash.AppendData(buffer, 0, count);
        }

        return new FileFingerprint(stream.Length, Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant());
    }

    public static void RequireMatch(
        string path,
        FileFingerprint expected,
        CancellationToken cancellationToken)
    {
        if (Fingerprint(path, cancellationToken) != expected)
        {
            throw new InvalidDataException("Restored file does not match its authenticated SHA-256 and length.");
        }
    }

    public static async Task CopyAtomicAsync(
        string source,
        string destination,
        CancellationToken cancellationToken,
        bool overwrite = true)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(destination))!);

        string temporary = destination + "." + Guid.NewGuid().ToString("N") + ".tmp";

        try
        {
            await using (var input = OpenRead(source))
            await using (var output = new FileStream(
                temporary,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                BufferBytes,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                await input.CopyToAsync(output, BufferBytes, cancellationToken);
                await output.FlushAsync(cancellationToken);
                output.Flush(flushToDisk: true);
            }

            File.Move(temporary, destination, overwrite);
        }
        finally
        {
            DeleteIfExists(temporary);
        }
    }

    public static void WriteJsonAtomic<T>(
        string path,
        T value,
        bool privateFile = false)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);

        string temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp";

        try
        {
            using (var output = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                if (privateFile
                    && !OperatingSystem.IsWindows())
                {
                    File.SetUnixFileMode(temporary, UnixFileMode.UserRead | UnixFileMode.UserWrite);
                }

                JsonSerializer.Serialize(output, value, LabOptions.Json);
                output.Flush(flushToDisk: true);
            }

            File.Move(temporary, path, overwrite: true);
        }
        finally
        {
            DeleteIfExists(temporary);
        }
    }

    public static T ReadJson<T>(string path) =>
        JsonSerializer.Deserialize<T>(File.ReadAllText(path), LabOptions.Json)
        ?? throw new InvalidDataException("Empty JSON: " + path);

    public static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    public static string ValidateObjectKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key)
            || key.Length > 1024
            || key.Contains('\\')
            || key.Split('/').Any(part => part.Length == 0 || part is "." or "..")
            || key.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '/' or '-' or '_' or '.')))
        {
            throw new InvalidDataException("Unsafe object key.");
        }

        return key;
    }
}